"""
Processing logic for delimited text files with CSVW metadata support.
"""

import logging
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, when

from .checksum import compute_file_checksum
from .config import MAX_CORRUPT_RECORDS_PERCENT
from .csvw_dialect import extract_csvw_dialect, extract_csvw_null_values
from .csvw_metadata import load_csvw_metadata
from .csvw_schema import csvw_to_spark_schema
from .exceptions import FileProcessingException
from .helpers import detect_header
from .models import FileProcessingResult

logger = logging.getLogger(__name__)


def _check_corrupt_records(df: DataFrame) -> int:
    if "_corrupt_record" in df.columns:
        return df.filter(df._corrupt_record.isNotNull()).count()
    return 0


def _process_delimited_file(
    file_path: str,
    spark: SparkSession,
    options: dict[str, Any],
    metadata_path: str | None = None,
) -> FileProcessingResult:
    try:
        if metadata_path is None:
            metadata_path = options.get("csvw_metadata_path")

        metadata = load_csvw_metadata(file_path, metadata_path)

        if metadata:
            logger.info("Using CSVW metadata for %s", file_path)
            return _process_with_csvw(file_path, spark, metadata, options)
        else:
            logger.info("No CSVW metadata found, using inference for %s", file_path)
            return _process_with_inference(file_path, spark, options)

    except Exception as e:
        logger.exception("Error processing %s", file_path)
        exc = FileProcessingException(
            f"Unexpected error: {str(e)}", "processing_error", type(e).__name__
        )
        return FileProcessingResult(
            success=False,
            file=file_path,
            message=exc.message,
            error=exc.to_dict(),
        )


def _process_with_csvw(
    file_path: str,
    spark: SparkSession,
    metadata: dict[str, Any],
    options: dict[str, Any],
) -> FileProcessingResult:
    try:
        schema = csvw_to_spark_schema(metadata)
        if not schema:
            logger.warning(
                "Failed to extract schema from CSVW metadata, falling back to inference"
            )
            return _process_with_inference(file_path, spark, options)

        dialect = extract_csvw_dialect(metadata)
        delimiter = dialect.get("delimiter", ",")

        if len(delimiter) > 1:
            exc = FileProcessingException(
                f"Multi-character delimiter '{delimiter}' not supported. "
                f"Spark only supports single-character delimiters.",
                "invalid_delimiter",
                "Use a single-character delimiter or preprocess the file",
            )
            return FileProcessingResult(
                success=False,
                file=file_path,
                message=exc.message,
                error=exc.to_dict(),
            )

        encoding = dialect.get("encoding", "UTF-8")
        header = "true" if dialect.get("header", True) else "false"

        null_values = extract_csvw_null_values(metadata)

        reader = (
            spark.read.schema(schema)
            .option("sep", delimiter)
            .option("header", header)
            .option("encoding", encoding)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .option("multiLine", "false")
        )

        if null_values:
            reader = reader.option("nullValue", null_values[0])

        skip_rows = dialect.get("skip_rows", 0)
        if skip_rows > 0:
            logger.warning("CSVW skipRows=%d not fully supported in Spark", skip_rows)

        df = reader.csv(file_path)

        if len(null_values) > 1:
            replacements = {}
            for col_name in df.columns:
                if col_name == "_corrupt_record":
                    continue
                col_expr = col(col_name)
                for null_val in null_values[1:]:
                    col_expr = when(col_expr == lit(null_val), lit(None)).otherwise(
                        col_expr
                    )
                replacements[col_name] = col_expr

            df = df.withColumns(replacements)

        final_result = _validate_and_return(
            df,
            file_path,
            metadata_source="CSVW",
            csvw_metadata=metadata,
            options=options,
        )

        final_result.checksum = compute_file_checksum(
            file_path, storage_options=options.get("storage_options")
        )

        return final_result

    except Exception as e:
        logger.error("Error processing with CSVW metadata: %s", e)
        logger.info("Falling back to inference mode")
        return _process_with_inference(file_path, spark, options)


def _process_with_inference(
    file_path: str, spark: SparkSession, options: dict[str, Any]
) -> FileProcessingResult:
    try:
        delimiter = options.get("delimiter", ",")

        if len(delimiter) > 1:
            exc = FileProcessingException(
                f"Multi-character delimiter '{delimiter}' not supported. "
                f"Spark only supports single-character delimiters.",
                "invalid_delimiter",
                "Use a single-character delimiter or preprocess the file",
            )
            return FileProcessingResult(
                success=False,
                file=file_path,
                message=exc.message,
                error=exc.to_dict(),
            )

        if "header" not in options:
            header = "true"
            logger.debug("No header specified, defaulting to 'true' for %s", file_path)
        elif options["header"] == "auto":
            has_header = detect_header(file_path, delimiter)
            header = "true" if has_header else "false"
            logger.info("Auto-detected header=%s for %s", header, file_path)
        else:
            header = options["header"]

        infer_schema = options.get("inferSchema", "true")

        df = (
            spark.read.option("header", header)
            .option("inferSchema", infer_schema)
            .option("sep", delimiter)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .option("encoding", "UTF-8")
            .option("multiLine", "false")
            .csv(file_path)
        )

        result = _validate_and_return(
            df, file_path, metadata_source="inference", options=options
        )

        result.checksum = compute_file_checksum(
            file_path, storage_options=options.get("storage_options")
        )

        return result

    except Exception as e:
        logger.exception("Error in inference processing for %s", file_path)
        exc = FileProcessingException(
            f"Error processing file: {str(e)}", "processing_error", type(e).__name__
        )
        return FileProcessingResult(
            success=False,
            file=file_path,
            message=exc.message,
            error=exc.to_dict(),
        )


def _validate_and_return(
    df: DataFrame,
    file_path: str,
    metadata_source: str,
    csvw_metadata: dict[str, Any] | None = None,
    options: dict[str, Any] | None = None,
) -> FileProcessingResult:
    if len(df.columns) == 0:
        exc = FileProcessingException(
            "No columns detected - file may be empty or have wrong delimiter",
            "no_columns",
            "Check file content and delimiter setting",
        )
        return FileProcessingResult(
            success=False,
            file=file_path,
            message=exc.message,
            error=exc.to_dict(),
        )

    total_rows = df.count()

    if total_rows == 0:
        exc = FileProcessingException(
            "File contains no data rows",
            "no_data",
            "File is empty or contains only headers",
        )
        return FileProcessingResult(
            success=False,
            file=file_path,
            message=exc.message,
            error=exc.to_dict(),
        )

    corrupt_rows = _check_corrupt_records(df)
    corruption_rate = corrupt_rows / total_rows if total_rows > 0 else 0

    max_corrupt_percent = (options or {}).get(
        "max_corrupt_records_percent", MAX_CORRUPT_RECORDS_PERCENT
    )

    if corruption_rate > max_corrupt_percent:
        exc = FileProcessingException(
            f"Too many corrupt records: {corrupt_rows}/{total_rows} ({corruption_rate:.1%})",
            "corrupt_data",
            "Check delimiter setting or file format",
        )
        return FileProcessingResult(
            success=False,
            file=file_path,
            message=f"Too many corrupt records: {corrupt_rows}/{total_rows} ({corruption_rate:.1%}).",
            error=exc.to_dict(),
            rows_processed=total_rows,
            rows_corrupt=corrupt_rows,
        )

    message = (
        f"Successfully parsed using {metadata_source}: "
        f"{total_rows} rows, {len(df.columns)} columns"
    )

    if corrupt_rows > 0:
        message += f" ({corrupt_rows} corrupt records)"

    return FileProcessingResult(
        success=True,
        file=file_path,
        dataframe=df,
        message=message,
        rows_processed=total_rows,
        rows_corrupt=corrupt_rows,
        metadata=csvw_metadata,
    )
