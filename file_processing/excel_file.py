"""Processing logic for Excel files (.xls and .xlsx)."""

import logging
from pathlib import Path
from typing import Any

import pandas as pd
from pyspark.sql import SparkSession

from .exceptions import FileProcessingException
from .models import FileProcessingResult

logger = logging.getLogger(__name__)

MAX_EXCEL_FILE_SIZE_MB = 100


def _process_excel_file(
    file_path: str,
    spark: SparkSession,
    options: dict[str, Any],
) -> FileProcessingResult:
    try:
        max_file_size_mb = options.get("max_file_size_mb", MAX_EXCEL_FILE_SIZE_MB)
        file_size_mb = Path(file_path).stat().st_size / (1024 * 1024)

        if file_size_mb > max_file_size_mb:
            logger.warning(
                "Excel file exceeds size limit: %s (%.1fMB)", file_path, file_size_mb
            )
            exc = FileProcessingException(
                f"Excel file too large ({file_size_mb:.1f}MB)",
                "file_too_large",
                f"Maximum allowed: {max_file_size_mb}MB. Convert to CSV first.",
            )
            return FileProcessingResult(
                success=False,
                file=file_path,
                message=f"Excel file too large ({file_size_mb:.1f}MB). Maximum allowed: {max_file_size_mb}MB. Please convert to CSV first.",
                error=exc.to_dict(),
            )

        sheet_name = options.get("sheet_name", 0)
        header = options.get("header", 0)

        df_pandas = pd.read_excel(file_path, sheet_name=sheet_name, header=header)

        if df_pandas.empty:
            exc = FileProcessingException(
                "Excel file contains no data",
                "no_data",
                "File is empty or contains only headers",
            )
            return FileProcessingResult(
                success=False,
                file=file_path,
                message=exc.message,
                error=exc.to_dict(),
            )

        df_spark = spark.createDataFrame(df_pandas)

        row_count = len(df_pandas)

        return FileProcessingResult(
            success=True,
            file=file_path,
            dataframe=df_spark,
            message=f"Successfully parsed Excel file: {row_count} rows",
            rows_processed=row_count,
        )

    except Exception as e:
        logger.exception("Error processing Excel file %s", file_path)
        exc = FileProcessingException(
            f"Error processing Excel file: {str(e)}",
            "processing_error",
            type(e).__name__,
        )
        return FileProcessingResult(
            success=False,
            file=file_path,
            message=exc.message,
            error=exc.to_dict(),
        )
