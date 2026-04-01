"""Main table writing functionality."""

import logging
import time

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from .config import (
    MODE_APPEND,
    MODE_ERROR_IF_EXISTS,
    MODE_MERGE,
    MODE_OVERWRITE,
    MODES,
)
from .exceptions import TableWritingException
from .format_handlers import get_format_handler
from .mode_handlers import get_mode_handler, register_mode_handler
from .models import TableWriteOptions, TableWriteResult
from .validation import validate_write_options

logger = logging.getLogger(__name__)


def _handle_error(result: TableWriteResult, raise_on_error: bool) -> TableWriteResult:
    if raise_on_error and result.error:
        err = result.error
        raise TableWritingException(
            message=err.get("message", "Unknown error"),
            error_type=err.get("error_type", "unknown_error"),
            details=err.get("details", ""),
        )
    return result


def write_table(
    df: DataFrame,
    table_name: str,
    spark: SparkSession,
    options: TableWriteOptions | None = None,
    source_file_path: str | None = None,
    file_size: int | None = None,
    raise_on_error: bool = False,
) -> TableWriteResult:
    """Write DataFrame to a Databricks table.

    Writes a Spark DataFrame to a table with configurable format, mode, and schema handling.
    Supports Delta, Parquet, and ORC formats with append, overwrite, merge, and errorIfExists modes.

    Args:
        df: Spark DataFrame to write. Must not be None.
        table_name: Fully qualified table name. Supports:
            - Three-level: "catalog.schema.table" (Unity Catalog)
            - Two-level: "schema.table" (Hive metastore)
            - One-level: "table" (default schema)
        spark: SparkSession for table operations. Used for catalog access and table creation.
        options: TableWriteOptions for configuring the write. If None, uses defaults:
            - format="delta"
            - mode="append"
            - schema_mode="strict"
            - create_if_not_exists=True
            - add_source_metadata=False
        source_file_path: Path to source file (required if add_source_metadata=True)
        file_size: Size of source file in bytes (optional, for metadata)
        raise_on_error: If True, raise TableWritingException instead of returning error result

    Returns:
        TableWriteResult containing:
            - success (bool): True if write succeeded
            - rows_written (int): Number of rows written
            - rows_updated (int): Number of rows updated (merge mode only)
            - rows_deleted (int): Number of rows deleted (merge mode only)
            - message (str): Success or error message
            - error (dict): Error details if failed
            - duration_seconds (float): Write operation duration

    Raises:
        TableWritingException: If raise_on_error=True and write fails

    Examples:
        Simple append to table:
        >>> result = write_table(df, "my_table", spark)
        >>> if result.success:
        ...     print(f"Wrote {result.rows_written} rows")

        With source metadata:
        >>> options = TableWriteOptions(add_source_metadata=True)
        >>> result = write_table(
        ...     df, "my_table", spark, options,
        ...     source_file_path="s3://bucket/data.csv",
        ...     file_size=1024
        ... )

        Overwrite with partitioning:
        >>> options = TableWriteOptions(
        ...     mode="overwrite",
        ...     partition_by=["date", "region"]
        ... )
        >>> result = write_table(df, "catalog.schema.table", spark, options)

        Merge/upsert (Delta only):
        >>> options = TableWriteOptions(
        ...     format="delta",
        ...     mode="merge",
        ...     merge_keys=["id"]
        ... )
        >>> result = write_table(df, "my_table", spark, options)

        Batch processing:
        >>> results = []
        >>> for df in dataframes:
        ...     result = write_table(df, "my_table", spark)
        ...     results.append(result)
        >>> successes, failures = collect_write_results(results)

    Notes:
        - Merge mode only works with Delta format
        - Schema mode "strict" (default) requires exact schema match
        - Pre-write validation checks merge keys, partitions, and table existence
        - Returns TableWriteResult even on failure (check result.success)
    """

    start_time = time.time()

    if options is None:
        options = TableWriteOptions()

    if options.add_source_metadata:
        if source_file_path is None:
            exc = TableWritingException(
                "source_file_path required when add_source_metadata=True",
                "invalid_input",
                "Provide source_file_path parameter",
            )
            return _handle_error(
                TableWriteResult(
                    success=False,
                    table_name=table_name,
                    rows_written=0,
                    message=exc.message,
                    error=exc.to_dict(),
                    duration_seconds=time.time() - start_time,
                ),
                raise_on_error,
            )

        from .helpers import add_source_metadata

        df = add_source_metadata(
            df,
            source_file_path=source_file_path,
            file_size=file_size,
            column_names=options.source_metadata_columns,
            extra_columns=options.extra_metadata,
        )

    try:
        validate_write_options(df, table_name, spark, options)
    except TableWritingException as exc:
        return _handle_error(
            TableWriteResult(
                success=False,
                table_name=table_name,
                rows_written=0,
                message=exc.message,
                error=exc.to_dict(),
                duration_seconds=time.time() - start_time,
            ),
            raise_on_error,
        )

    try:
        handler = get_mode_handler(options.mode)
        if handler is None:
            exc = TableWritingException(
                message=f"Unsupported mode: {options.mode}",
                error_type="invalid_mode",
                details=f"Valid modes: {', '.join(MODES)}",
            )
            return _handle_error(
                TableWriteResult(
                    success=False,
                    table_name=table_name,
                    rows_written=0,
                    message=exc.message,
                    error=exc.to_dict(),
                    duration_seconds=time.time() - start_time,
                ),
                raise_on_error,
            )

        result = handler(df, table_name, spark, options, raise_on_error)
        result.duration_seconds = time.time() - start_time
        return result

    except TableWritingException:
        raise

    except Exception as e:
        logger.error("Write failed for %s: %s", table_name, e)
        return _handle_error(
            TableWriteResult(
                success=False,
                table_name=table_name,
                rows_written=0,
                error={
                    "error_type": "write_failed",
                    "message": str(e),
                    "details": type(e).__name__,
                },
                duration_seconds=time.time() - start_time,
            ),
            raise_on_error,
        )


def _write_append(
    df: DataFrame,
    table_name: str,
    spark: SparkSession,
    options: TableWriteOptions,
    raise_on_error: bool,
) -> TableWriteResult:
    row_count = df.count()
    format_handler = get_format_handler(options.format)
    format_handler.write(df, table_name, MODE_APPEND, options)
    return TableWriteResult(
        success=True,
        table_name=table_name,
        rows_written=row_count,
        message=f"Successfully appended to {table_name}",
    )


def _write_overwrite(
    df: DataFrame,
    table_name: str,
    spark: SparkSession,
    options: TableWriteOptions,
    raise_on_error: bool,
) -> TableWriteResult:
    row_count = df.count()
    format_handler = get_format_handler(options.format)
    format_handler.write(df, table_name, MODE_OVERWRITE, options)
    return TableWriteResult(
        success=True,
        table_name=table_name,
        rows_written=row_count,
        message=f"Successfully overwrote {table_name}",
    )


def _write_error_if_exists(
    df: DataFrame,
    table_name: str,
    spark: SparkSession,
    options: TableWriteOptions,
    raise_on_error: bool,
) -> TableWriteResult:
    row_count = df.count()
    format_handler = get_format_handler(options.format)
    # Validation already confirmed table doesn't exist, so append creates it
    format_handler.write(df, table_name, MODE_APPEND, options)
    return TableWriteResult(
        success=True,
        table_name=table_name,
        rows_written=row_count,
        message=f"Successfully created {table_name}",
    )


def _write_merge(
    df: DataFrame,
    table_name: str,
    spark: SparkSession,
    options: TableWriteOptions,
    raise_on_error: bool,
) -> TableWriteResult:

    row_count = df.count()

    if not spark.catalog.tableExists(table_name):
        if options.create_if_not_exists:
            return _write_append(df, table_name, spark, options, raise_on_error)
        else:
            exc = TableWritingException(
                "Table does not exist",
                "table_not_found",
                f"Table: {table_name}. Set create_if_not_exists=True.",
            )
            return _handle_error(
                TableWriteResult(
                    success=False,
                    table_name=table_name,
                    rows_written=0,
                    message=exc.message,
                    error=exc.to_dict(),
                ),
                raise_on_error,
            )

    delta_table = DeltaTable.forName(spark, table_name)

    merge_condition = " AND ".join(
        [f"target.{key} = source.{key}" for key in options.merge_keys]
    )

    merge_builder = (
        delta_table.alias("target")
        .merge(df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
    )
    merge_builder.execute()

    try:
        metrics = delta_table.history(1).select("operationMetrics").first()[0]
        rows_inserted = int(metrics.get("numTargetRowsInserted", 0))
        rows_updated = int(metrics.get("numTargetRowsUpdated", 0))
        rows_deleted = int(metrics.get("numTargetRowsDeleted", 0))

        return TableWriteResult(
            success=True,
            table_name=table_name,
            rows_written=rows_inserted + rows_updated,
            rows_updated=rows_updated,
            rows_deleted=rows_deleted,
            message=(
                f"Successfully merged into {table_name}: "
                f"{rows_inserted} inserted, {rows_updated} updated"
            ),
        )
    except Exception as e:
        logger.warning("Could not retrieve merge metrics: %s", e)
        return TableWriteResult(
            success=True,
            table_name=table_name,
            rows_written=row_count,
            message=f"Successfully merged into {table_name}",
        )


register_mode_handler(MODE_APPEND, _write_append)
register_mode_handler(MODE_OVERWRITE, _write_overwrite)
register_mode_handler(MODE_MERGE, _write_merge)
register_mode_handler(MODE_ERROR_IF_EXISTS, _write_error_if_exists)
