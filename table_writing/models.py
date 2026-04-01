"""Data models for table writing operations."""

from dataclasses import dataclass
from typing import Any

from .config import FORMAT_DELTA, MODE_APPEND, SCHEMA_MODE_STRICT


@dataclass
class TableWriteOptions:
    """Configuration options for table writing.

    Attributes:
        format: Table format. Options: "delta", "parquet", "orc". Default: "delta"
        mode: Write mode. Options: "append", "overwrite", "merge", "errorIfExists". Default: "append"
            - append: Add rows to existing table
            - overwrite: Replace all data in table
            - merge: Upsert based on merge_keys (Delta only)
            - errorIfExists: Fail if table already exists
        partition_by: List of column names to partition by. Default: None
        schema_mode: Schema evolution mode. Options: "strict", "merge", "overwrite". Default: "strict"
            - strict: Fail if schemas don't match exactly
            - merge: Add new columns from DataFrame to table
            - overwrite: Replace table schema with DataFrame schema
        merge_keys: Column names to match on for merge mode. Required when mode="merge". Default: None
        create_if_not_exists: Auto-create table if it doesn't exist. Default: True
        add_source_metadata: Add source file metadata columns. Default: False
        source_metadata_columns: Custom column names for metadata. Default: None (uses dpmetadata_*)
        extra_metadata: Additional metadata columns as {column_name: value}. Default: None

    Examples:
        Default options (append to Delta table):
        >>> options = TableWriteOptions()

        Overwrite with partitioning:
        >>> options = TableWriteOptions(
        ...     mode="overwrite",
        ...     partition_by=["year", "month"]
        ... )

        Merge/upsert:
        >>> options = TableWriteOptions(
        ...     mode="merge",
        ...     merge_keys=["id"],
        ...     schema_mode="merge"
        ... )

        With source metadata:
        >>> options = TableWriteOptions(
        ...     add_source_metadata=True
        ... )

        Custom metadata column names:
        >>> options = TableWriteOptions(
        ...     add_source_metadata=True,
        ...     source_metadata_columns={
        ...         "directory_path": "my_source_dir",
        ...         "filename": "my_source_file",
        ...         "timestamp": "my_ingested_at",
        ...         "size": "my_file_size"
        ...     }
        ... )
    """

    format: str = FORMAT_DELTA
    mode: str = MODE_APPEND
    partition_by: list[str] | None = None
    schema_mode: str = SCHEMA_MODE_STRICT
    merge_keys: list[str] | None = None
    create_if_not_exists: bool = True
    add_source_metadata: bool = False
    source_metadata_columns: dict[str, str] | None = None
    extra_metadata: dict[str, Any] | None = None


@dataclass
class TableWriteResult:
    """Result of a table write operation.

    Attributes:
        success: True if write succeeded, False otherwise
        table_name: Name of table that was written to
        rows_written: Number of rows written (or attempted)
        rows_updated: Number of rows updated (merge mode only)
        rows_deleted: Number of rows deleted (merge mode only)
        message: Success or error message
        error: Error details dict if failed. Contains:
            - error_type: Category of error
            - message: Human-readable error message
            - details: Additional context
        duration_seconds: Time taken for write operation

    Examples:
        Check success:
        >>> result = write_table(df, "my_table", spark)
        >>> if result.success:
        ...     print(f"Wrote {result.rows_written} rows")
        ... else:
        ...     print(f"Failed: {result.error['message']}")

        Collect batch results:
        >>> results = [write_table(df, "table", spark) for df in dfs]
        >>> successes, failures = collect_write_results(results)
        >>> print(f"{len(successes)} succeeded, {len(failures)} failed")
    """

    success: bool
    table_name: str
    rows_written: int
    rows_updated: int | None = None
    rows_deleted: int | None = None
    message: str = ""
    error: dict[str, Any] | None = None
    duration_seconds: float = 0.0
