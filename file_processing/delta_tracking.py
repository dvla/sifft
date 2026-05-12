"""Delta table tracking for file processing deduplication."""

import logging
import re
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.utils import AnalysisException

from .checksum import TrackingResult, compute_file_checksum

logger = logging.getLogger(__name__)

# Duplicated from table_writing.validation to avoid cross-module dependency.
# Both modules are independent packages.
_VALID_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z0-9_\.]+$")


def _validate_table_name(table_name: str) -> None:
    if not _VALID_IDENTIFIER_PATTERN.match(table_name):
        from .exceptions import FileProcessingException

        raise FileProcessingException(
            f"Invalid table name: {table_name}",
            "invalid_input",
            "Only alphanumeric, underscore, and dot allowed",
        )


def _ensure_tracking_table(spark: SparkSession, table_name: str) -> None:
    _validate_table_name(table_name)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            checksum STRING,
            source_file STRING,
            processed_at TIMESTAMP,
            rows_processed INT,
            is_active BOOLEAN,
            cleared_at TIMESTAMP
        )
        USING DELTA
    """)


def _active_record_filter(checksum: str, file_path: str):
    return (
        (col("checksum") == checksum)
        & (col("source_file") == file_path)
        & (col("is_active") == True)  # noqa: E712
    )


def is_file_processed_delta(
    file_path: str,
    tracking_location: str,
    spark: SparkSession,
    storage_options: dict[str, Any] | None = None,
) -> bool:
    """Check if file has already been processed (Delta table tracking).

    Args:
        file_path: Path to file to check
        tracking_location: Delta table name (e.g., catalog.schema.tracking)
        spark: SparkSession
        storage_options: Optional dict for cloud authentication

    Returns:
        True if this exact file+content was previously processed
    """
    checksum = compute_file_checksum(file_path, storage_options=storage_options)

    try:
        return (
            spark.table(tracking_location)
            .filter(_active_record_filter(checksum, file_path))
            .limit(1)
            .count()
            > 0
        )
    except AnalysisException:
        return False


def clear_tracking_delta(
    file_path: str,
    tracking_location: str,
    spark: SparkSession,
    storage_options: dict[str, Any] | None = None,
) -> bool:
    """Remove tracking record to allow reprocessing.

    Uses soft delete — sets is_active=False rather than removing the row.

    Args:
        file_path: Path to the data file
        tracking_location: Delta table name
        spark: SparkSession
        storage_options: Optional dict for cloud authentication

    Returns:
        True if record was cleared, False if not found
    """
    from delta.tables import DeltaTable

    checksum = compute_file_checksum(file_path, storage_options=storage_options)

    try:
        delta_table = DeltaTable.forName(spark, tracking_location)

        delta_table.update(
            condition=_active_record_filter(checksum, file_path),
            set={
                "is_active": lit(False),
                "cleared_at": current_timestamp(),
            },
        )

        # Check if any active records remain (if none, the update worked)
        still_active = (
            spark.table(tracking_location)
            .filter(_active_record_filter(checksum, file_path))
            .count()
        )
        return still_active == 0

    except AnalysisException:
        return False


def mark_file_processed_delta(
    file_path: str,
    tracking_location: str,
    spark: SparkSession,
    rows_processed: int = 0,
    raise_on_error: bool = False,
    storage_options: dict[str, Any] | None = None,
) -> TrackingResult:
    """Record successful processing in Delta table.

    Args:
        file_path: Path to processed file
        tracking_location: Delta table name
        spark: SparkSession
        rows_processed: Number of rows processed
        raise_on_error: If True, raises FileProcessingException on failure
        storage_options: Optional dict for cloud authentication

    Returns:
        TrackingResult with success status
    """
    from .exceptions import FileProcessingException

    checksum = compute_file_checksum(file_path, storage_options=storage_options)
    processed_at = datetime.now(timezone.utc)

    try:
        _ensure_tracking_table(spark, tracking_location)

        row = Row(
            checksum=checksum,
            source_file=file_path,
            processed_at=processed_at,
            rows_processed=rows_processed,
            is_active=True,
            cleared_at=None,
        )
        schema = spark.table(tracking_location).schema
        spark.createDataFrame([row], schema=schema).write.insertInto(tracking_location)

        return TrackingResult(
            success=True,
            file_path=file_path,
            location=tracking_location,
        )

    except Exception as e:
        exc = FileProcessingException(
            f"Failed to write tracking record: {e}",
            "tracking_write_error",
            f"Check permissions for {tracking_location}",
        )
        logger.warning("Could not create tracking record for %s: %s", file_path, e)

        if raise_on_error:
            raise exc from e

        return TrackingResult(
            success=False,
            file_path=file_path,
            location=tracking_location,
            error=exc.to_dict(),
        )
