"""Unified tracking interface for file processing deduplication."""

from typing import Any, Literal

from pyspark.sql import SparkSession

from .checksum import (
    TrackingResult,
    clear_marker,
    is_file_processed,
    mark_file_processed,
)
from .delta_tracking import (
    clear_tracking_delta,
    is_file_processed_delta,
    mark_file_processed_delta,
)

TrackingMethod = Literal["marker_file", "delta_table"]


def check_already_processed(
    file_path: str,
    tracking: TrackingMethod,
    tracking_location: str,
    spark: SparkSession,
    storage_options: dict[str, Any] | None = None,
) -> bool:
    """Check if file has already been processed.

    Args:
        file_path: Path to file to check
        tracking: Tracking method - "marker_file" or "delta_table"
        tracking_location: Marker directory or Delta table name
        spark: SparkSession (required for delta_table)
        storage_options: Optional dict for cloud authentication

    Returns:
        True if this exact file+content was previously processed
    """
    if tracking == "marker_file":
        return is_file_processed(file_path, tracking_location, storage_options)
    return is_file_processed_delta(file_path, tracking_location, spark, storage_options)


def record_processed(
    file_path: str,
    tracking: TrackingMethod,
    tracking_location: str,
    spark: SparkSession,
    rows_processed: int = 0,
    raise_on_error: bool = False,
    storage_options: dict[str, Any] | None = None,
) -> TrackingResult:
    """Record successful file processing.

    Args:
        file_path: Path to processed file
        tracking: Tracking method - "marker_file" or "delta_table"
        tracking_location: Marker directory or Delta table name
        spark: SparkSession (required for delta_table)
        rows_processed: Number of rows processed
        raise_on_error: If True, raises FileProcessingException on failure
        storage_options: Optional dict for cloud authentication

    Returns:
        TrackingResult with success status
    """
    if tracking == "marker_file":
        return mark_file_processed(
            file_path,
            tracking_location,
            rows_processed,
            raise_on_error,
            storage_options,
        )
    return mark_file_processed_delta(
        file_path,
        tracking_location,
        spark,
        rows_processed,
        raise_on_error,
        storage_options,
    )


def clear_tracking(
    file_path: str,
    tracking: TrackingMethod,
    tracking_location: str,
    spark: SparkSession,
    storage_options: dict[str, Any] | None = None,
) -> bool:
    """Clear tracking record to allow reprocessing.

    Args:
        file_path: Path to the data file
        tracking: Tracking method - "marker_file" or "delta_table"
        tracking_location: Marker directory or Delta table name
        spark: SparkSession (required for delta_table)
        storage_options: Optional dict for cloud authentication

    Returns:
        True if record was removed, False if not found
    """
    if tracking == "marker_file":
        return clear_marker(file_path, tracking_location, storage_options)
    return clear_tracking_delta(file_path, tracking_location, spark, storage_options)
