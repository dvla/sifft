import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from .config import (
    EXCEL_EXTENSIONS,
    EXTENSION_DEFAULTS,
    NO_EXTENSION_DEFAULTS,
    TEXT_EXTENSIONS,
)
from .delimited_files import _process_delimited_file
from .excel_file import _process_excel_file
from .exceptions import FileProcessingException
from .helpers import check_file_exists
from .models import FileProcessingResult
from .registry import get_handler
from .tracking import TrackingMethod, check_already_processed, record_processed

logger = logging.getLogger(__name__)


def _handle_error(
    result: FileProcessingResult, raise_on_error: bool
) -> FileProcessingResult:
    """Handle error result - either raise exception or return result."""
    if raise_on_error and result.error:
        raise FileProcessingException(
            result.error["message"],
            result.error["error_type"],
            result.error.get("details", ""),
        )
    return result


def _validate_inputs(
    file_path: str, spark: SparkSession
) -> FileProcessingResult | None:
    """Validate inputs and return error result if invalid, None if valid."""
    if spark is None:
        exc = FileProcessingException(
            "SparkSession is required but was not provided",
            "missing_spark",
            "Provide a valid SparkSession instance",
        )
        return FileProcessingResult(
            success=False,
            file=file_path,
            message=exc.message,
            error=exc.to_dict(),
        )

    error = check_file_exists(file_path)
    if error:
        exc = FileProcessingException(
            error, "file_not_found", "Check file path and permissions"
        )
        return FileProcessingResult(
            success=False,
            file=file_path,
            message=exc.message,
            error=exc.to_dict(),
        )

    return None


def _select_handler(
    file_path: str, ext: str
) -> tuple[Callable | None, dict[str, Any], FileProcessingResult | None]:
    """Select appropriate handler based on file extension.

    Returns:
        Tuple of (handler_function, default_options, error_result)
        If error_result is not None, handler selection failed.
    """
    custom_handler = get_handler(ext)
    if custom_handler:
        return custom_handler, {}, None

    if ext in EXCEL_EXTENSIONS:
        return _process_excel_file, {}, None

    if ext in TEXT_EXTENSIONS:
        default_options = EXTENSION_DEFAULTS.get(ext, {})
        return _process_delimited_file, default_options, None

    if ext == "":
        logger.warning(
            "No file extension detected for %s, attempting delimited file parsing",
            file_path,
        )
        return (
            _process_delimited_file,
            NO_EXTENSION_DEFAULTS.copy(),
            None,
        )

    exc = FileProcessingException(
        f"Unsupported file extension: {ext}",
        "unsupported_format",
        f"Supported extensions: {', '.join(TEXT_EXTENSIONS | EXCEL_EXTENSIONS)}",
    )
    return (
        None,
        {},
        FileProcessingResult(
            success=False,
            file=file_path,
            message=exc.message,
            error=exc.to_dict(),
        ),
    )


def _merge_options(
    default_options: dict[str, Any], read_options: dict[str, Any] | None
) -> dict[str, Any]:
    """Merge default options with user-provided options."""
    options = default_options.copy()
    if read_options:
        options.update(read_options)
    return options


def process_file(
    file_path: str,
    spark: SparkSession,
    read_options: dict[str, Any] | None = None,
    metadata_path: str | None = None,
    raise_on_error: bool = False,
    tracking: TrackingMethod | None = None,
    tracking_location: str | None = None,
    storage_options: dict[str, Any] | None = None,
) -> FileProcessingResult:
    """Process a single file and return a FileProcessingResult.

    Args:
        file_path: Path to the file to process
        spark: Active SparkSession instance
        read_options: Optional dict to override parsing options:
            - delimiter: Column delimiter (e.g., '|', ',', '\t')
            - header: 'true' or 'false' for header row
            - max_file_size_mb: Maximum Excel file size in MB (default: 100)
            - max_corrupt_records_percent: Maximum corruption threshold (default: 0.1)
            - sheet_name: Excel sheet to read (default: 0)
            - encoding: File encoding (default: 'utf-8')
        metadata_path: Optional path to CSVW metadata file (if not in standard location)
        raise_on_error: If True, raise FileProcessingException instead of returning error result
        tracking: Tracking method - "marker_file" or "delta_table". If None, no tracking.
        tracking_location: Directory for markers or Delta table name. Required if tracking is set.
        storage_options: Optional dict for cloud storage authentication (S3, Azure, GCS)

    Returns:
        FileProcessingResult with success status, DataFrame (if successful), and metadata

    Raises:
        FileProcessingException: If raise_on_error=True and processing fails
        ValueError: If tracking is set but tracking_location is not provided
    """
    if tracking and not tracking_location:
        raise ValueError("tracking_location is required when tracking is enabled")

    if tracking:
        if check_already_processed(
            file_path, tracking, tracking_location, spark, storage_options
        ):
            logger.info("File already processed, skipping: %s", file_path)
            return FileProcessingResult(
                success=True,
                file=file_path,
                message="File already processed (skipped)",
                rows_processed=0,
            )

    validation_error = _validate_inputs(file_path, spark)
    if validation_error:
        return _handle_error(validation_error, raise_on_error)

    ext = Path(file_path).suffix.lower()
    handler, default_options, handler_error = _select_handler(file_path, ext)
    if handler_error:
        return _handle_error(handler_error, raise_on_error)

    options = _merge_options(default_options, read_options)

    if storage_options:
        options["storage_options"] = storage_options

    logger.info("Processing file: %s with options: %s", file_path, options)

    if handler == _process_delimited_file:
        result = handler(file_path, spark, options, metadata_path)
    else:
        result = handler(file_path, spark, options)

    if result.success:
        logger.info("Successfully processed %s: %s", file_path, result.message)
        if tracking:
            record_processed(
                file_path,
                tracking,
                tracking_location,
                spark,
                result.rows_processed,
                storage_options=storage_options,
            )
    else:
        logger.error("Failed to process %s: %s", file_path, result.message)
        return _handle_error(result, raise_on_error)

    return result


def process_files_batch(
    file_paths: list[str],
    spark: SparkSession,
    read_options: dict[str, Any] | None = None,
    raise_on_error: bool = False,
) -> list[FileProcessingResult]:
    """Process multiple files and return a list of FileProcessingResults.

    Args:
        file_paths: List of file paths to process
        spark: Active SparkSession instance
        read_options: Optional dict to override parsing options for all files
        raise_on_error: If True, raise FileProcessingException after processing all files if any failed

    Returns:
        List of FileProcessingResult objects, one per file

    Raises:
        FileProcessingException: If raise_on_error=True and any files failed processing
    """
    results = []

    for file_path in file_paths:
        result = process_file(file_path, spark, read_options)
        results.append(result)

    successful = sum(1 for r in results if r.success)
    failed = len(results) - successful

    logger.info(
        "Batch processing complete: %d/%d files successful", successful, len(results)
    )

    if raise_on_error and failed > 0:
        failures = [r for r in results if not r.success]
        exc = FileProcessingException(
            f"{failed} file(s) failed processing",
            "batch_processing_failed",
            f"Failed: {failed}/{len(results)} files",
        )
        exc.failures = [{"file": r.file, "error": r.error} for r in failures]
        raise exc

    return results


def process_directory(
    directory_path: str,
    spark: SparkSession,
    read_options: dict[str, Any] | None = None,
    raise_on_error: bool = False,
) -> list[FileProcessingResult]:
    """Process all files in a directory and return a list of FileProcessingResults.

    Args:
        directory_path: Path to the directory containing files to process
        spark: Active SparkSession instance
        read_options: Optional dict to override parsing options for all files
        raise_on_error: If True, raise FileProcessingException after processing all files if any failed

    Returns:
        List of FileProcessingResult objects, one per file in the directory

    Raises:
        FileProcessingException: If raise_on_error=True and any files failed processing
    """
    results = []

    directory = Path(directory_path)

    for file in directory.iterdir():
        if not file.is_file():
            continue

        result = process_file(str(file), spark, read_options)
        results.append(result)

    successful = sum(1 for r in results if r.success)
    failed = len(results) - successful

    logger.info(
        "Directory processing complete: %d/%d files successful",
        successful,
        len(results),
    )

    if raise_on_error and failed > 0:
        failures = [r for r in results if not r.success]
        exc = FileProcessingException(
            f"{failed} file(s) failed processing",
            "batch_processing_failed",
            f"Failed: {failed}/{len(results)} files",
        )
        exc.failures = [{"file": r.file, "error": r.error} for r in failures]
        raise exc

    return results
