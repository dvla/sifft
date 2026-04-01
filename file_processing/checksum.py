import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .storage import detect_protocol, get_filesystem

logger = logging.getLogger(__name__)


@dataclass
class TrackingResult:
    """Result of tracking operation (marker file or delta table).

    Attributes:
        success: Whether tracking operation succeeded
        file_path: Path to the data file
        location: Marker path or table name
        error: Error details dict if failed
    """

    success: bool
    file_path: str
    location: str | None = None
    error: dict[str, Any] | None = None


def _get_fs(path: str, storage_options: dict[str, Any] | None = None):
    return get_filesystem(path, storage_options)


def _sanitise_path_segment(value: str) -> str:
    return re.sub(r"[^\w\-.]", "_", value)


def _get_marker_path(marker_dir: str, checksum: str, file_path: str) -> str:
    prefix1 = checksum[:2]
    prefix2 = checksum[2:4]
    safe_name = _sanitise_path_segment(file_path)
    return (
        f"{marker_dir.rstrip('/')}/{prefix1}/{prefix2}/{checksum}_{safe_name}.processed"
    )


def compute_file_checksum(
    file_path: str,
    algo: str = "md5",
    storage_options: dict[str, Any] | None = None,
) -> str:
    """Compute checksum of file contents for duplicate detection.

    Args:
        file_path: Path to file (local or cloud URI)
        algo: Hash algorithm - "md5" or "sha256"
        storage_options: Optional dict for cloud authentication

    Returns:
        Hex digest string
    """
    if algo.lower() == "md5":
        hasher = hashlib.md5()
    elif algo.lower() == "sha256":
        hasher = hashlib.sha256()
    else:
        raise ValueError("Unsupported algo. Use 'md5' or 'sha256'.")

    fs = _get_fs(file_path, storage_options)
    with fs.open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)

    return hasher.hexdigest()


def is_file_processed(
    file_path: str,
    tracking_location: str,
    storage_options: dict[str, Any] | None = None,
) -> bool:
    """Check if file has already been processed (marker file tracking).

    Args:
        file_path: Path to file to check
        tracking_location: Directory for marker files
        storage_options: Optional dict for cloud authentication

    Returns:
        True if this exact file+content was previously processed
    """
    checksum = compute_file_checksum(file_path, storage_options=storage_options)
    marker = _get_marker_path(tracking_location, checksum, file_path)

    fs = _get_fs(marker, storage_options)
    return fs.exists(marker)


def clear_marker(
    file_path: str,
    tracking_location: str,
    storage_options: dict[str, Any] | None = None,
) -> bool:
    """Remove marker file to allow reprocessing.

    Args:
        file_path: Path to the data file
        tracking_location: Directory for marker files
        storage_options: Optional dict for cloud authentication

    Returns:
        True if marker was removed, False if not found
    """
    checksum = compute_file_checksum(file_path, storage_options=storage_options)
    marker = _get_marker_path(tracking_location, checksum, file_path)

    fs = _get_fs(marker, storage_options)

    try:
        fs.rm(marker)
        return True
    except FileNotFoundError:
        return False
    except OSError:
        logger.warning("Could not remove marker: %s", marker)
        return False


def mark_file_processed(
    file_path: str,
    tracking_location: str,
    rows_processed: int = 0,
    raise_on_error: bool = False,
    storage_options: dict[str, Any] | None = None,
) -> TrackingResult:
    """Create marker file to record successful processing.

    Args:
        file_path: Path to processed file
        tracking_location: Directory for marker files
        rows_processed: Number of rows processed
        raise_on_error: If True, raises FileProcessingException on failure
        storage_options: Optional dict for cloud authentication

    Returns:
        TrackingResult with success status
    """
    from .exceptions import FileProcessingException

    checksum = compute_file_checksum(file_path, storage_options=storage_options)
    marker = _get_marker_path(tracking_location, checksum, file_path)

    fs = _get_fs(marker, storage_options)

    try:
        if detect_protocol(marker) == "file":
            Path(marker).parent.mkdir(parents=True, exist_ok=True)

        data = {
            "source_file": file_path,
            "checksum": checksum,
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "rows_processed": rows_processed,
        }
        with fs.open(marker, "w") as f:
            json.dump(data, f)

        return TrackingResult(success=True, file_path=file_path, location=marker)

    except OSError as e:
        exc = FileProcessingException(
            f"Failed to write marker file: {e}",
            "marker_write_error",
            f"Check permissions for {marker}",
        )
        logger.warning("Could not create marker for %s: %s", file_path, e)

        if raise_on_error:
            raise exc from e

        return TrackingResult(
            success=False,
            file_path=file_path,
            location=marker,
            error=exc.to_dict(),
        )
