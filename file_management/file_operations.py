"""File management operations with consistent error handling.

Supports local filesystem and cloud storage (S3, Azure, GCS) via fsspec.
"""

from typing import Any

import fsspec

from .exceptions import FileManagementException
from .models import FileOperation, FileOperationResult


def _get_filesystem(path: str, storage_options: dict[str, Any] | None = None):
    # Duplicated from file_processing.storage to avoid cross-module dependency.
    # Both modules are independent packages.
    if path.startswith(("s3://", "az://", "gs://", "hdfs://")):
        protocol = path.split("://")[0]
    else:
        protocol = "file"
    return fsspec.filesystem(protocol, **(storage_options or {}))


def safe_move(
    src_path: str,
    dst_path: str,
    storage_options: dict[str, Any] | None = None,
    raise_on_error: bool = False,
) -> FileOperationResult:
    """Move a file safely with sanity checks.

    Supports local and cloud storage paths:
    - Local: /path/to/file.csv
    - S3: s3://bucket/path/to/file.csv
    - Azure: az://container/path/to/file.csv
    - GCS: gs://bucket/path/to/file.csv

    Args:
        src_path: Source file path (local or cloud URI)
        dst_path: Destination file path (local or cloud URI)
        storage_options: Optional dict of storage-specific options for authentication
            - S3: {"key": "ACCESS_KEY", "secret": "SECRET_KEY", "token": "SESSION_TOKEN"}
            - Azure: {"account_name": "name", "account_key": "key"} or {"sas_token": "token"}
            - GCS: {"token": "path/to/credentials.json"}
        raise_on_error: If True, raises FileManagementException on failure

    Returns:
        FileOperationResult with success status

    Raises:
        FileManagementException: If raise_on_error=True and operation fails

    Example:
        >>> result = safe_move("input/data.csv", "processed/data.csv")
        >>> if result.success:
        ...     print(f"Moved to {result.destination_path}")
    """
    try:
        fs = _get_filesystem(src_path, storage_options)

        if not fs.exists(src_path):
            exc = FileManagementException(
                f"Source file does not exist: {src_path}",
                "file_not_found",
                "Check file path and permissions",
            )
            result = FileOperationResult(
                operation=FileOperation.MOVE,
                source_path=src_path,
                destination_path=dst_path,
                success=False,
                error=exc.to_dict(),
            )
            if raise_on_error:
                raise exc
            return result

        if fs.exists(dst_path):
            exc = FileManagementException(
                f"Destination file already exists: {dst_path}",
                "destination_exists",
                "Remove existing file or choose different destination",
            )
            result = FileOperationResult(
                operation=FileOperation.MOVE,
                source_path=src_path,
                destination_path=dst_path,
                success=False,
                error=exc.to_dict(),
            )
            if raise_on_error:
                raise exc
            return result

        last_slash = dst_path.rfind("/")
        dst_dir = dst_path[:last_slash] if last_slash > 0 else ""
        if dst_dir:
            fs.makedirs(dst_dir, exist_ok=True)

        fs.mv(src_path, dst_path)

        return FileOperationResult(
            operation=FileOperation.MOVE,
            source_path=src_path,
            destination_path=dst_path,
            success=True,
        )

    except FileManagementException:
        raise
    except Exception as e:
        exc = FileManagementException(
            f"Failed to move file: {str(e)}", "move_failed", type(e).__name__
        )
        result = FileOperationResult(
            operation=FileOperation.MOVE,
            source_path=src_path,
            destination_path=dst_path,
            success=False,
            error=exc.to_dict(),
        )
        if raise_on_error:
            raise exc from e
        return result


def list_files_in_directory(
    directory: str,
    storage_options: dict[str, Any] | None = None,
    raise_on_error: bool = False,
) -> FileOperationResult:
    """List all files in a directory (non-recursive).

    Supports local and cloud storage paths:
    - Local: /path/to/directory
    - S3: s3://bucket/path/
    - Azure: az://container/path/
    - GCS: gs://bucket/path/

    Args:
        directory: Directory path to list (local or cloud URI)
        storage_options: Optional dict of storage-specific options for authentication
            - S3: {"key": "ACCESS_KEY", "secret": "SECRET_KEY", "token": "SESSION_TOKEN"}
            - Azure: {"account_name": "name", "account_key": "key"} or {"sas_token": "token"}
            - GCS: {"token": "path/to/credentials.json"}
        raise_on_error: If True, raises FileManagementException on failure

    Returns:
        FileOperationResult with files list

    Raises:
        FileManagementException: If raise_on_error=True and operation fails

    Example:
        >>> result = list_files_in_directory("input/")
        >>> if result.success:
        ...     for file in result.files:
        ...         print(f"{file['name']}: {file['size']} bytes")
    """
    try:
        fs = _get_filesystem(directory, storage_options)

        if not fs.exists(directory):
            exc = FileManagementException(
                f"Directory does not exist: {directory}",
                "directory_not_found",
                "Check directory path and permissions",
            )
            result = FileOperationResult(
                operation=FileOperation.LIST,
                source_path=directory,
                success=False,
                error=exc.to_dict(),
            )
            if raise_on_error:
                raise exc
            return result

        files = []
        for item in fs.ls(directory, detail=True):
            if item["type"] == "file":
                files.append(
                    {
                        "name": item["name"].rsplit("/", 1)[-1],
                        "path": item["name"],
                        "size": item.get("size", 0),
                    }
                )

        return FileOperationResult(
            operation=FileOperation.LIST,
            source_path=directory,
            success=True,
            files=files,
        )

    except FileManagementException:
        raise
    except Exception as e:
        exc = FileManagementException(
            f"Failed to list directory: {str(e)}", "list_failed", type(e).__name__
        )
        result = FileOperationResult(
            operation=FileOperation.LIST,
            source_path=directory,
            success=False,
            error=exc.to_dict(),
        )
        if raise_on_error:
            raise exc from e
        return result
