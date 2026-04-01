"""Helper functions for table writing operations."""

from pathlib import Path
from typing import Any

import fsspec
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit

from .models import TableWriteResult


def split_write_results(
    results: list[TableWriteResult],
) -> tuple[list[TableWriteResult], list[TableWriteResult]]:
    """Split write results into successes and failures.

    Args:
        results: List of TableWriteResult objects

    Returns:
        Tuple of (successes, failures)
    """
    successes = [r for r in results if r.success]
    failures = [r for r in results if not r.success]
    return successes, failures


def add_source_metadata(
    df: DataFrame,
    source_file_path: str,
    file_size: int | None = None,
    column_names: dict[str, str] | None = None,
    extra_columns: dict[str, Any] | None = None,
) -> DataFrame:
    """Add source file metadata columns to DataFrame.

    Args:
        df: DataFrame to add metadata to
        source_file_path: Full path to source file
        file_size: Size of source file in bytes (optional)
        column_names: Custom column names. Keys: directory_path, filename, timestamp, size
                     Default: dpmetadata_directory_path, dpmetadata_filename, etc.
        extra_columns: Additional metadata columns to add as {column_name: value}

    Returns:
        DataFrame with metadata columns added
    """
    default_names = {
        "directory_path": "dp_metadata_directory_path",
        "filename": "dp_metadata_filename",
        "timestamp": "dp_metadata_timestamp",
        "size": "dp_metadata_size",
    }

    col_names = {**default_names, **(column_names or {})}

    protocol, path_part = fsspec.core.split_protocol(source_file_path)
    if protocol:
        last_slash = source_file_path.rfind("/")
        directory = source_file_path[:last_slash] if last_slash != -1 else ""
        filename = (
            source_file_path[last_slash + 1 :] if last_slash != -1 else source_file_path
        )
    else:
        path = Path(source_file_path)
        directory = str(path.parent) if path.parent != Path(".") else ""
        filename = path.name

    df = df.withColumn(col_names["directory_path"], lit(directory))
    df = df.withColumn(col_names["filename"], lit(filename))
    df = df.withColumn(col_names["timestamp"], current_timestamp())

    if file_size is not None:
        df = df.withColumn(col_names["size"], lit(file_size))

    for col_name, value in (extra_columns or {}).items():
        df = df.withColumn(col_name, lit(value))

    return df
