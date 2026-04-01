"""Table writing module.

Provides functionality for writing DataFrames to Databricks tables.
"""

from .config import (
    FORMAT_DELTA,
    FORMAT_ORC,
    FORMAT_PARQUET,
    FORMATS,
    MODE_APPEND,
    MODE_ERROR_IF_EXISTS,
    MODE_MERGE,
    MODE_OVERWRITE,
    MODES,
    SCHEMA_MODE_MERGE,
    SCHEMA_MODE_OVERWRITE,
    SCHEMA_MODE_STRICT,
    SCHEMA_MODES,
)
from .exceptions import TableWritingException
from .format_handlers import get_format_handler, register_format_handler
from .helpers import add_source_metadata, split_write_results
from .mode_handlers import (
    list_registered_modes,
    register_mode_handler,
    unregister_mode_handler,
)
from .models import TableWriteOptions, TableWriteResult
from .writer import write_table

__all__ = [
    "TableWriteOptions",
    "TableWriteResult",
    "write_table",
    "TableWritingException",
    "split_write_results",
    "add_source_metadata",
    "register_mode_handler",
    "unregister_mode_handler",
    "list_registered_modes",
    "register_format_handler",
    "get_format_handler",
    "FORMATS",
    "FORMAT_DELTA",
    "FORMAT_PARQUET",
    "FORMAT_ORC",
    "MODES",
    "MODE_APPEND",
    "MODE_OVERWRITE",
    "MODE_MERGE",
    "MODE_ERROR_IF_EXISTS",
    "SCHEMA_MODES",
    "SCHEMA_MODE_STRICT",
    "SCHEMA_MODE_MERGE",
    "SCHEMA_MODE_OVERWRITE",
]
