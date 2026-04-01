"""Validation logic for pre-write checks."""

import re

from pyspark.sql import DataFrame, SparkSession

from .config import FORMATS, MODES, SCHEMA_MODES
from .exceptions import TableWritingException
from .format_handlers import get_format_handler
from .mode_handlers import get_mode_handler
from .models import TableWriteOptions

_MAX_IDENTIFIER_LENGTH = 255
_VALID_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z0-9_\.]+$")


def _raise(error_type: str, message: str, details: str = "") -> None:
    """Helper to raise standardized exceptions."""
    raise TableWritingException(message=message, error_type=error_type, details=details)


def _validate_identifier(name: str, field: str) -> None:
    if len(name) > _MAX_IDENTIFIER_LENGTH:
        _raise(
            "invalid_input",
            f"{field} exceeds maximum length of {_MAX_IDENTIFIER_LENGTH}",
            f"Length: {len(name)}",
        )

    if not _VALID_IDENTIFIER_PATTERN.match(name):
        _raise(
            "invalid_input",
            f"{field} contains invalid characters",
            "Only alphanumeric, underscore, and dot allowed",
        )


def _validate_inputs(
    df: DataFrame, table_name: str, spark: SparkSession, **kwargs
) -> None:
    if df is None:
        _raise(
            "invalid_input",
            "DataFrame cannot be None",
            "Provide a valid Spark DataFrame",
        )
    if not isinstance(df, DataFrame):
        _raise(
            "invalid_input",
            f"Expected DataFrame, got {type(df).__name__}",
            "Provide a valid Spark DataFrame",
        )
    if table_name is None or not isinstance(table_name, str):
        _raise(
            "invalid_input",
            "table_name must be a non-None string",
            f"Got: {type(table_name).__name__}",
        )
    if not table_name.strip():
        _raise(
            "invalid_input",
            "table_name cannot be empty or whitespace",
            "Provide a valid table name",
        )
    if spark is None:
        _raise(
            "invalid_input",
            "SparkSession cannot be None",
            "Provide a valid SparkSession",
        )

    _validate_identifier(table_name, "table_name")


def _validate_dataframe_not_empty(df: DataFrame, **kwargs) -> None:
    if len(df.columns) == 0:
        _raise(
            "invalid_input",
            "DataFrame has no columns",
            "Cannot write empty schema",
        )


def _validate_format(options: TableWriteOptions, **kwargs) -> None:
    if options.format not in FORMATS and get_format_handler(options.format) is None:
        _raise(
            "invalid_format",
            f"Invalid format: {options.format}",
            f"Format must be one of: {', '.join(FORMATS)}",
        )


def _validate_mode(options: TableWriteOptions, **kwargs) -> None:
    if options.mode not in MODES and get_mode_handler(options.mode) is None:
        _raise(
            "invalid_mode",
            f"Invalid mode: {options.mode}",
            f"Mode must be one of: {', '.join(MODES)}",
        )


def _validate_merge_keys(df: DataFrame, options: TableWriteOptions, **kwargs) -> None:
    if options.mode != "merge":
        return

    if options.format != "delta":
        _raise(
            "invalid_mode",
            "Merge mode only supported for Delta format",
            f"Current format: {options.format}",
        )

    if options.merge_keys is None:
        _raise(
            "merge_key_missing",
            "merge_keys required for merge mode",
            "Specify merge_keys in TableWriteOptions",
        )

    if not isinstance(options.merge_keys, list):
        _raise(
            "invalid_input",
            f"merge_keys must be a list, got {type(options.merge_keys).__name__}",
            "Provide list of column names",
        )

    if len(options.merge_keys) == 0:
        _raise(
            "merge_key_missing",
            "merge_keys cannot be empty list",
            "Specify at least one merge key",
        )

    for key in options.merge_keys:
        if not isinstance(key, str):
            _raise(
                "invalid_input",
                f"merge_keys must contain strings, found {type(key).__name__}",
                f"Invalid key: {key}",
            )
        _validate_identifier(key, "merge_key")
        if key not in df.columns:
            _raise(
                "merge_key_missing",
                f"Merge key not found: {key}",
                f"Available columns: {df.columns}",
            )


def _validate_partitions(df: DataFrame, options: TableWriteOptions, **kwargs) -> None:
    if not options.partition_by:
        return

    if not isinstance(options.partition_by, list):
        _raise(
            "invalid_input",
            "partition_by must be a list",
            "Provide list of column names",
        )

    for partition in options.partition_by:
        if not isinstance(partition, str):
            _raise(
                "invalid_input",
                "partition_by must contain strings",
                f"Invalid partition: {partition}",
            )

        _validate_identifier(partition, "partition column")

        if partition not in df.columns:
            _raise(
                "partition_error",
                f"Partition column not found: {partition}",
                f"Available columns: {df.columns}",
            )


def _validate_table_exists(
    table_name: str, spark: SparkSession, options: TableWriteOptions, **kwargs
) -> None:
    if options.mode == "errorIfExists" and spark.catalog.tableExists(table_name):
        _raise(
            "table_exists",
            f"Table already exists: {table_name}",
            "Use different mode or drop table first",
        )


def _validate_schema_mode(options: TableWriteOptions, **kwargs) -> None:
    if options.schema_mode not in SCHEMA_MODES:
        _raise(
            "invalid_schema_mode",
            f"Invalid schema_mode: {options.schema_mode}",
            f"schema_mode must be one of: {', '.join(SCHEMA_MODES)}",
        )


def validate_write_options(
    df: DataFrame, table_name: str, spark: SparkSession, options: TableWriteOptions
) -> None:
    """Run all validators, raising TableWritingException if any fail."""
    validators = [
        _validate_inputs,
        _validate_dataframe_not_empty,
        _validate_format,
        _validate_mode,
        _validate_merge_keys,
        _validate_partitions,
        _validate_table_exists,
        _validate_schema_mode,
    ]

    for validator in validators:
        validator(df=df, table_name=table_name, spark=spark, options=options)
