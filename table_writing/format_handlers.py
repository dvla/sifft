"""Format-specific write handlers."""

from typing import Protocol

from pyspark.sql import DataFrame

from .models import TableWriteOptions


class FormatHandler(Protocol):
    """Protocol for format-specific write handlers."""

    def supports_merge(self) -> bool: ...

    def write(
        self,
        df: DataFrame,
        table_name: str,
        mode: str,
        options: TableWriteOptions,
    ) -> None: ...


class DeltaFormatHandler:
    def supports_merge(self) -> bool:
        return True

    def write(
        self,
        df: DataFrame,
        table_name: str,
        mode: str,
        options: TableWriteOptions,
    ) -> None:
        writer = df.write.format("delta").mode(mode)
        if options.partition_by:
            writer = writer.partitionBy(*options.partition_by)

        if mode == "append" and options.schema_mode == "merge":
            writer = writer.option("mergeSchema", "true")
        elif mode == "overwrite" and options.schema_mode == "overwrite":
            writer = writer.option("overwriteSchema", "true")

        writer.saveAsTable(table_name)


class ParquetFormatHandler:
    def supports_merge(self) -> bool:
        return False

    def write(
        self,
        df: DataFrame,
        table_name: str,
        mode: str,
        options: TableWriteOptions,
    ) -> None:
        writer = df.write.format("parquet").mode(mode)
        if options.partition_by:
            writer = writer.partitionBy(*options.partition_by)
        writer.saveAsTable(table_name)


class OrcFormatHandler:
    def supports_merge(self) -> bool:
        return False

    def write(
        self,
        df: DataFrame,
        table_name: str,
        mode: str,
        options: TableWriteOptions,
    ) -> None:
        writer = df.write.format("orc").mode(mode)
        if options.partition_by:
            writer = writer.partitionBy(*options.partition_by)
        writer.saveAsTable(table_name)


_FORMAT_HANDLERS: dict[str, FormatHandler] = {
    "delta": DeltaFormatHandler(),
    "parquet": ParquetFormatHandler(),
    "orc": OrcFormatHandler(),
}


def get_format_handler(format_name: str) -> FormatHandler | None:
    """Get handler for a table format.

    Args:
        format_name: Format name (e.g., "delta", "parquet", "orc")

    Returns:
        FormatHandler if registered, None otherwise
    """
    return _FORMAT_HANDLERS.get(format_name)


def register_format_handler(format_name: str, handler: FormatHandler) -> None:
    """Register a custom format handler.

    Args:
        format_name: Format name to register
        handler: Handler implementing FormatHandler protocol
    """
    _FORMAT_HANDLERS[format_name] = handler
