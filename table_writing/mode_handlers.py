"""Registry for write mode handlers."""

from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession

from .models import TableWriteOptions, TableWriteResult

ModeHandler = Callable[
    [DataFrame, str, SparkSession, TableWriteOptions, bool], TableWriteResult
]

_MODE_HANDLERS: dict[str, ModeHandler] = {}


def register_mode_handler(mode: str, handler: ModeHandler) -> None:
    """Register a custom mode handler.

    Args:
        mode: Mode name (e.g., "append", "merge", "custom")
        handler: Function with signature:
            (df, table_name, spark, options, raise_on_error) -> TableWriteResult

    Example:
        >>> def custom_upsert(df, table_name, spark, options, raise_on_error):
        ...     # Custom logic
        ...     return TableWriteResult(...)
        >>> register_mode_handler("upsert", custom_upsert)
    """
    _MODE_HANDLERS[mode] = handler


def unregister_mode_handler(mode: str) -> None:
    """Remove a mode handler from registry."""
    _MODE_HANDLERS.pop(mode, None)


def get_mode_handler(mode: str) -> ModeHandler | None:
    """Get handler for mode."""
    return _MODE_HANDLERS.get(mode)


def list_registered_modes() -> list[str]:
    """List all registered mode names."""
    return list(_MODE_HANDLERS.keys())
