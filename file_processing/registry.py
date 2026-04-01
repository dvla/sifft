"""Handler registry for extensible file format support.

Allows users to register custom file format handlers for any extension.
"""

from collections.abc import Callable
from typing import Any

from pyspark.sql import SparkSession

from .models import FileProcessingResult

FileHandler = Callable[[str, SparkSession, dict[str, Any]], FileProcessingResult]

_HANDLER_REGISTRY: dict[str, list[tuple[int, FileHandler]]] = {}


def register_handler(extension: str, handler: FileHandler, priority: int = 0) -> None:
    """Register a custom file format handler.

    Args:
        extension: File extension (e.g., ".dat", ".json", ".csv")
        handler: Function that processes the file and returns FileProcessingResult
        priority: Higher priority handlers are tried first (default: 0)

    Example:
        >>> def my_dat_handler(file_path, spark, options):
        ...     df = spark.read.format("custom").load(file_path)
        ...     return FileProcessingResult(success=True, dataframe=df, ...)
        >>>
        >>> register_handler(".dat", my_dat_handler, priority=10)
    """
    extension = extension.lower()
    if extension not in _HANDLER_REGISTRY:
        _HANDLER_REGISTRY[extension] = []

    _HANDLER_REGISTRY[extension].append((priority, handler))
    _HANDLER_REGISTRY[extension].sort(key=lambda x: x[0], reverse=True)


def get_handler(extension: str) -> FileHandler | None:
    """Get the highest priority handler for an extension.

    Args:
        extension: File extension to look up

    Returns:
        Handler function if registered, None otherwise
    """
    extension = extension.lower()
    handlers = _HANDLER_REGISTRY.get(extension, [])
    if handlers:
        return handlers[0][1]
    return None


def unregister_handler(extension: str, handler: FileHandler | None = None) -> None:
    """Unregister a handler for an extension.

    Args:
        extension: File extension
        handler: Specific handler to remove, or None to remove all handlers for extension
    """
    extension = extension.lower()
    if extension not in _HANDLER_REGISTRY:
        return

    if handler is None:
        del _HANDLER_REGISTRY[extension]
    else:
        _HANDLER_REGISTRY[extension] = [
            (p, h) for p, h in _HANDLER_REGISTRY[extension] if h != handler
        ]
        if not _HANDLER_REGISTRY[extension]:
            del _HANDLER_REGISTRY[extension]


def list_registered_extensions() -> list[str]:
    """Get list of all registered extensions.

    Returns:
        List of file extensions with custom handlers
    """
    return list(_HANDLER_REGISTRY.keys())
