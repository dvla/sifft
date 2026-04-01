from dataclasses import dataclass
from enum import Enum
from typing import Any


class FileOperation(Enum):
    """Type of file operation performed."""

    MOVE = "move"
    LIST = "list"


@dataclass
class FileOperationResult:
    """Result of a file management operation.

    Attributes:
        operation: Type of operation performed
        source_path: Source file or directory path
        destination_path: Destination path (for move operations)
        success: Whether operation succeeded
        files: List of file info dicts (for list operations)
        error: Error details dict if failed
    """

    operation: FileOperation
    source_path: str
    destination_path: str | None = None
    success: bool = False
    files: list[dict[str, Any]] | None = None
    error: dict[str, Any] | None = None
