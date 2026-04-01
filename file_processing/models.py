"""Data models for file processing results."""

from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame


@dataclass
class FileProcessingResult:
    """Result of file processing operation.

    Attributes:
        success: Whether file was successfully processed
        file: Path to the processed file
        dataframe: Parsed Spark DataFrame (None if processing failed)
        message: Human-readable status message
        error: Error details dict if failed (None if successful)
        rows_processed: Total number of rows read from file
        rows_corrupt: Number of rows that couldn't be parsed correctly
        metadata: CSVW metadata dict if used (None for inference mode)
    """

    success: bool
    file: str
    dataframe: DataFrame | None = None
    message: str = ""
    error: dict[str, Any] | None = None
    rows_processed: int = 0
    rows_corrupt: int = 0
    metadata: dict[str, Any] | None = None
    checksum: str | None = None

    def __repr__(self) -> str:
        """Human-readable summary of processing result."""
        status = "SUCCESS" if self.success else "FAILED"
        return (
            f"FileProcessingResult({status}, {self.file}, "
            f"rows={self.rows_processed}, corrupt={self.rows_corrupt})"
        )
