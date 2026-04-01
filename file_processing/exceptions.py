"""Custom exceptions for file processing."""


class FileProcessingException(Exception):
    """Base exception for file processing errors.

    Attributes:
        message: Human-readable error message
        error_type: Category of error (e.g., 'file_not_found', 'processing_error')
        details: Additional context or suggestions
    """

    def __init__(self, message: str, error_type: str, details: str = ""):
        super().__init__(message)
        self.message = message
        self.error_type = error_type
        self.details = details
        self.failures: list[dict[str, str]] | None = None

    def to_dict(self) -> dict[str, str]:
        return {
            "error_type": self.error_type,
            "message": self.message,
            "details": self.details,
        }
