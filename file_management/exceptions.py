"""Custom exceptions for file management operations."""


class FileManagementException(Exception):
    """Exception raised when file management operation fails.

    Attributes:
        message: Human-readable error message
        error_type: Category of error
        details: Additional context or suggestions
    """

    def __init__(self, message: str, error_type: str, details: str = ""):
        super().__init__(message)
        self.message = message
        self.error_type = error_type
        self.details = details

    def to_dict(self) -> dict[str, str]:
        return {
            "error_type": self.error_type,
            "message": self.message,
            "details": self.details,
        }
