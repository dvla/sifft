"""Tests for exception classes across all modules."""


class TestFileProcessingException:
    """Tests for FileProcessingException."""

    def test_exception_attributes(self):
        from file_processing import FileProcessingException

        exc = FileProcessingException(
            "File not found", "file_not_found", "Check the path"
        )

        assert exc.message == "File not found"
        assert exc.error_type == "file_not_found"
        assert exc.details == "Check the path"
        assert str(exc) == "File not found"

    def test_exception_to_dict(self):
        from file_processing import FileProcessingException

        exc = FileProcessingException(
            "File not found", "file_not_found", "Check the path"
        )

        error_dict = exc.to_dict()

        assert error_dict["message"] == "File not found"
        assert error_dict["error_type"] == "file_not_found"
        assert error_dict["details"] == "Check the path"

    def test_exception_with_default_details(self):
        from file_processing import FileProcessingException

        exc = FileProcessingException("Error occurred", "processing_error")

        assert exc.message == "Error occurred"
        assert exc.error_type == "processing_error"
        assert exc.details == ""

    def test_exception_is_exception(self):
        from file_processing import FileProcessingException

        exc = FileProcessingException("Test", "test_error")

        assert isinstance(exc, Exception)


class TestTableWritingException:
    """Tests for TableWritingException."""

    def test_exception_attributes(self):
        from table_writing import TableWritingException

        exc = TableWritingException("Write failed", "write_error", "Check permissions")

        assert exc.message == "Write failed"
        assert exc.error_type == "write_error"
        assert exc.details == "Check permissions"

    def test_exception_to_dict(self):
        from table_writing import TableWritingException

        exc = TableWritingException("Write failed", "write_error", "Check permissions")

        error_dict = exc.to_dict()

        assert error_dict["message"] == "Write failed"
        assert error_dict["error_type"] == "write_error"
        assert error_dict["details"] == "Check permissions"


class TestFileManagementException:
    """Tests for FileManagementException."""

    def test_exception_attributes(self):
        from file_management import FileManagementException

        exc = FileManagementException("Move failed", "move_error", "Source not found")

        assert exc.message == "Move failed"
        assert exc.error_type == "move_error"
        assert exc.details == "Source not found"

    def test_exception_to_dict(self):
        from file_management import FileManagementException

        exc = FileManagementException("Move failed", "move_error", "Source not found")

        error_dict = exc.to_dict()

        assert error_dict["message"] == "Move failed"
        assert error_dict["error_type"] == "move_error"
        assert error_dict["details"] == "Source not found"
