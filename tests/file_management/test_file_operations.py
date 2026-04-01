"""Tests for file_management module."""

import pytest
from file_management import list_files_in_directory, safe_move
from file_management.models import FileOperation


class TestSafeMove:
    """Tests for safe_move function."""

    def test_move_file_success(self, tmp_path):
        """Test successful file move."""
        src = tmp_path / "source.txt"
        src.write_text("test content")
        dst = tmp_path / "dest.txt"

        result = safe_move(str(src), str(dst))

        assert result.success is True
        assert result.operation == FileOperation.MOVE
        assert result.source_path == str(src)
        assert result.destination_path == str(dst)
        assert dst.exists()
        assert not src.exists()

    def test_move_creates_destination_directory(self, tmp_path):
        """Test that destination directory is created if it doesn't exist."""
        src = tmp_path / "source.txt"
        src.write_text("test content")
        dst = tmp_path / "subdir" / "dest.txt"

        result = safe_move(str(src), str(dst))

        assert result.success is True
        assert dst.exists()
        assert dst.read_text() == "test content"

    def test_move_nonexistent_file_returns_error(self, tmp_path):
        """Test that moving non-existent file returns error result."""
        src = tmp_path / "nonexistent.txt"
        dst = tmp_path / "dest.txt"

        result = safe_move(str(src), str(dst))

        assert not result.success
        assert result.error is not None
        assert result.error["error_type"] == "file_not_found"

    def test_move_nonexistent_file_with_raise_on_error(self, tmp_path):
        """Test that moving non-existent file raises with flag."""
        from file_management import FileManagementException

        src = tmp_path / "nonexistent.txt"
        dst = tmp_path / "dest.txt"

        with pytest.raises(FileManagementException):
            safe_move(str(src), str(dst), raise_on_error=True)

    def test_move_to_existing_destination_returns_error(self, tmp_path):
        """Test that moving to existing destination returns error."""
        src = tmp_path / "source.txt"
        src.write_text("source content")
        dst = tmp_path / "dest.txt"
        dst.write_text("existing content")

        result = safe_move(str(src), str(dst))

        assert not result.success
        assert result.error is not None
        assert result.error["error_type"] == "destination_exists"
        # Source should still exist
        assert src.exists()
        # Destination should be unchanged
        assert dst.read_text() == "existing content"

    def test_move_to_existing_destination_with_raise_on_error(self, tmp_path):
        """Test that moving to existing destination raises with flag."""
        from file_management import FileManagementException

        src = tmp_path / "source.txt"
        src.write_text("source content")
        dst = tmp_path / "dest.txt"
        dst.write_text("existing content")

        with pytest.raises(FileManagementException) as exc_info:
            safe_move(str(src), str(dst), raise_on_error=True)

        assert exc_info.value.error_type == "destination_exists"


class TestListFilesInDirectory:
    """Tests for list_files_in_directory function."""

    def test_list_files_success(self, tmp_path):
        """Test listing files in directory."""
        (tmp_path / "file1.txt").write_text("content1")
        (tmp_path / "file2.txt").write_text("content2")
        (tmp_path / "subdir").mkdir()

        result = list_files_in_directory(str(tmp_path))

        assert result.success is True
        assert result.operation == FileOperation.LIST
        assert result.files is not None
        assert len(result.files) == 2

        file_names = [f["name"] for f in result.files]
        assert "file1.txt" in file_names
        assert "file2.txt" in file_names

    def test_list_files_includes_metadata(self, tmp_path):
        """Test that file metadata is included."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")

        result = list_files_in_directory(str(tmp_path))

        assert result.success is True
        assert len(result.files) == 1

        file_info = result.files[0]
        assert file_info["name"] == "test.txt"
        assert file_info["path"] == str(test_file)
        assert file_info["size"] == 12

    def test_list_empty_directory(self, tmp_path):
        """Test listing empty directory."""
        result = list_files_in_directory(str(tmp_path))

        assert result.success is True
        assert result.files == []

    def test_list_nonexistent_directory(self, tmp_path):
        """Test listing non-existent directory."""
        nonexistent = tmp_path / "nonexistent"

        result = list_files_in_directory(str(nonexistent))

        assert result.success is False
        assert result.error is not None
        assert "does not exist" in result.error["message"]

    def test_list_excludes_subdirectories(self, tmp_path):
        """Test that subdirectories are not included in file list."""
        (tmp_path / "file.txt").write_text("content")
        (tmp_path / "subdir").mkdir()
        (tmp_path / "subdir" / "nested.txt").write_text("nested")

        result = list_files_in_directory(str(tmp_path))

        assert result.success is True
        assert len(result.files) == 1
        assert result.files[0]["name"] == "file.txt"
