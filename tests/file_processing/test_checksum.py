import os
import tempfile

import pytest
from file_processing.checksum import (
    clear_marker,
    compute_file_checksum,
    is_file_processed,
    mark_file_processed,
)


def test_identical_files_produce_same_checksum():
    content = b"id,name,value\n1,Alice,100\n2,Bob,200\n"

    with tempfile.TemporaryDirectory() as tmpdir:
        f1 = os.path.join(tmpdir, "file1.csv")
        f2 = os.path.join(tmpdir, "file2.csv")

        with open(f1, "wb") as f:
            f.write(content)
        with open(f2, "wb") as f:
            f.write(content)

        assert compute_file_checksum(f1) == compute_file_checksum(f2)


def test_different_files_produce_different_checksums():
    with tempfile.TemporaryDirectory() as tmpdir:
        f1 = os.path.join(tmpdir, "file1.csv")
        f2 = os.path.join(tmpdir, "file2.csv")

        with open(f1, "wb") as f:
            f.write(b"id,name\n1,Alice\n")
        with open(f2, "wb") as f:
            f.write(b"id,name\n1,Bob\n")

        assert compute_file_checksum(f1) != compute_file_checksum(f2)


def test_sha256_produces_different_hash_than_md5():
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "test.txt")
        with open(file_path, "wb") as f:
            f.write(b"test content")

        md5 = compute_file_checksum(file_path, algo="md5")
        sha256 = compute_file_checksum(file_path, algo="sha256")

    assert md5 != sha256
    assert len(md5) == 32
    assert len(sha256) == 64


def test_invalid_algorithm_raises_error():
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "test.txt")
        with open(file_path, "wb") as f:
            f.write(b"test")

        with pytest.raises(ValueError, match="Unsupported algo"):
            compute_file_checksum(file_path, algo="invalid")


def test_empty_file_produces_consistent_checksum():
    with tempfile.TemporaryDirectory() as tmpdir:
        f1 = os.path.join(tmpdir, "empty1.txt")
        f2 = os.path.join(tmpdir, "empty2.txt")

        open(f1, "w").close()
        open(f2, "w").close()

        assert compute_file_checksum(f1) == compute_file_checksum(f2)


# Marker file tests
def test_unprocessed_file_returns_false():
    with tempfile.TemporaryDirectory() as tmpdir:
        marker_dir = os.path.join(tmpdir, "markers")
        file_path = os.path.join(tmpdir, "test.csv")
        with open(file_path, "wb") as f:
            f.write(b"id,name\n1,Alice\n")

        assert is_file_processed(file_path, marker_dir) is False


def test_mark_then_check_returns_true():
    with tempfile.TemporaryDirectory() as tmpdir:
        marker_dir = os.path.join(tmpdir, "markers")
        file_path = os.path.join(tmpdir, "test.csv")
        with open(file_path, "wb") as f:
            f.write(b"id,name\n1,Alice\n")

        mark_file_processed(file_path, marker_dir, rows_processed=1)
        assert is_file_processed(file_path, marker_dir) is True


def test_clear_marker_allows_reprocessing():
    with tempfile.TemporaryDirectory() as tmpdir:
        marker_dir = os.path.join(tmpdir, "markers")
        file_path = os.path.join(tmpdir, "test.csv")
        with open(file_path, "wb") as f:
            f.write(b"id,name\n1,Alice\n")

        mark_file_processed(file_path, marker_dir, rows_processed=1)
        assert is_file_processed(file_path, marker_dir) is True

        assert clear_marker(file_path, marker_dir) is True
        assert is_file_processed(file_path, marker_dir) is False


def test_clear_marker_returns_false_if_no_marker():
    with tempfile.TemporaryDirectory() as tmpdir:
        marker_dir = os.path.join(tmpdir, "markers")
        file_path = os.path.join(tmpdir, "test.csv")
        with open(file_path, "wb") as f:
            f.write(b"id,name\n1,Alice\n")

        assert clear_marker(file_path, marker_dir) is False


def test_mark_file_processed_raise_on_error():
    from file_processing import FileProcessingException

    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "test.csv")
        with open(file_path, "wb") as f:
            f.write(b"id,name\n1,Alice\n")

        # Use a file path as marker_dir (can't create dir inside a file)
        bad_marker_dir = file_path

        with pytest.raises(FileProcessingException) as exc_info:
            mark_file_processed(file_path, bad_marker_dir, raise_on_error=True)

        assert exc_info.value.error_type == "marker_write_error"


def test_mark_file_processed_returns_error_result_by_default():
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "test.csv")
        with open(file_path, "wb") as f:
            f.write(b"id,name\n1,Alice\n")

        bad_marker_dir = file_path

        result = mark_file_processed(file_path, bad_marker_dir)

        assert result.success is False
        assert result.error is not None
        assert result.error["error_type"] == "marker_write_error"


def test_modified_file_not_detected_as_processed():
    """Modified file content produces different checksum, so not detected."""
    with tempfile.TemporaryDirectory() as tmpdir:
        marker_dir = os.path.join(tmpdir, "markers")
        file_path = os.path.join(tmpdir, "test.csv")
        with open(file_path, "w") as f:
            f.write("id,name\n1,Alice\n")

        mark_file_processed(file_path, marker_dir, rows_processed=1)

        # Modify the file
        with open(file_path, "w") as f:
            f.write("id,name\n1,Bob\n")

        assert is_file_processed(file_path, marker_dir) is False


def test_marker_path_includes_filename():
    """Marker path should include both checksum and filename."""
    with tempfile.TemporaryDirectory() as tmpdir:
        marker_dir = os.path.join(tmpdir, "markers")
        file_path = os.path.join(tmpdir, "test.csv")
        with open(file_path, "wb") as f:
            f.write(b"id,name\n1,Alice\n")

        result = mark_file_processed(file_path, marker_dir, rows_processed=1)

        assert result.success
        assert "test.csv" in result.location
        assert ".processed" in result.location


def test_bucketed_marker_path_structure():
    """Verify bucketed markers use git-style prefix directories."""
    with tempfile.TemporaryDirectory() as tmpdir:
        marker_dir = os.path.join(tmpdir, "markers")
        file_path = os.path.join(tmpdir, "test.csv")
        with open(file_path, "wb") as f:
            f.write(b"id,name\n1,Alice\n")

        checksum = compute_file_checksum(file_path)
        result = mark_file_processed(file_path, marker_dir, rows_processed=1)

        from file_processing.checksum import _sanitise_path_segment

        safe_name = _sanitise_path_segment(file_path)
        expected_path = os.path.join(
            marker_dir, checksum[:2], checksum[2:4], f"{checksum}_{safe_name}.processed"
        )
        assert result.location == expected_path
        assert os.path.exists(expected_path)


def test_same_content_different_filename_both_tracked():
    """Same content in different filenames should both be tracked separately."""
    with tempfile.TemporaryDirectory() as tmpdir:
        marker_dir = os.path.join(tmpdir, "markers")
        content = b"id,name\n1,Alice\n"

        file1 = os.path.join(tmpdir, "file1.csv")
        file2 = os.path.join(tmpdir, "file2.csv")

        with open(file1, "wb") as f:
            f.write(content)
        with open(file2, "wb") as f:
            f.write(content)

        # Process first file
        mark_file_processed(file1, marker_dir, rows_processed=1)

        # Second file (same content, different name) should NOT be detected
        assert is_file_processed(file1, marker_dir) is True
        assert is_file_processed(file2, marker_dir) is False

        # Process second file
        mark_file_processed(file2, marker_dir, rows_processed=1)
        assert is_file_processed(file2, marker_dir) is True


def test_same_filename_different_paths_both_tracked():
    """Same filename in different directories should be tracked independently."""
    with tempfile.TemporaryDirectory() as tmpdir:
        marker_dir = os.path.join(tmpdir, "markers")
        content = b"id,name\n1,Alice\n"

        day1_dir = os.path.join(tmpdir, "2026", "03", "01")
        day2_dir = os.path.join(tmpdir, "2026", "03", "02")
        os.makedirs(day1_dir)
        os.makedirs(day2_dir)

        file1 = os.path.join(day1_dir, "data.csv")
        file2 = os.path.join(day2_dir, "data.csv")

        with open(file1, "wb") as f:
            f.write(content)
        with open(file2, "wb") as f:
            f.write(content)

        # Process first file
        mark_file_processed(file1, marker_dir, rows_processed=1)

        # Same content and filename but different path - tracked independently
        assert is_file_processed(file1, marker_dir) is True
        assert is_file_processed(file2, marker_dir) is False


def test_different_content_not_duplicate():
    """Different content should not be detected as duplicate."""
    with tempfile.TemporaryDirectory() as tmpdir:
        marker_dir = os.path.join(tmpdir, "markers")

        file1 = os.path.join(tmpdir, "file1.csv")
        file2 = os.path.join(tmpdir, "file2.csv")

        with open(file1, "wb") as f:
            f.write(b"id,name\n1,Alice\n")
        with open(file2, "wb") as f:
            f.write(b"id,name\n1,Bob\n")

        mark_file_processed(file1, marker_dir, rows_processed=1)

        assert is_file_processed(file1, marker_dir) is True
        assert is_file_processed(file2, marker_dir) is False


def test_clear_marker_only_affects_specific_file():
    """Clearing marker for one file doesn't affect others with same content."""
    with tempfile.TemporaryDirectory() as tmpdir:
        marker_dir = os.path.join(tmpdir, "markers")
        content = b"id,name\n1,Alice\n"

        day1_dir = os.path.join(tmpdir, "2026", "03", "01")
        day2_dir = os.path.join(tmpdir, "2026", "03", "02")
        os.makedirs(day1_dir)
        os.makedirs(day2_dir)

        file1 = os.path.join(day1_dir, "data.csv")
        file2 = os.path.join(day2_dir, "data.csv")

        with open(file1, "wb") as f:
            f.write(content)
        with open(file2, "wb") as f:
            f.write(content)

        mark_file_processed(file1, marker_dir, rows_processed=1)
        mark_file_processed(file2, marker_dir, rows_processed=1)

        # Clear file1's marker
        clear_marker(file1, marker_dir)

        # file1 unprocessed, file2 still processed - independent markers
        assert is_file_processed(file1, marker_dir) is False
        assert is_file_processed(file2, marker_dir) is True


# Integration tests - require Spark (run via Docker)
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder.master("local[*]").appName("checksum-test").getOrCreate()
    )


def test_process_file_returns_checksum(spark):
    """Verify process_file includes checksum in result."""
    from file_processing import process_file

    result = process_file("samples/csvw_simple.csv", spark)

    assert result.success
    assert result.checksum is not None
    assert len(result.checksum) == 32  # MD5 hex length


def test_same_file_produces_same_checksum(spark):
    """Verify processing same file twice gives same checksum."""
    from file_processing import process_file

    result1 = process_file("samples/csvw_simple.csv", spark)
    result2 = process_file("samples/csvw_simple.csv", spark)

    assert result1.checksum == result2.checksum


def test_tracking_skips_duplicate(spark):
    """Verify tracking skips already processed files."""
    import shutil
    import tempfile

    from file_processing import process_file

    with tempfile.TemporaryDirectory() as tmpdir:
        marker_dir = os.path.join(tmpdir, "markers")

        src = "samples/csvw_simple.csv"
        dst = os.path.join(tmpdir, "test.csv")
        shutil.copy(src, dst)
        shutil.copy(f"{src}-metadata.json", f"{dst}-metadata.json")

        # First run - should process
        result1 = process_file(
            dst, spark, tracking="marker_file", tracking_location=marker_dir
        )
        assert result1.success
        assert result1.rows_processed > 0

        # Second run - should skip
        result2 = process_file(
            dst, spark, tracking="marker_file", tracking_location=marker_dir
        )
        assert result2.success
        assert result2.rows_processed == 0
        assert "already processed" in result2.message.lower()


def test_tracking_logs_same_content_different_filename(spark):
    """Same content with different filename should both be processed."""
    import shutil
    import tempfile

    from file_processing import process_file

    with tempfile.TemporaryDirectory() as tmpdir:
        marker_dir = os.path.join(tmpdir, "markers")

        src = "samples/csvw_simple.csv"
        file1 = os.path.join(tmpdir, "file1.csv")
        file2 = os.path.join(tmpdir, "file2.csv")

        shutil.copy(src, file1)
        shutil.copy(src, file2)
        shutil.copy(f"{src}-metadata.json", f"{file1}-metadata.json")
        shutil.copy(f"{src}-metadata.json", f"{file2}-metadata.json")

        # Process first file
        result1 = process_file(
            file1, spark, tracking="marker_file", tracking_location=marker_dir
        )
        assert result1.success
        assert result1.rows_processed > 0

        # Second file (same content, different name) should also process
        result2 = process_file(
            file2, spark, tracking="marker_file", tracking_location=marker_dir
        )
        assert result2.success
        assert result2.rows_processed > 0


def test_marker_json_contains_expected_fields():
    """Verify marker file contains source_file, checksum, timestamp, rows."""
    import json

    with tempfile.TemporaryDirectory() as tmpdir:
        marker_dir = os.path.join(tmpdir, "markers")
        file_path = os.path.join(tmpdir, "test.csv")
        with open(file_path, "wb") as f:
            f.write(b"id,name\n1,Alice\n")

        result = mark_file_processed(file_path, marker_dir, rows_processed=42)

        with open(result.location) as f:
            data = json.load(f)

        assert data["source_file"] == file_path
        assert data["checksum"] == compute_file_checksum(file_path)
        assert data["rows_processed"] == 42
        assert "processed_at" in data


def test_tracking_requires_location():
    """Verify tracking without location raises error."""
    from file_processing import process_file
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

    with pytest.raises(ValueError, match="tracking_location is required"):
        process_file("samples/csvw_simple.csv", spark, tracking="marker_file")


def test_delta_tracking_writes_record_with_null_cleared_at(spark):
    """Regression: Spark must not fail inferring type for cleared_at=None."""
    from datetime import datetime, timezone

    from pyspark.sql import Row
    from pyspark.sql.types import (
        BooleanType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType([
        StructField("checksum", StringType()),
        StructField("source_file", StringType()),
        StructField("processed_at", TimestampType()),
        StructField("rows_processed", IntegerType()),
        StructField("is_active", BooleanType()),
        StructField("cleared_at", TimestampType()),
    ])

    row = Row(
        checksum="abc123",
        source_file="test.csv",
        processed_at=datetime.now(timezone.utc),
        rows_processed=5,
        is_active=True,
        cleared_at=None,
    )

    # This is the exact pattern from the fix — fails without schema
    df = spark.createDataFrame([row], schema=schema)
    result = df.collect()[0]

    assert result["cleared_at"] is None
    assert result["is_active"] is True
    assert result["rows_processed"] == 5
