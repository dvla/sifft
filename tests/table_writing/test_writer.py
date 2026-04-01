"""Tests for table writing functionality."""

import tempfile

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from table_writing import TableWriteOptions, TableWriteResult, write_table
from table_writing.helpers import split_write_results


@pytest.fixture(scope="session")
def spark():
    warehouse_dir = tempfile.mkdtemp()
    spark = (
        SparkSession.builder.appName("TableWritingTests")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .getOrCreate()
    )

    yield spark
    spark.stop()


@pytest.fixture(autouse=True)
def cleanup_tables(spark):
    """Clean up test tables before each test."""
    import shutil
    from pathlib import Path

    warehouse_dir = Path(spark.conf.get("spark.sql.warehouse.dir"))

    # Clean before test
    for table_dir in warehouse_dir.glob("test_*"):
        if table_dir.is_dir():
            shutil.rmtree(table_dir, ignore_errors=True)

    yield

    # Clean after test
    for table_dir in warehouse_dir.glob("test_*"):
        if table_dir.is_dir():
            shutil.rmtree(table_dir, ignore_errors=True)


@pytest.fixture
def sample_df(spark):
    """Create a sample DataFrame for testing."""
    data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)]
    return spark.createDataFrame(data, ["id", "name", "age"])


@pytest.fixture
def parquet_options():
    """Default options for testing with Parquet."""
    return TableWriteOptions(format="parquet")


class TestWriteAppend:
    """Tests for append mode."""

    def test_append_to_new_table(self, spark, sample_df, parquet_options):
        """Test appending to a new table creates it."""
        table_name = "test_append_new"

        result = write_table(sample_df, table_name, spark, parquet_options)

        assert result.success
        assert result.rows_written == 3
        assert result.table_name == table_name

        written_df = spark.table(table_name)
        assert written_df.count() == 3

    def test_append_to_existing_table(self, spark, sample_df, parquet_options):
        """Test appending to existing table adds rows."""
        table_name = "test_append_existing"

        result1 = write_table(sample_df, table_name, spark, parquet_options)
        assert result1.success

        result2 = write_table(sample_df, table_name, spark, parquet_options)
        assert result2.success

        written_df = spark.table(table_name)
        assert written_df.count() == 6


class TestWriteOverwrite:
    """Tests for overwrite mode."""

    def test_overwrite_replaces_data(self, spark, sample_df, parquet_options):
        """Test overwrite mode replaces existing data."""
        table_name = "test_overwrite"

        result1 = write_table(sample_df, table_name, spark, parquet_options)
        assert result1.success

        new_data = [(4, "David", 40)]
        new_df = spark.createDataFrame(new_data, ["id", "name", "age"])

        overwrite_options = TableWriteOptions(format="parquet", mode="overwrite")
        result2 = write_table(new_df, table_name, spark, overwrite_options)
        assert result2.success

        written_df = spark.table(table_name)
        assert written_df.count() == 1
        assert written_df.first()["name"] == "David"


class TestWriteMerge:
    """Tests for merge mode."""

    def test_merge_not_supported_for_parquet(self, spark, sample_df, parquet_options):
        """Test that merge mode is only for Delta."""
        table_name = "test_merge"

        merge_options = TableWriteOptions(
            format="parquet", mode="merge", merge_keys=["id"]
        )
        result = write_table(sample_df, table_name, spark, merge_options)

        assert not result.success


class TestWriteErrorIfExists:
    """Tests for errorIfExists mode."""

    def test_error_if_exists_creates_new(self, spark, sample_df):
        """Test errorIfExists creates new table."""
        table_name = "test_error_new"

        error_options = TableWriteOptions(format="parquet", mode="errorIfExists")
        result = write_table(sample_df, table_name, spark, error_options)

        assert result.success
        assert result.rows_written == 3

    def test_error_if_exists_fails_on_existing(self, spark, sample_df, parquet_options):
        """Test errorIfExists fails if table exists."""
        table_name = "test_error_existing"

        result1 = write_table(sample_df, table_name, spark, parquet_options)
        assert result1.success

        error_options = TableWriteOptions(format="parquet", mode="errorIfExists")
        result2 = write_table(sample_df, table_name, spark, error_options)

        assert not result2.success
        assert result2.error["error_type"] == "table_exists"


class TestValidation:
    """Tests for pre-write validation."""

    def test_none_dataframe(self, spark):
        """Test None DataFrame is rejected."""
        result = write_table(None, "test", spark, TableWriteOptions())

        assert not result.success
        assert result.error["error_type"] == "invalid_input"
        assert "DataFrame cannot be None" in result.error["message"]

    def test_invalid_dataframe_type(self, spark):
        """Test non-DataFrame type is rejected."""
        result = write_table("not a dataframe", "test", spark, TableWriteOptions())

        assert not result.success
        assert result.error["error_type"] == "invalid_input"
        assert "Expected DataFrame" in result.error["message"]

    def test_none_table_name(self, spark, sample_df):
        """Test None table_name is rejected."""
        result = write_table(sample_df, None, spark, TableWriteOptions())

        assert not result.success
        assert result.error["error_type"] == "invalid_input"
        assert "table_name must be a non-None string" in result.error["message"]

    def test_empty_table_name(self, spark, sample_df):
        """Test empty table_name is rejected."""
        result = write_table(sample_df, "  ", spark, TableWriteOptions())

        assert not result.success
        assert result.error["error_type"] == "invalid_input"
        assert "cannot be empty" in result.error["message"]

    def test_none_spark_session(self, sample_df):
        """Test None SparkSession is rejected."""
        result = write_table(sample_df, "test", None, TableWriteOptions())

        assert not result.success
        assert result.error["error_type"] == "invalid_input"
        assert "SparkSession cannot be None" in result.error["message"]

    def test_table_name_too_long(self, spark, sample_df):
        """Test table name exceeding max length is rejected."""
        long_name = "a" * 256
        result = write_table(sample_df, long_name, spark, TableWriteOptions())

        assert not result.success
        assert result.error["error_type"] == "invalid_input"
        assert "exceeds maximum length" in result.error["message"]

    def test_table_name_invalid_characters(self, spark, sample_df):
        """Test table name with SQL injection characters is rejected."""
        result = write_table(
            sample_df, "test'; DROP TABLE users--", spark, TableWriteOptions()
        )

        assert not result.success
        assert result.error["error_type"] == "invalid_input"
        assert "invalid characters" in result.error["message"]

    def test_empty_dataframe_schema(self, spark):
        """Test DataFrame with no columns is rejected."""
        empty_df = spark.createDataFrame([], StructType([]))
        result = write_table(empty_df, "test", spark, TableWriteOptions())

        assert not result.success
        assert result.error["error_type"] == "invalid_input"
        assert "no columns" in result.error["message"]

    def test_invalid_format(self, spark, sample_df):
        """Test invalid format is rejected."""
        result = write_table(
            sample_df, "test", spark, TableWriteOptions(format="invalid")
        )

        assert not result.success
        assert result.error["error_type"] == "invalid_format"

    def test_invalid_mode(self, spark, sample_df):
        """Test invalid mode is rejected."""
        result = write_table(
            sample_df, "test", spark, TableWriteOptions(mode="invalid")
        )

        assert not result.success
        assert result.error["error_type"] == "invalid_mode"

    def test_merge_without_keys(self, spark, sample_df):
        """Test merge mode requires merge_keys."""
        result = write_table(sample_df, "test", spark, TableWriteOptions(mode="merge"))

        assert not result.success
        assert result.error["error_type"] == "merge_key_missing"

    def test_merge_with_empty_keys_list(self, spark, sample_df):
        """Test merge mode rejects empty merge_keys list."""
        result = write_table(
            sample_df, "test", spark, TableWriteOptions(mode="merge", merge_keys=[])
        )

        assert not result.success
        assert result.error["error_type"] == "merge_key_missing"
        assert "cannot be empty" in result.error["message"]

    def test_merge_with_non_list_keys(self, spark, sample_df):
        """Test merge_keys must be a list."""
        result = write_table(
            sample_df, "test", spark, TableWriteOptions(mode="merge", merge_keys="id")
        )

        assert not result.success
        assert result.error["error_type"] == "invalid_input"
        assert "must be a list" in result.error["message"]

    def test_merge_with_non_string_key(self, spark, sample_df):
        """Test merge_keys must contain strings."""
        result = write_table(
            sample_df, "test", spark, TableWriteOptions(mode="merge", merge_keys=[123])
        )

        assert not result.success
        assert result.error["error_type"] == "invalid_input"
        assert "must contain strings" in result.error["message"]

    def test_merge_key_too_long(self, spark, sample_df):
        """Test merge key exceeding max length is rejected."""
        long_key = "a" * 256
        result = write_table(
            sample_df,
            "test",
            spark,
            TableWriteOptions(mode="merge", merge_keys=[long_key]),
        )

        assert not result.success
        assert result.error["error_type"] == "invalid_input"
        assert "exceeds maximum length" in result.error["message"]

    def test_merge_key_invalid_characters(self, spark, sample_df):
        """Test merge key with SQL injection characters is rejected."""
        result = write_table(
            sample_df,
            "test",
            spark,
            TableWriteOptions(mode="merge", merge_keys=["id'; DROP TABLE--"]),
        )

        assert not result.success
        assert result.error["error_type"] == "invalid_input"
        assert "invalid characters" in result.error["message"]

    def test_merge_with_missing_keys(self, spark, sample_df):
        """Test merge keys must exist in DataFrame."""
        result = write_table(
            sample_df,
            "test",
            spark,
            TableWriteOptions(mode="merge", merge_keys=["nonexistent"]),
        )

        assert not result.success
        assert result.error["error_type"] == "merge_key_missing"

    def test_partition_column_missing(self, spark, sample_df):
        """Test partition columns must exist in DataFrame."""
        result = write_table(
            sample_df, "test", spark, TableWriteOptions(partition_by=["nonexistent"])
        )

        assert not result.success
        assert result.error["error_type"] == "partition_error"

    def test_partition_non_list(self, spark, sample_df):
        """Test partition_by must be a list."""
        result = write_table(
            sample_df, "test", spark, TableWriteOptions(partition_by="age")
        )

        assert not result.success
        assert result.error["error_type"] == "invalid_input"
        assert "must be a list" in result.error["message"]

    def test_partition_non_string(self, spark, sample_df):
        """Test partition_by must contain strings."""
        result = write_table(
            sample_df, "test", spark, TableWriteOptions(partition_by=[123])
        )

        assert not result.success
        assert result.error["error_type"] == "invalid_input"
        assert "must contain strings" in result.error["message"]

    def test_partition_column_too_long(self, spark, sample_df):
        """Test partition column exceeding max length is rejected."""
        long_col = "a" * 256
        result = write_table(
            sample_df, "test", spark, TableWriteOptions(partition_by=[long_col])
        )

        assert not result.success
        assert result.error["error_type"] == "invalid_input"
        assert "exceeds maximum length" in result.error["message"]

    def test_partition_column_invalid_characters(self, spark, sample_df):
        """Test partition column with SQL injection characters is rejected."""
        result = write_table(
            sample_df, "test", spark, TableWriteOptions(partition_by=["age'; DROP--"])
        )

        assert not result.success
        assert result.error["error_type"] == "invalid_input"
        assert "invalid characters" in result.error["message"]


class TestHelpers:
    """Tests for helper functions."""

    def test_split_write_results(self, spark, sample_df):
        """Test split_write_results separates successes and failures."""
        results = [
            TableWriteResult(success=True, table_name="table1", rows_written=10),
            TableWriteResult(
                success=False, table_name="table2", rows_written=0, error={}
            ),
            TableWriteResult(success=True, table_name="table3", rows_written=20),
        ]

        successes, failures = split_write_results(results)

        assert len(successes) == 2
        assert len(failures) == 1
        assert successes[0].table_name == "table1"
        assert failures[0].table_name == "table2"
