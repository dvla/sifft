"""Tests for source metadata functionality."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for tests."""
    spark = (
        SparkSession.builder.appName("MetadataTests").master("local[2]").getOrCreate()
    )
    yield spark
    spark.stop()


class TestAddSourceMetadata:
    """Tests for add_source_metadata helper function."""

    def test_add_default_metadata_columns(self, spark):
        from table_writing import add_source_metadata

        # Create test DataFrame
        df = spark.createDataFrame([("Alice", 30), ("Bob", 25)], ["name", "age"])

        # Add metadata
        df_with_metadata = add_source_metadata(
            df, source_file_path="s3://bucket/data/file.csv", file_size=1024
        )

        # Check columns were added
        assert "dp_metadata_directory_path" in df_with_metadata.columns
        assert "dp_metadata_filename" in df_with_metadata.columns
        assert "dp_metadata_timestamp" in df_with_metadata.columns
        assert "dp_metadata_size" in df_with_metadata.columns

        # Check values
        row = df_with_metadata.first()
        assert row.dp_metadata_directory_path == "s3://bucket/data"
        assert row.dp_metadata_filename == "file.csv"
        assert row.dp_metadata_size == 1024
        assert row.dp_metadata_timestamp is not None

    def test_add_metadata_without_size(self, spark):
        from table_writing import add_source_metadata

        df = spark.createDataFrame([("Alice", 30)], ["name", "age"])

        # Add metadata without size
        df_with_metadata = add_source_metadata(df, source_file_path="data.csv")

        # Size column should not be added
        assert "dp_metadata_size" not in df_with_metadata.columns

    def test_custom_column_names(self, spark):
        from table_writing import add_source_metadata

        df = spark.createDataFrame([("Alice", 30)], ["name", "age"])

        # Custom column names
        custom_names = {
            "directory_path": "my_source_dir",
            "filename": "my_source_file",
            "timestamp": "my_ingested_at",
            "size": "my_file_size",
        }

        df_with_metadata = add_source_metadata(
            df, source_file_path="data.csv", file_size=1024, column_names=custom_names
        )

        # Check custom columns
        assert "my_source_dir" in df_with_metadata.columns
        assert "my_source_file" in df_with_metadata.columns
        assert "my_ingested_at" in df_with_metadata.columns
        assert "my_file_size" in df_with_metadata.columns

        # Default columns should not exist
        assert "dp_metadata_directory_path" not in df_with_metadata.columns


class TestWriteTableWithMetadata:
    """Tests for write_table with source metadata."""

    def test_write_with_metadata_enabled(self, spark):
        from table_writing import TableWriteOptions, write_table

        df = spark.createDataFrame([("Alice", 30), ("Bob", 25)], ["name", "age"])

        options = TableWriteOptions(
            format="parquet", mode="overwrite", add_source_metadata=True
        )

        result = write_table(
            df,
            "test_metadata_table",
            spark,
            options,
            source_file_path="s3://bucket/data.csv",
            file_size=2048,
        )

        assert result.success

        # Read back and verify metadata columns
        written_df = spark.table("test_metadata_table")
        assert "dp_metadata_directory_path" in written_df.columns
        assert "dp_metadata_filename" in written_df.columns
        assert "dp_metadata_timestamp" in written_df.columns
        assert "dp_metadata_size" in written_df.columns

        # Verify values
        row = written_df.first()
        assert row.dp_metadata_directory_path == "s3://bucket"
        assert row.dp_metadata_filename == "data.csv"
        assert row.dp_metadata_size == 2048

    def test_write_without_metadata(self, spark):
        from table_writing import TableWriteOptions, write_table

        df = spark.createDataFrame([("Alice", 30)], ["name", "age"])

        options = TableWriteOptions(format="parquet", mode="overwrite")

        result = write_table(df, "test_no_metadata_table", spark, options)

        assert result.success

        # Metadata columns should not exist
        written_df = spark.table("test_no_metadata_table")
        assert "dp_metadata_directory_path" not in written_df.columns
        assert "dp_metadata_filename" not in written_df.columns

    def test_write_with_metadata_missing_source_path(self, spark):
        from table_writing import TableWriteOptions, write_table

        df = spark.createDataFrame([("Alice", 30)], ["name", "age"])

        options = TableWriteOptions(add_source_metadata=True)

        # Should fail without source_file_path
        result = write_table(df, "test_fail_table", spark, options)

        assert not result.success
        assert "source_file_path required" in result.message

    def test_write_with_custom_metadata_columns(self, spark):
        from table_writing import TableWriteOptions, write_table

        df = spark.createDataFrame([("Alice", 30)], ["name", "age"])

        options = TableWriteOptions(
            format="parquet",
            mode="overwrite",
            add_source_metadata=True,
            source_metadata_columns={
                "directory_path": "custom_dir",
                "filename": "custom_file",
            },
        )

        result = write_table(
            df,
            "test_custom_metadata_table",
            spark,
            options,
            source_file_path="data.csv",
        )

        assert result.success

        # Check custom columns
        written_df = spark.table("test_custom_metadata_table")
        assert "custom_dir" in written_df.columns
        assert "custom_file" in written_df.columns

    def test_integration_with_file_processing(self, spark, tmp_path):
        """Integration test: Process file and write with metadata."""
        from file_processing import process_file
        from table_writing import TableWriteOptions, write_table

        # Create test file
        test_file = tmp_path / "data.csv"
        test_file.write_text("name,age\nAlice,30\nBob,25\n")

        # Process file
        result = process_file(str(test_file), spark)
        assert result.success

        # Write with metadata
        options = TableWriteOptions(
            format="parquet", mode="overwrite", add_source_metadata=True
        )

        write_result = write_table(
            result.dataframe,
            "test_integration_table",
            spark,
            options,
            source_file_path=str(test_file),
            file_size=test_file.stat().st_size,
        )

        assert write_result.success

        # Verify metadata
        written_df = spark.table("test_integration_table")
        assert written_df.count() == 2

        row = written_df.first()
        assert row.dp_metadata_filename == "data.csv"
        assert row.dp_metadata_size == test_file.stat().st_size
