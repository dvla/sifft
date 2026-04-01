from pyspark.sql import SparkSession

"""Simple unit test to verify our implementation works"""
import os
import tempfile

from file_processing.file_processor import process_file


def test_process_single_file_unit():
    """Unit test for processing a single CSV file"""
    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("name,age\n")
        f.write("Alice,25\n")
        f.write("Bob,30\n")
        temp_file = f.name

    try:
        # Test our API
        spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        result = process_file(temp_file, spark)

        # Assertions
        assert result.success
        assert result.file == temp_file
        assert result.dataframe is not None

        print("✅ Unit test passed!")

    finally:
        # Clean up
        os.unlink(temp_file)


if __name__ == "__main__":
    test_process_single_file_unit()
