"""Integration test for complete CSVW workflow."""

import tempfile
from pathlib import Path

import pytest
from file_processing import process_file
from pyspark.sql import SparkSession

from dataframe_validation import validate_csvw_constraints


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.appName("CSVWIntegrationTests")
        .master("local[2]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


def test_full_csvw_workflow(spark, temp_dir):
    """Test complete workflow: process file with CSVW, then validate constraints."""

    # Create CSV file
    csv_file = temp_dir / "data.csv"
    csv_file.write_text(
        "id,name,age,email\n1,Alice,25,alice@example.com\n2,Bob,30,bob@example.com\n"
    )

    # Create CSVW metadata
    metadata_file = temp_dir / "data.csv-metadata.json"
    metadata_content = """
    {
        "@context": "http://www.w3.org/ns/csvw",
        "tableSchema": {
            "columns": [
                {"name": "id", "datatype": "integer", "required": true},
                {"name": "name", "datatype": "string", "required": true},
                {
                    "name": "age",
                    "datatype": "integer",
                    "constraints": {"minimum": 18, "maximum": 100}
                },
                {
                    "name": "email",
                    "datatype": "string",
                    "constraints": {"pattern": "^[a-z]+@[a-z]+\\\\.[a-z]+$"}
                }
            ],
            "primaryKey": "id"
        },
        "dialect": {
            "delimiter": ",",
            "header": true
        }
    }
    """
    metadata_file.write_text(metadata_content)

    # Step 1: Process file (should use CSVW metadata)
    result = process_file(str(csv_file), spark)

    assert result.success
    assert result.metadata is not None  # CSVW metadata was loaded
    assert result.dataframe is not None

    # Step 2: Validate constraints
    report = validate_csvw_constraints(result.dataframe, result.metadata)

    assert report.valid
    assert len(report.violations) == 0
