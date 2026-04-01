"""Tests for CSVW metadata parsing and schema conversion."""

import tempfile
from pathlib import Path

import pytest
from file_processing.csvw_dialect import extract_csvw_dialect, extract_csvw_null_values
from file_processing.csvw_metadata import load_csvw_metadata
from file_processing.csvw_schema import csvw_to_spark_schema
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    TimestampType,
)


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def samples_dir():
    return Path(__file__).parent.parent.parent / "samples"


# Metadata Discovery Tests


def test_load_csvw_metadata_finds_standard_location(temp_dir):
    """Test auto-discovery of metadata in standard location."""
    csv_file = temp_dir / "data.csv"
    metadata_file = temp_dir / "data.csv-metadata.json"

    csv_file.write_text("id,name\n1,test")
    metadata_file.write_text(
        '{"@context": "http://www.w3.org/ns/csvw", "tableSchema": {"columns": []}}'
    )

    metadata = load_csvw_metadata(str(csv_file))

    assert metadata is not None
    assert "tableSchema" in metadata or "@context" in metadata


def test_load_csvw_metadata_finds_alternate_location(temp_dir):
    """Test auto-discovery of metadata in alternate location."""
    csv_file = temp_dir / "data.csv"
    metadata_file = temp_dir / "data-metadata.json"

    csv_file.write_text("id,name\n1,test")
    metadata_file.write_text(
        '{"@context": "http://www.w3.org/ns/csvw", "tableSchema": {"columns": []}}'
    )

    metadata = load_csvw_metadata(str(csv_file))

    assert metadata is not None


def test_load_csvw_metadata_returns_none_when_not_found(temp_dir):
    """Test that None is returned when no metadata file exists."""
    csv_file = temp_dir / "data.csv"
    csv_file.write_text("id,name\n1,test")

    metadata = load_csvw_metadata(str(csv_file))

    assert metadata is None


def test_load_csvw_metadata_with_explicit_path(temp_dir):
    """Test loading metadata from explicit path."""
    csv_file = temp_dir / "data.csv"
    metadata_file = temp_dir / "custom-metadata.json"

    csv_file.write_text("id,name\n1,test")
    metadata_file.write_text(
        '{"@context": "http://www.w3.org/ns/csvw", "tableSchema": {"columns": []}}'
    )

    metadata = load_csvw_metadata(str(csv_file), str(metadata_file))

    assert metadata is not None


# Schema Conversion Tests


def test_csvw_to_spark_schema_basic_types():
    """Test conversion of basic CSVW types to Spark types."""
    metadata = {
        "tableSchema": {
            "columns": [
                {"name": "id", "datatype": "integer"},
                {"name": "name", "datatype": "string"},
                {"name": "active", "datatype": "boolean"},
            ]
        }
    }

    schema = csvw_to_spark_schema(metadata)

    assert schema is not None
    assert len(schema.fields) == 3
    assert schema.fields[0].name == "id"
    assert isinstance(schema.fields[0].dataType, IntegerType)
    assert schema.fields[1].name == "name"
    assert isinstance(schema.fields[1].dataType, StringType)
    assert schema.fields[2].name == "active"
    assert isinstance(schema.fields[2].dataType, BooleanType)


def test_csvw_to_spark_schema_date_types():
    """Test conversion of temporal types."""
    metadata = {
        "tableSchema": {
            "columns": [
                {"name": "birth_date", "datatype": "date"},
                {"name": "created_at", "datatype": "datetime"},
            ]
        }
    }

    schema = csvw_to_spark_schema(metadata)

    assert schema is not None
    assert len(schema.fields) == 2
    assert isinstance(schema.fields[0].dataType, DateType)
    assert isinstance(schema.fields[1].dataType, TimestampType)


def test_csvw_to_spark_schema_unknown_type_defaults_to_string():
    """Test that unknown types default to StringType."""
    metadata = {
        "tableSchema": {
            "columns": [{"name": "weird_field", "datatype": "unknown_type"}]
        }
    }

    schema = csvw_to_spark_schema(metadata)

    assert schema is not None
    assert isinstance(schema.fields[0].dataType, StringType)


def test_csvw_to_spark_schema_handles_required_fields():
    """Test that required fields are mapped to non-nullable."""
    metadata = {
        "tableSchema": {
            "columns": [
                {"name": "required_field", "datatype": "string", "required": True},
                {"name": "optional_field", "datatype": "string", "required": False},
            ]
        }
    }

    schema = csvw_to_spark_schema(metadata)

    assert schema is not None
    assert not schema.fields[0].nullable
    assert schema.fields[1].nullable


def test_csvw_to_spark_schema_returns_none_for_invalid_metadata():
    """Test that invalid metadata returns None."""
    metadata = {"no_table_schema": "invalid"}

    schema = csvw_to_spark_schema(metadata)

    assert schema is None


def test_csvw_to_spark_schema_handles_titles_as_name():
    """Test that titles can be used as column names."""
    metadata = {
        "tableSchema": {"columns": [{"titles": "Column Name", "datatype": "string"}]}
    }

    schema = csvw_to_spark_schema(metadata)

    assert schema is not None
    assert schema.fields[0].name == "Column Name"


# Dialect Extraction Tests


def test_extract_csvw_dialect_delimiter():
    """Test extraction of custom delimiter."""
    metadata = {"dialect": {"delimiter": "|"}}

    dialect = extract_csvw_dialect(metadata)

    assert dialect["delimiter"] == "|"


def test_extract_csvw_dialect_encoding():
    """Test extraction of custom encoding."""
    metadata = {"dialect": {"encoding": "ISO-8859-1"}}

    dialect = extract_csvw_dialect(metadata)

    assert dialect["encoding"] == "ISO-8859-1"


def test_extract_csvw_dialect_header():
    """Test extraction of header setting."""
    metadata = {"dialect": {"header": False}}

    dialect = extract_csvw_dialect(metadata)

    assert not dialect["header"]


def test_extract_csvw_dialect_defaults():
    """Test that defaults are used when no dialect specified."""
    metadata = {}

    dialect = extract_csvw_dialect(metadata)

    assert dialect["delimiter"] == ","
    assert dialect["encoding"] == "UTF-8"
    assert dialect["header"]


# Null Value Tests


def test_extract_csvw_null_values_single():
    """Test extraction of single null value."""
    metadata = {"dialect": {"null": "NA"}}

    null_values = extract_csvw_null_values(metadata)
    assert null_values == ["NA"]


def test_extract_csvw_null_values_multiple():
    """Test extraction of multiple null values."""
    metadata = {"dialect": {"null": ["NA", "N/A", ""]}}

    null_values = extract_csvw_null_values(metadata)
    assert null_values == ["NA", "N/A", ""]


def test_extract_csvw_null_values_empty():
    """Test empty null values when none specified."""
    metadata = {}

    null_values = extract_csvw_null_values(metadata)
    assert null_values == []


# Integration with Sample Files


def test_load_csvw_simple_sample(samples_dir):
    """Test loading actual CSVW sample file."""
    csv_file = samples_dir / "csvw_simple.csv"

    if not csv_file.exists():
        pytest.skip(f"Sample file not found: {csv_file}")

    metadata = load_csvw_metadata(str(csv_file))

    assert metadata is not None
    assert "tableSchema" in metadata or "@context" in metadata


def test_csvw_simple_schema_conversion(samples_dir):
    """Test schema conversion with actual sample file."""
    csv_file = samples_dir / "csvw_simple.csv"

    if not csv_file.exists():
        pytest.skip(f"Sample file not found: {csv_file}")

    metadata = load_csvw_metadata(str(csv_file))

    if metadata is None:
        pytest.skip("No metadata found for sample file")

    schema = csvw_to_spark_schema(metadata)

    assert schema is not None
    assert len(schema.fields) == 4

    field_names = [f.name for f in schema.fields]
    assert "product_id" in field_names
    assert "name" in field_names
    assert "category" in field_names
    assert "price" in field_names

    price_field = next(f for f in schema.fields if f.name == "price")
    assert isinstance(price_field.dataType, DoubleType)
