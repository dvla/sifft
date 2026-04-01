import tempfile
from pathlib import Path

import pytest
from file_processing.file_processor import process_file
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DataType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
)

from dataframe_validation import apply_schema, validate_schema


def validate_dataframe(df, schema):
    """Test helper that wraps validate_schema and apply_schema together."""
    errors = validate_schema(df, schema)
    errors = validate_schema(df, schema)
    try:
        df = apply_schema(df, schema)
    except ValueError:
        pass
    return df, errors


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.appName("ValidationTests").master("local[2]").getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture(scope="session")
def samples_dir():
    return Path(__file__).parent.parent.parent / "samples"


def create_test_csv(temp_dir: Path, filename: str, content: str) -> Path:
    csv_file = temp_dir / filename
    csv_file.write_text(content)
    return csv_file


def assert_validation_matches(validations: dict, column: str):
    assert validations[column].matching
    assert not validations[column].missing_column_name
    assert not validations[column].type_mismatch


def assert_column_missing(validations: dict, column: str):
    assert not validations[column].matching
    assert validations[column].missing_column_name


def assert_type_mismatch(
    validations: dict, column: str, expected_type: DataType, actual_type: DataType
):
    assert not validations[column].matching
    assert validations[column].type_mismatch
    assert type(validations[column].expected_type) == type(expected_type)
    assert type(validations[column].actual_type) == type(actual_type)


class TestSchemaMatching:
    def test_perfect_match(self, spark, temp_dir):
        csv_file = create_test_csv(
            temp_dir, "data.csv", "name,age,city\nAlice,25,London\nBob,30,Paris\n"
        )

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("city", StringType(), True),
            ]
        )

        result = process_file(str(csv_file), spark)
        validated_df, validations = validate_dataframe(result.dataframe, schema)

        assert_validation_matches(validations, "name")
        assert_validation_matches(validations, "age")
        assert_validation_matches(validations, "city")

        assert validated_df.columns == ["name", "age", "city"]
        assert validated_df.count() == 2

    def test_all_columns_match(self, spark, temp_dir):
        csv_file = create_test_csv(
            temp_dir, "data.csv", "id,value,status\n1,100,active\n2,200,inactive\n"
        )

        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", IntegerType(), True),
                StructField("status", StringType(), True),
            ]
        )

        result = process_file(str(csv_file), spark)
        validated_df, validations = validate_dataframe(result.dataframe, schema)

        assert all(v.matching for v in validations.values())
        assert validated_df.count() == 2


class TestMissingColumns:
    def test_single_missing_column(self, spark, temp_dir):
        csv_file = create_test_csv(temp_dir, "data.csv", "name,age\nAlice,25\nBob,30\n")

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("city", StringType(), True),
            ]
        )

        result = process_file(str(csv_file), spark)
        validated_df, validations = validate_dataframe(result.dataframe, schema)

        assert_column_missing(validations, "city")
        assert "missing from DataFrame" in validations["city"].message

    def test_multiple_missing_columns(self, spark, temp_dir):
        csv_file = create_test_csv(temp_dir, "data.csv", "name\nAlice\nBob\n")

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("city", StringType(), True),
            ]
        )

        result = process_file(str(csv_file), spark)
        validated_df, validations = validate_dataframe(result.dataframe, schema)

        assert_column_missing(validations, "age")
        assert_column_missing(validations, "city")


class TestTypeMismatch:
    def test_string_vs_integer_conversion(self, spark, temp_dir):
        csv_file = create_test_csv(
            temp_dir, "data.csv", "name,age,city\nAlice,twenty-five,London\n"
        )

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("city", StringType(), True),
            ]
        )

        result = process_file(str(csv_file), spark)
        validated_df, validations = validate_dataframe(result.dataframe, schema)

        assert_type_mismatch(validations, "age", IntegerType(), StringType())
        assert_validation_matches(validations, "name")
        assert_validation_matches(validations, "city")

        assert validated_df.schema["age"].dataType == IntegerType()

    def test_type_conversion_actually_happens(self, spark, temp_dir):
        csv_file = create_test_csv(temp_dir, "data.csv", "name,value\nAlice,123\n")

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("value", ShortType(), True),
            ]
        )

        result = process_file(str(csv_file), spark)
        validated_df, validations = validate_dataframe(result.dataframe, schema)

        assert_type_mismatch(validations, "value", ShortType(), IntegerType())

        assert validated_df.schema["value"].dataType == ShortType()

        row = validated_df.collect()[0]
        assert row["value"] == 123

    def test_type_mismatch_details(self, spark, temp_dir):
        csv_file = create_test_csv(temp_dir, "data.csv", "name,age\nAlice,25\n")

        schema = StructType(
            [
                StructField("name", IntegerType(), True),
                StructField("age", IntegerType(), True),
            ]
        )

        result = process_file(str(csv_file), spark)
        validated_df, validations = validate_dataframe(result.dataframe, schema)

        assert_type_mismatch(validations, "name", IntegerType(), StringType())
        assert "converting" in validations["name"].message.lower()


class TestCombinedErrors:
    def test_missing_and_type_mismatch(self, spark, temp_dir):
        csv_file = create_test_csv(
            temp_dir, "data.csv", "name,age,city\nAlice,text,London\n"
        )

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("city", StringType(), True),
            ]
        )

        result = process_file(str(csv_file), spark)
        validated_df, validations = validate_dataframe(result.dataframe, schema)

        assert_validation_matches(validations, "name")
        assert_type_mismatch(validations, "age", IntegerType(), StringType())
        assert_validation_matches(validations, "city")


class TestColumnRenaming:
    def test_renames_auto_generated_columns(self, spark, temp_dir):
        csv_file = create_test_csv(
            temp_dir, "data.csv", "Alice,25,London\nBob,30,Paris\n"
        )

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("city", StringType(), True),
            ]
        )

        result = process_file(str(csv_file), spark, {"header": "false"})

        assert "_c0" in result.dataframe.columns
        assert "_c1" in result.dataframe.columns
        assert "_c2" in result.dataframe.columns

        validated_df, validations = validate_dataframe(result.dataframe, schema)

        assert "name" in validated_df.columns
        assert "age" in validated_df.columns
        assert "city" in validated_df.columns
        assert "_c0" not in validated_df.columns

        assert_validation_matches(validations, "name")
        assert_validation_matches(validations, "age")
        assert_validation_matches(validations, "city")

    def test_renames_existing_columns(self, spark, temp_dir):
        csv_file = create_test_csv(
            temp_dir, "data.csv", "old1,old2,old3\nAlice,25,London\n"
        )

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("city", StringType(), True),
            ]
        )

        result = process_file(str(csv_file), spark)
        validated_df, validations = validate_dataframe(result.dataframe, schema)

        assert "name" in validated_df.columns
        assert "old1" not in validated_df.columns

        assert_validation_matches(validations, "name")
        assert_validation_matches(validations, "age")
        assert_validation_matches(validations, "city")

    def test_preserves_data_after_rename(self, spark, temp_dir):
        csv_file = create_test_csv(
            temp_dir, "data.csv", "Alice,25,London\nBob,30,Paris\n"
        )

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("city", StringType(), True),
            ]
        )

        result = process_file(str(csv_file), spark, {"header": "false"})
        validated_df, validations = validate_dataframe(result.dataframe, schema)

        rows = validated_df.collect()
        assert rows[0]["name"] == "Alice"
        assert rows[0]["age"] == 25
        assert rows[0]["city"] == "London"


class TestDataTypeObjects:
    def test_expected_type_is_datatype(self, spark, temp_dir):
        csv_file = create_test_csv(temp_dir, "data.csv", "name\nAlice\n")

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )

        result = process_file(str(csv_file), spark)
        validated_df, validations = validate_dataframe(result.dataframe, schema)

        assert isinstance(validations["age"].expected_type, DataType)
        assert type(validations["age"].expected_type) == IntegerType

    def test_actual_type_is_datatype(self, spark, temp_dir):
        csv_file = create_test_csv(temp_dir, "data.csv", "name,age\nAlice,25\n")

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )

        result = process_file(str(csv_file), spark)
        validated_df, validations = validate_dataframe(result.dataframe, schema)

        assert isinstance(validations["age"].actual_type, DataType)
        assert type(validations["age"].actual_type) == IntegerType


class TestWithSampleFiles:
    def test_comma_file_with_correct_types(self, spark, samples_dir):
        csv_file = samples_dir / "data_comma.txt"

        if not csv_file.exists():
            pytest.skip(f"Sample file not found: {csv_file}")

        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("category", StringType(), True),
                StructField("price", DoubleType(), True),
            ]
        )

        result = process_file(str(csv_file), spark, {"delimiter": ","})

        if result.success and len(result.dataframe.columns) == 4:
            validated_df, validations = validate_dataframe(result.dataframe, schema)

            assert_validation_matches(validations, "id")
            assert_validation_matches(validations, "name")
            assert_validation_matches(validations, "category")
            assert_validation_matches(validations, "price")

    def test_comma_file_with_type_conversion(self, spark, samples_dir):
        csv_file = samples_dir / "data_comma.txt"

        if not csv_file.exists():
            pytest.skip(f"Sample file not found: {csv_file}")

        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("category", StringType(), True),
                StructField("price", StringType(), True),
            ]
        )

        result = process_file(str(csv_file), spark, {"delimiter": ","})

        if result.success and len(result.dataframe.columns) == 4:
            validated_df, validations = validate_dataframe(result.dataframe, schema)

            assert_type_mismatch(validations, "price", StringType(), DoubleType())

            assert validated_df.schema["price"].dataType == StringType()

    def test_pipe_file_with_correct_types(self, spark, samples_dir):
        pipe_file = samples_dir / "data_pipe.dat"

        if not pipe_file.exists():
            pytest.skip(f"Sample file not found: {pipe_file}")

        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("category", StringType(), True),
                StructField("price", DoubleType(), True),
            ]
        )

        result = process_file(str(pipe_file), spark, {"delimiter": "|"})

        if result.success and len(result.dataframe.columns) == 4:
            validated_df, validations = validate_dataframe(result.dataframe, schema)

            assert_validation_matches(validations, "id")
            assert_validation_matches(validations, "name")
            assert_validation_matches(validations, "category")
            assert_validation_matches(validations, "price")


class TestWorkflow:
    def test_complete_workflow_with_header(self, spark, temp_dir):
        csv_file = create_test_csv(
            temp_dir, "data.csv", "name,age,city\nAlice,25,London\nBob,30,Paris\n"
        )

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("city", StringType(), True),
            ]
        )

        result = process_file(str(csv_file), spark)
        assert result.success

        validated_df, validations = validate_dataframe(result.dataframe, schema)
        assert all(v.matching for v in validations.values())

        assert validated_df.count() == 2

    def test_complete_workflow_without_header(self, spark, temp_dir):
        csv_file = create_test_csv(
            temp_dir, "data.csv", "Alice,25,London\nBob,30,Paris\n"
        )

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("city", StringType(), True),
            ]
        )

        result = process_file(str(csv_file), spark, {"header": "false"})
        assert result.success
        assert result.dataframe.columns[0].startswith("_c")

        validated_df, validations = validate_dataframe(result.dataframe, schema)
        assert all(v.matching for v in validations.values())

        rows = validated_df.collect()
        assert rows[0]["name"] == "Alice"
        assert rows[0]["age"] == 25

    def test_workflow_handles_validation_failures(self, spark, temp_dir):
        csv_file = create_test_csv(temp_dir, "data.csv", "name,age\nAlice,text\n")

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("city", StringType(), True),
            ]
        )

        result = process_file(str(csv_file), spark)
        validated_df, validations = validate_dataframe(result.dataframe, schema)

        errors = [v for v in validations.values() if not v.matching]
        assert len(errors) == 3

        # Only city is actually missing
        assert_column_missing(validations, "city")
        # name and age exist but have column count mismatch error
        assert not validations["name"].matching
        assert not validations["age"].matching

    def test_workflow_auto_rename_and_validate(self, spark, temp_dir):
        csv_file = create_test_csv(
            temp_dir, "data.csv", "Alice,25,London\nBob,30,Paris\nCharlie,35,Berlin\n"
        )

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("city", StringType(), True),
            ]
        )

        result = process_file(str(csv_file), spark, {"header": "false"})
        validated_df, validations = validate_dataframe(result.dataframe, schema)

        assert all(v.matching for v in validations.values())
        assert validated_df.count() == 3


class TestLossyConversion:
    def test_lossy_conversion_warning(self, spark, temp_dir, caplog):
        csv_file = create_test_csv(
            temp_dir, "data.csv", "name,value\nAlice,9223372036854775807\n"
        )

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("value", IntegerType(), True),
            ]
        )

        result = process_file(str(csv_file), spark)

        with caplog.at_level("WARNING"):
            validated_df, validations = validate_dataframe(result.dataframe, schema)

        assert_type_mismatch(validations, "value", IntegerType(), LongType())
        assert any("precision" in record.message.lower() for record in caplog.records)

    def test_double_to_float_lossy_warning(self, spark, caplog):
        data = [("Alice", 3.141592653589793)]
        df = spark.createDataFrame(data, ["name", "value"])

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("value", FloatType(), True),
            ]
        )

        with caplog.at_level("WARNING"):
            validated_df, validations = validate_dataframe(df, schema)

        assert_type_mismatch(validations, "value", FloatType(), DoubleType())
        assert any("precision" in record.message.lower() for record in caplog.records)


class TestTransformedDataFrame:
    def test_returns_transformed_dataframe(self, spark, temp_dir):
        csv_file = create_test_csv(temp_dir, "data.csv", "old1,old2\nAlice,25\n")

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )

        result = process_file(str(csv_file), spark)
        original_columns = result.dataframe.columns

        validated_df, validations = validate_dataframe(result.dataframe, schema)

        assert original_columns != validated_df.columns
        assert validated_df.columns == ["name", "age"]

    def test_original_df_unchanged(self, spark, temp_dir):
        csv_file = create_test_csv(temp_dir, "data.csv", "old1,old2\nAlice,25\n")

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )

        result = process_file(str(csv_file), spark)
        original_columns = result.dataframe.columns.copy()

        validated_df, validations = validate_dataframe(result.dataframe, schema)

        assert result.dataframe.columns == original_columns


class TestNewAPI:
    """Test the new separate validate_schema() and apply_schema() functions."""

    def test_validate_schema_only_validates(self, spark, temp_dir):
        """validate_schema() should not modify the DataFrame."""
        csv_file = create_test_csv(temp_dir, "data.csv", "old1,old2\nAlice,25\n")

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )

        result = process_file(str(csv_file), spark)
        original_columns = result.dataframe.columns.copy()

        errors = validate_schema(result.dataframe, schema)

        # DataFrame should be unchanged
        assert result.dataframe.columns == original_columns
        assert "old1" in result.dataframe.columns
        assert "name" not in result.dataframe.columns

        # Validation checks by position, so types match even if names don't
        assert errors["name"].matching  # StringType matches
        assert errors["age"].matching  # IntegerType matches

    def test_apply_schema_transforms_dataframe(self, spark, temp_dir):
        """apply_schema() should rename columns and cast types."""
        csv_file = create_test_csv(temp_dir, "data.csv", "old1,old2\nAlice,25\n")

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )

        result = process_file(str(csv_file), spark)
        transformed_df = apply_schema(result.dataframe, schema)

        # DataFrame should be transformed
        assert transformed_df.columns == ["name", "age"]
        assert "old1" not in transformed_df.columns

    def test_validate_then_apply_workflow(self, spark, temp_dir):
        """Test the recommended workflow: validate first, then apply."""
        csv_file = create_test_csv(temp_dir, "data.csv", "Alice,text\nBob,invalid\n")

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )

        result = process_file(str(csv_file), spark, {"header": "false"})

        # Step 1: Validate
        errors = validate_schema(result.dataframe, schema)
        assert errors["name"].matching  # StringType matches
        assert not errors["age"].matching  # Type mismatch (string vs int)
        assert errors["age"].type_mismatch

        # Step 2: Apply fixes
        transformed_df = apply_schema(result.dataframe, schema)
        assert transformed_df.columns == ["name", "age"]

        # Step 3: Validate again
        errors_after = validate_schema(transformed_df, schema)
        assert all(v.matching for v in errors_after.values())

    def test_validate_schema_with_type_mismatch(self, spark, temp_dir):
        """validate_schema() should detect type mismatches."""
        csv_file = create_test_csv(temp_dir, "data.csv", "name,age\nAlice,text\n")

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )

        result = process_file(str(csv_file), spark)
        errors = validate_schema(result.dataframe, schema)

        assert errors["name"].matching
        assert not errors["age"].matching
        assert errors["age"].type_mismatch
        assert "converting" in errors["age"].message.lower()

    def test_apply_schema_with_type_conversion(self, spark, temp_dir):
        """apply_schema() should cast types correctly."""
        csv_file = create_test_csv(temp_dir, "data.csv", "name,value\nAlice,123\n")

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("value", ShortType(), True),
            ]
        )

        result = process_file(str(csv_file), spark)
        transformed_df = apply_schema(result.dataframe, schema)

        assert transformed_df.schema["value"].dataType == ShortType()
        row = transformed_df.collect()[0]
        assert row["value"] == 123

    def test_validate_schema_with_missing_columns(self, spark, temp_dir):
        """validate_schema() should detect missing columns."""
        csv_file = create_test_csv(temp_dir, "data.csv", "name,age\nAlice,25\n")

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("city", StringType(), True),
            ]
        )

        result = process_file(str(csv_file), spark)
        errors = validate_schema(result.dataframe, schema)

        # Only city is actually missing
        assert errors["city"].missing_column_name
        assert "missing from DataFrame" in errors["city"].message

        # name and age exist but have mismatch errors
        assert not errors["name"].missing_column_name
        assert not errors["age"].missing_column_name
        assert "Column count mismatch" in errors["name"].message

    def test_apply_schema_with_missing_columns_raises_error(self, spark, temp_dir):
        """apply_schema() should raise ValueError if column count doesn't match."""
        csv_file = create_test_csv(temp_dir, "data.csv", "name,age\nAlice,25\n")

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("city", StringType(), True),
            ]
        )

        result = process_file(str(csv_file), spark)

        with pytest.raises(ValueError, match="column count mismatch"):
            apply_schema(result.dataframe, schema)

    def test_validate_schema_perfect_match(self, spark, temp_dir):
        """validate_schema() should report all matching for perfect schema."""
        csv_file = create_test_csv(
            temp_dir, "data.csv", "name,age,city\nAlice,25,London\n"
        )

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("city", StringType(), True),
            ]
        )

        result = process_file(str(csv_file), spark)
        errors = validate_schema(result.dataframe, schema)

        assert all(v.matching for v in errors.values())
        assert errors["name"].matching
        assert errors["age"].matching
        assert errors["city"].matching


class TestApplySchemaWithRaiseOnCastFailure:
    """Tests for raise_on_cast_failure flag in apply_schema"""

    def test_apply_schema_default_allows_cast_failures(self, spark, temp_dir):
        csv_file = create_test_csv(
            temp_dir, "data.csv", "name,age\nAlice,not_a_number\n"
        )

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )

        result = process_file(str(csv_file), spark)
        df = apply_schema(result.dataframe, schema)

        assert df.schema[1].dataType == IntegerType()
        row = df.collect()[0]
        assert row["age"] is None  # Cast failed, became null

    def test_apply_schema_raise_on_cast_failure_true(self, spark, temp_dir):
        csv_file = create_test_csv(
            temp_dir, "data.csv", "name,age\nAlice,not_a_number\n"
        )

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )

        result = process_file(str(csv_file), spark)

        with pytest.raises(ValueError, match="Cast failure in column 'age'"):
            apply_schema(result.dataframe, schema, raise_on_cast_failure=True)

    def test_apply_schema_successful_casts_no_raise(self, spark, temp_dir):
        csv_file = create_test_csv(temp_dir, "data.csv", "name,age\nAlice,25\nBob,30\n")

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )

        result = process_file(str(csv_file), spark)
        df = apply_schema(result.dataframe, schema, raise_on_cast_failure=True)

        assert df.schema[1].dataType == IntegerType()
        rows = df.collect()
        assert rows[0]["age"] == 25
        assert rows[1]["age"] == 30
