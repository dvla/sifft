"""Tests for CSVW constraint validation."""

import tempfile
from pathlib import Path

import pytest
from dataframe_validation.csvw import validate_csvw_constraints
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.appName("CSVWConstraintTests")
        .master("local[2]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_null_dataframe(self, spark):
        """Test that None DataFrame returns error."""
        metadata = {"tableSchema": {"columns": []}}

        report = validate_csvw_constraints(None, metadata)

        assert not report.valid
        assert len(report.violations) == 1
        assert report.violations[0].constraint_type == "invalid_input"

    def test_invalid_metadata_type(self, spark):
        """Test that invalid metadata type returns error."""
        df = spark.createDataFrame([(1, "Alice")], ["id", "name"])

        report = validate_csvw_constraints(df, "not a dict")

        assert not report.valid
        assert len(report.violations) == 1
        assert report.violations[0].constraint_type == "invalid_metadata"

    def test_invalid_constraint_types(self, spark):
        """Test that invalid constraint value types are handled gracefully."""
        df = spark.createDataFrame([(1, "Alice", 25)], ["id", "name", "age"])

        metadata = {
            "tableSchema": {
                "columns": [
                    {
                        "name": "age",
                        "datatype": "integer",
                        "constraints": {
                            "minimum": "not a number",  # Should be int/float
                            "maxLength": "not an int",  # Should be int
                            "pattern": 123,  # Should be string
                            "enum": "not a list",  # Should be list
                        },
                    }
                ]
            }
        }

        report = validate_csvw_constraints(df, metadata)

        # Should be valid because invalid constraints are skipped
        assert report.valid
        assert len(report.violations) == 0
        assert len(report.violations) == 0


class TestColumnConstraints:
    """Tests for column-level constraints."""

    def test_required_constraint_passes(self, spark):
        """Test that required constraint passes with no nulls."""
        df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])

        metadata = {
            "tableSchema": {
                "columns": [
                    {"name": "id", "datatype": "integer", "required": True},
                    {"name": "name", "datatype": "string", "required": True},
                ]
            }
        }

        report = validate_csvw_constraints(df, metadata)

        assert report.valid
        assert len(report.violations) == 0

    def test_required_constraint_fails(self, spark):
        """Test that required constraint fails with nulls."""
        df = spark.createDataFrame([(1, "Alice"), (None, "Bob")], ["id", "name"])

        metadata = {
            "tableSchema": {
                "columns": [{"name": "id", "datatype": "integer", "required": True}]
            }
        }

        report = validate_csvw_constraints(df, metadata)

        assert not report.valid
        assert len(report.violations) == 1
        assert report.violations[0].constraint_type == "required"
        assert report.violations[0].violating_rows == 1

    def test_minimum_constraint(self, spark):
        """Test minimum value constraint."""
        df = spark.createDataFrame([(1, 25), (2, 15), (3, 30)], ["id", "age"])

        metadata = {
            "tableSchema": {
                "columns": [
                    {
                        "name": "age",
                        "datatype": "integer",
                        "constraints": {"minimum": 18},
                    }
                ]
            }
        }

        report = validate_csvw_constraints(df, metadata)

        assert not report.valid
        assert len(report.violations) == 1
        assert report.violations[0].constraint_type == "minimum"
        assert report.violations[0].violating_rows == 1

    def test_maximum_constraint(self, spark):
        """Test maximum value constraint."""
        df = spark.createDataFrame([(1, 25), (2, 150), (3, 30)], ["id", "age"])

        metadata = {
            "tableSchema": {
                "columns": [
                    {
                        "name": "age",
                        "datatype": "integer",
                        "constraints": {"maximum": 100},
                    }
                ]
            }
        }

        report = validate_csvw_constraints(df, metadata)

        assert not report.valid
        assert report.violations[0].constraint_type == "maximum"

    def test_min_length_constraint(self, spark):
        """Test minimum string length constraint."""
        df = spark.createDataFrame(
            [(1, "Alice"), (2, "Bo"), (3, "Charlie")], ["id", "name"]
        )

        metadata = {
            "tableSchema": {
                "columns": [
                    {
                        "name": "name",
                        "datatype": "string",
                        "constraints": {"minLength": 3},
                    }
                ]
            }
        }

        report = validate_csvw_constraints(df, metadata)

        assert not report.valid
        assert report.violations[0].constraint_type == "minLength"
        assert report.violations[0].violating_rows == 1

    def test_max_length_constraint(self, spark):
        """Test maximum string length constraint."""
        df = spark.createDataFrame(
            [(1, "Alice"), (2, "Bob"), (3, "Christopher")], ["id", "name"]
        )

        metadata = {
            "tableSchema": {
                "columns": [
                    {
                        "name": "name",
                        "datatype": "string",
                        "constraints": {"maxLength": 10},
                    }
                ]
            }
        }

        report = validate_csvw_constraints(df, metadata)

        assert not report.valid
        assert report.violations[0].constraint_type == "maxLength"

    def test_pattern_constraint(self, spark):
        """Test regex pattern constraint."""
        df = spark.createDataFrame(
            [(1, "alice@example.com"), (2, "invalid-email"), (3, "bob@test.com")],
            ["id", "email"],
        )

        metadata = {
            "tableSchema": {
                "columns": [
                    {
                        "name": "email",
                        "datatype": "string",
                        "constraints": {"pattern": "^[a-z]+@[a-z]+\\.[a-z]+$"},
                    }
                ]
            }
        }

        report = validate_csvw_constraints(df, metadata)

        assert not report.valid
        assert report.violations[0].constraint_type == "pattern"

    def test_pattern_too_long(self, spark):
        """Test that overly long regex patterns are rejected."""
        df = spark.createDataFrame([(1, "test@example.com")], ["id", "email"])

        # Create a pattern longer than 500 chars
        long_pattern = "a" * 501

        metadata = {
            "tableSchema": {
                "columns": [
                    {
                        "name": "email",
                        "datatype": "string",
                        "constraints": {"pattern": long_pattern},
                    }
                ]
            }
        }

        report = validate_csvw_constraints(df, metadata)

        # Should be valid because pattern is rejected (not evaluated)
        assert report.valid
        assert len(report.violations) == 0

    def test_enum_constraint(self, spark):
        """Test enum (allowed values) constraint."""
        df = spark.createDataFrame(
            [(1, "active"), (2, "pending"), (3, "unknown")], ["id", "status"]
        )

        metadata = {
            "tableSchema": {
                "columns": [
                    {
                        "name": "status",
                        "datatype": "string",
                        "constraints": {"enum": ["active", "inactive", "pending"]},
                    }
                ]
            }
        }

        report = validate_csvw_constraints(df, metadata)

        assert not report.valid
        assert report.violations[0].constraint_type == "enum"

    def test_unique_constraint(self, spark):
        """Test unique values constraint."""
        df = spark.createDataFrame(
            [
                (1, "alice@example.com"),
                (2, "bob@example.com"),
                (3, "alice@example.com"),
            ],
            ["id", "email"],
        )

        metadata = {
            "tableSchema": {
                "columns": [
                    {
                        "name": "email",
                        "datatype": "string",
                        "constraints": {"unique": True},
                    }
                ]
            }
        }

        report = validate_csvw_constraints(df, metadata)

        assert not report.valid
        assert report.violations[0].constraint_type == "unique"


class TestPrimaryKeyConstraints:
    """Tests for primary key constraints."""

    def test_primary_key_single_column(self, spark):
        """Test single column primary key."""
        df = spark.createDataFrame(
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")], ["id", "name"]
        )

        metadata = {
            "tableSchema": {
                "primaryKey": "id",
                "columns": [
                    {"name": "id", "datatype": "integer"},
                    {"name": "name", "datatype": "string"},
                ],
            }
        }

        report = validate_csvw_constraints(df, metadata)

        assert report.valid

    def test_primary_key_with_nulls(self, spark):
        """Test primary key fails with null values."""
        df = spark.createDataFrame(
            [(1, "Alice"), (None, "Bob"), (3, "Charlie")], ["id", "name"]
        )

        metadata = {
            "tableSchema": {
                "primaryKey": "id",
                "columns": [{"name": "id", "datatype": "integer"}],
            }
        }

        report = validate_csvw_constraints(df, metadata)

        assert not report.valid
        assert any(v.constraint_type == "primaryKey_null" for v in report.violations)

    def test_primary_key_with_duplicates(self, spark):
        """Test primary key fails with duplicate values."""
        df = spark.createDataFrame(
            [(1, "Alice"), (2, "Bob"), (1, "Charlie")], ["id", "name"]
        )

        metadata = {
            "tableSchema": {
                "primaryKey": "id",
                "columns": [{"name": "id", "datatype": "integer"}],
            }
        }

        report = validate_csvw_constraints(df, metadata)

        assert not report.valid
        assert any(
            v.constraint_type == "primaryKey_duplicate" for v in report.violations
        )

    def test_composite_primary_key(self, spark):
        """Test composite (multi-column) primary key."""
        df = spark.createDataFrame(
            [(1, "A", "Data1"), (1, "B", "Data2"), (2, "A", "Data3")],
            ["id", "code", "data"],
        )

        metadata = {
            "tableSchema": {
                "primaryKey": ["id", "code"],
                "columns": [
                    {"name": "id", "datatype": "integer"},
                    {"name": "code", "datatype": "string"},
                    {"name": "data", "datatype": "string"},
                ],
            }
        }

        report = validate_csvw_constraints(df, metadata)

        assert report.valid


class TestMultipleConstraints:
    """Tests for multiple constraints on same data."""

    def test_multiple_violations(self, spark):
        """Test that multiple violations are all reported."""
        df = spark.createDataFrame(
            [(1, None, 15), (2, "Bob", 150), (3, "Alice", 25)], ["id", "name", "age"]
        )

        metadata = {
            "tableSchema": {
                "columns": [
                    {"name": "name", "datatype": "string", "required": True},
                    {
                        "name": "age",
                        "datatype": "integer",
                        "constraints": {"minimum": 18, "maximum": 100},
                    },
                ]
            }
        }

        report = validate_csvw_constraints(df, metadata)

        assert not report.valid
        assert len(report.violations) == 3  # required, minimum, maximum

    def test_all_constraints_pass(self, spark):
        """Test that report is valid when all constraints pass."""
        df = spark.createDataFrame(
            [(1, "Alice", 25), (2, "Bob", 30)], ["id", "name", "age"]
        )

        metadata = {
            "tableSchema": {
                "primaryKey": "id",
                "columns": [
                    {"name": "id", "datatype": "integer", "required": True},
                    {"name": "name", "datatype": "string", "required": True},
                    {
                        "name": "age",
                        "datatype": "integer",
                        "constraints": {"minimum": 18, "maximum": 100},
                    },
                ],
            }
        }

        report = validate_csvw_constraints(df, metadata)

        assert report.valid
        assert len(report.violations) == 0
        assert report.total_violations == 0


class TestConstraintReport:
    """Tests for CSVWConstraintReport model."""

    def test_report_repr_valid(self, spark):
        """Test report string representation when valid."""
        df = spark.createDataFrame([(1, "Alice")], ["id", "name"])

        metadata = {
            "tableSchema": {
                "columns": [
                    {"name": "id", "datatype": "integer"},
                    {"name": "name", "datatype": "string"},
                ]
            }
        }

        report = validate_csvw_constraints(df, metadata)

        assert "VALID" in str(report)
        assert "0 violations" in str(report)

    def test_report_repr_invalid(self, spark):  # ✅ CORRECT - inside class
        """Test report string representation when invalid."""

        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )

        df = spark.createDataFrame([(None, "Alice")], schema)

        metadata = {
            "tableSchema": {
                "columns": [{"name": "id", "datatype": "integer", "required": True}]
            }
        }

        report = validate_csvw_constraints(df, metadata)

        assert "INVALID" in str(report)
        assert "violation" in str(report)


class TestInvalidRegexPattern:
    """Tests for invalid regex pattern handling"""

    def test_invalid_regex_pattern_raises_exception(self, spark):
        from dataframe_validation import ValidationException

        df = spark.createDataFrame([("test@example.com",)], ["email"])

        metadata = {
            "tableSchema": {
                "columns": [
                    {
                        "name": "email",
                        "datatype": "string",
                        "constraints": {
                            "pattern": "[invalid("  # Invalid regex - unbalanced parenthesis
                        },
                    }
                ]
            }
        }

        with pytest.raises(ValidationException) as exc_info:
            validate_csvw_constraints(df, metadata)

        assert exc_info.value.error_type == "invalid_pattern"
        assert "email" in exc_info.value.message

    def test_valid_regex_pattern_works(self, spark):
        df = spark.createDataFrame([("test@example.com",)], ["email"])

        metadata = {
            "tableSchema": {
                "columns": [
                    {
                        "name": "email",
                        "datatype": "string",
                        "constraints": {
                            "pattern": "^[^@]+@[^@]+$"  # Valid regex
                        },
                    }
                ]
            }
        }

        report = validate_csvw_constraints(df, metadata)

        assert report.valid
        assert len(report.violations) == 0
