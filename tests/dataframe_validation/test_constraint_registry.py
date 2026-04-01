"""Tests for custom constraint validator registry."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
from pyspark.sql.functions import sum as spark_sum

from dataframe_validation import (
    ConstraintViolation,
    list_registered_constraints,
    register_constraint_validator,
    unregister_constraint_validator,
    validate_csvw_constraints,
)


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder.appName("ConstraintRegistryTests")
        .master("local[1]")
        .getOrCreate()
    )


@pytest.fixture
def sample_df(spark):
    data = [
        (1, "Alice", 100, 200),  # OK: 100 < 200
        (2, "Bob", 300, 250),  # violation: 300 > 250
        (3, "Charlie", 150, 200),  # OK: 150 < 200
    ]
    return spark.createDataFrame(data, ["id", "name", "balance", "credit_limit"])


class TestConstraintRegistry:
    """Tests for constraint validator registry."""

    def test_register_and_list(self):
        """Test registering and listing custom validators."""

        def dummy_validator(df, column_name, config):
            return []

        register_constraint_validator("testConstraint", dummy_validator)

        try:
            assert "testConstraint" in list_registered_constraints()
        finally:
            unregister_constraint_validator("testConstraint")

    def test_unregister(self):
        """Test unregistering a validator."""

        def dummy_validator(df, column_name, config):
            return []

        register_constraint_validator("tempConstraint", dummy_validator)
        assert "tempConstraint" in list_registered_constraints()

        unregister_constraint_validator("tempConstraint")
        assert "tempConstraint" not in list_registered_constraints()

    def test_unregister_nonexistent(self):
        """Test unregistering non-existent validator doesn't error."""
        unregister_constraint_validator("nonexistent")

    def test_custom_validator_executed(self, spark, sample_df):
        """Test custom validator is called during CSVW validation."""
        validator_called = {"count": 0}

        def tracking_validator(df, column_name, config):
            validator_called["count"] += 1
            return []

        register_constraint_validator("trackingTest", tracking_validator)

        try:
            metadata = {
                "tableSchema": {
                    "columns": [
                        {
                            "name": "balance",
                            "constraints": {"trackingTest": True},
                        }
                    ]
                }
            }

            validate_csvw_constraints(sample_df, metadata)
            assert validator_called["count"] == 1
        finally:
            unregister_constraint_validator("trackingTest")

    def test_custom_validator_returns_violations(self, spark, sample_df):
        """Test custom validator violations are included in report."""

        def balance_limit_validator(df, column_name, config):
            """Check that balance doesn't exceed credit_limit."""
            limit_col = config.get("limitColumn", "credit_limit")

            result = df.select(
                count("*").alias("total"),
                spark_sum(
                    when(col(column_name) > col(limit_col), 1).otherwise(0)
                ).alias("violations"),
            ).collect()[0]

            if result["violations"] > 0:
                return [
                    ConstraintViolation(
                        column=column_name,
                        constraint_type="balanceLimit",
                        message=f"{column_name} exceeds {limit_col}",
                        violating_rows=result["violations"],
                        total_rows=result["total"],
                    )
                ]
            return []

        register_constraint_validator("balanceLimit", balance_limit_validator)

        try:
            metadata = {
                "tableSchema": {
                    "columns": [
                        {
                            "name": "balance",
                            "constraints": {
                                "balanceLimit": {"limitColumn": "credit_limit"}
                            },
                        }
                    ]
                }
            }

            report = validate_csvw_constraints(sample_df, metadata)

            assert not report.valid
            assert len(report.violations) == 1
            assert report.violations[0].constraint_type == "balanceLimit"
            assert report.violations[0].violating_rows == 1
        finally:
            unregister_constraint_validator("balanceLimit")
