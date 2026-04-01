"""Main CSVW constraint validation orchestrator.

Coordinates validation of all constraint types and assembles
the final validation report.
"""

import logging
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct, when
from pyspark.sql.functions import sum as spark_sum

from .column import _validate_column_constraints
from .datatype import _validate_datatype_constraints
from .models import ConstraintViolation, CSVWConstraintReport

logger = logging.getLogger(__name__)


def _get_table_schema(metadata: dict[str, Any]) -> dict[str, Any] | None:
    """Extract tableSchema from CSVW metadata.

    Note: Duplicated from file_processing.csvw_metadata.get_table_schema
    to avoid cross-module dependency. Both modules are independent packages.
    """
    if "tableSchema" in metadata:
        return metadata["tableSchema"]
    if "tables" in metadata and len(metadata["tables"]) > 0:
        return metadata["tables"][0].get("tableSchema")
    return None


def validate_csvw_constraints(
    df: DataFrame, metadata: dict[str, Any]
) -> CSVWConstraintReport:
    """Validate DataFrame against CSVW metadata constraints.

    Checks all constraint types defined in CSVW metadata including:
    - Column constraints: required, unique, min/max, length, pattern, enum
    - Datatype constraints: type-specific validations
    - Table constraints: primary key uniqueness and nullability

    Args:
        df: Spark DataFrame to validate
        metadata: Parsed CSVW metadata dictionary containing tableSchema
            with column definitions and constraints

    Returns:
        CSVWConstraintReport containing:
            - valid (bool): True if all constraints pass
            - violations (List[ConstraintViolation]): List of violations found
            - total_violations (int): Total count of violating rows

    Example:
        >>> from file_processing import process_file
        >>> from dataframe_validation import validate_csvw_constraints
        >>>
        >>> result = process_file("data.csv", spark)
        >>> if result.success and result.metadata:
        ...     report = validate_csvw_constraints(result.dataframe, result.metadata)
        ...     if report.valid:
        ...         print("All constraints passed!")
        ...     else:
        ...         for v in report.violations:
        ...             print(f"{v.column}: {v.constraint_type} - {v.message}")
    """
    if df is None:
        logger.error("DataFrame is None, cannot validate")
        return CSVWConstraintReport(
            valid=False,
            violations=[
                ConstraintViolation(
                    column="N/A",
                    constraint_type="invalid_input",
                    message="DataFrame is None",
                    violating_rows=0,
                )
            ],
            total_violations=0,
        )

    if not isinstance(metadata, dict):
        logger.error("Invalid metadata type: %s, expected dict", type(metadata))
        return CSVWConstraintReport(
            valid=False,
            violations=[
                ConstraintViolation(
                    column="N/A",
                    constraint_type="invalid_metadata",
                    message=f"Metadata must be dict, got {type(metadata).__name__}",
                    violating_rows=0,
                )
            ],
            total_violations=0,
        )

    violations = []

    table_schema = _get_table_schema(metadata)
    if not table_schema:
        logger.warning(
            "No tableSchema found in metadata, skipping constraint validation"
        )
        return CSVWConstraintReport(valid=True, violations=[], total_violations=0)

    columns = table_schema.get("columns", [])

    for column_def in columns:
        column_name = column_def.get("name")

        if not column_name or column_name not in df.columns:
            continue

        column_violations = _validate_column_constraints(df, column_name, column_def)
        violations.extend(column_violations)

        datatype_violations = _validate_datatype_constraints(
            df, column_name, column_def
        )
        violations.extend(datatype_violations)

    primary_key = table_schema.get("primaryKey")
    if primary_key:
        pk_violations = _validate_primary_key(df, primary_key)
        violations.extend(pk_violations)

    total_violations = sum(v.violating_rows for v in violations)
    valid = len(violations) == 0

    return CSVWConstraintReport(
        valid=valid, violations=violations, total_violations=total_violations
    )


def _validate_primary_key(df: DataFrame, primary_key: Any) -> list[ConstraintViolation]:
    violations: list[ConstraintViolation] = []

    if isinstance(primary_key, str):
        pk_columns = [primary_key]
    elif isinstance(primary_key, list):
        pk_columns = primary_key
    else:
        logger.warning("Invalid primaryKey format: %s", primary_key)
        return violations

    valid_pk_columns = [
        column_name for column_name in pk_columns if column_name in df.columns
    ]
    if len(valid_pk_columns) != len(pk_columns):
        missing = set(pk_columns) - set(valid_pk_columns)
        logger.warning("Primary key columns not found in DataFrame: %s", missing)

    if not valid_pk_columns:
        return violations

    agg_exprs = [count("*").alias("total")]
    for pk_col in valid_pk_columns:
        agg_exprs.append(
            spark_sum(when(col(pk_col).isNull(), 1).otherwise(0)).alias(
                f"{pk_col}_nulls"
            )
        )
    agg_exprs.append(countDistinct(*valid_pk_columns).alias("distinct"))

    result = df.select(*agg_exprs).collect()[0]
    total_rows = result["total"]

    for pk_col in valid_pk_columns:
        null_count = result[f"{pk_col}_nulls"]
        if null_count > 0:
            violations.append(
                ConstraintViolation(
                    column=pk_col,
                    constraint_type="primaryKey_null",
                    message=f"Primary key column {pk_col} contains null values",
                    violating_rows=null_count,
                    total_rows=total_rows,
                )
            )

    distinct_count = result["distinct"]
    duplicates = total_rows - distinct_count

    if duplicates > 0:
        violations.append(
            ConstraintViolation(
                column=", ".join(valid_pk_columns),
                constraint_type="primaryKey_duplicate",
                message=f"Primary key {valid_pk_columns} has duplicate values",
                violating_rows=duplicates,
                total_rows=total_rows,
            )
        )

    return violations
