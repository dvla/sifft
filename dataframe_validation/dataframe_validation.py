import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType

from .helpers import is_potential_lossy_cast
from .models import ValidationLog

logger = logging.getLogger(__name__)


def validate_schema(
    df: DataFrame, expected_schema: StructType
) -> dict[str, ValidationLog]:
    """Validate DataFrame schema against expected schema.

    Validates by position - checks if columns can be renamed and cast to match schema.
    Use apply_schema() to actually apply the fixes after validation.

    Args:
        df: Spark DataFrame to validate
        expected_schema: Expected StructType schema

    Returns:
        Dictionary mapping column names to ValidationLog objects

    Example:
        >>> errors = validate_schema(df, expected_schema)
        >>> if any(not v.matching for v in errors.values()):
        ...     print("Schema validation failed")
        ...     df = apply_schema(df, expected_schema)
    """
    errors = {}
    target_names = [field.name for field in expected_schema.fields]

    if len(target_names) != len(df.columns):
        missing = set(target_names) - set(df.columns)
        extra = set(df.columns) - set(target_names)

        for field in expected_schema.fields:
            if field.name in missing:
                errors[field.name] = ValidationLog(
                    message=f"{field.name} missing from DataFrame",
                    matching=False,
                    missing_column_name=True,
                    expected_type=field.dataType,
                )
            else:
                errors[field.name] = ValidationLog(
                    message=f"Column count mismatch. Missing: {missing}, Extra: {extra}",
                    matching=False,
                    expected_type=field.dataType,
                )
        return errors

    for i, field in enumerate(expected_schema.fields):
        expected_name = field.name
        actual_name = df.columns[i]
        expected_type = field.dataType
        actual_type = df.schema[actual_name].dataType

        if type(actual_type) != type(expected_type):
            errors[expected_name] = ValidationLog(
                message=f"{expected_name} has wrong type. expected {expected_type} but got {actual_type}. Converting to schema type.",
                matching=False,
                type_mismatch=True,
                expected_type=expected_type,
                actual_type=actual_type,
            )

            if is_potential_lossy_cast(actual_type, expected_type):
                logger.warning(
                    "May lose precision on %s: %s -> %s",
                    expected_name,
                    actual_type.simpleString(),
                    expected_type.simpleString(),
                )
        else:
            errors[expected_name] = ValidationLog(
                message=f"{expected_name} matches schema: {actual_type}",
                matching=True,
                expected_type=expected_type,
                actual_type=actual_type,
            )

    return errors


def apply_schema(
    df: DataFrame, expected_schema: StructType, raise_on_cast_failure: bool = False
) -> DataFrame:
    """Apply expected schema to DataFrame.

    Renames columns and casts types to match expected schema.
    Use after validate_schema() to fix schema mismatches.

    Note: Cannot add missing columns - only renames and casts existing ones.

    Args:
        df: Spark DataFrame to transform
        expected_schema: Expected StructType schema
        raise_on_cast_failure: If True, raise ValueError if any casts would produce nulls

    Returns:
        Transformed DataFrame with expected schema applied

    Raises:
        ValueError: If column counts don't match or if raise_on_cast_failure=True
            and any casts would fail

    Example:
        >>> errors = validate_schema(df, expected_schema)
        >>> if not all(v.matching for v in errors.values()):
        ...     df = apply_schema(df, expected_schema)
        >>>
        >>> # Strict mode - fail if any casts would produce nulls
        >>> df = apply_schema(df, expected_schema, raise_on_cast_failure=True)
    """
    target_names = [field.name for field in expected_schema.fields]

    if len(target_names) != len(df.columns):
        raise ValueError(
            f"Cannot apply schema: column count mismatch. "
            f"Expected {len(target_names)} columns, got {len(df.columns)}. "
            f"Use validate_schema() first to check compatibility."
        )

    df = df.toDF(*target_names)

    columns_to_cast = []
    for field in expected_schema.fields:
        actual_type = df.schema[field.name].dataType
        if type(actual_type) != type(field.dataType):
            columns_to_cast.append((field.name, field.dataType, actual_type))

    if raise_on_cast_failure and columns_to_cast:
        from pyspark.sql.functions import sum as spark_sum
        from pyspark.sql.functions import when

        agg_exprs = []
        for col_name, expected_type, _ in columns_to_cast:
            agg_exprs.append(
                spark_sum(
                    when(
                        col(col_name).isNotNull()
                        & col(col_name).cast(expected_type).isNull(),
                        1,
                    ).otherwise(0)
                ).alias(col_name)
            )

        result = df.select(*agg_exprs).collect()[0]

        for col_name, expected_type, actual_type in columns_to_cast:
            failures = result[col_name]
            if failures > 0:
                raise ValueError(
                    f"Cast failure in column '{col_name}': {failures} value(s) "
                    f"would become null when casting from {actual_type.simpleString()} "
                    f"to {expected_type.simpleString()}"
                )

    for col_name, expected_type, _ in columns_to_cast:
        df = df.withColumn(col_name, col(col_name).cast(expected_type))

    return df
