"""Column-level CSVW constraint validation.

Validates constraints that apply to individual columns:
- required (not null)
- minimum/maximum (numeric bounds)
- minInclusive/maxInclusive (inclusive numeric bounds)
- minExclusive/maxExclusive (exclusive numeric bounds)
- minLength/maxLength (string length bounds)
- pattern (regex matching, max 500 chars)
- enum (allowed value set)
- unique (no duplicates)
"""

import logging
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    length,
    regexp_extract,
    when,
)
from pyspark.sql.functions import (
    sum as spark_sum,
)

from ..constraint_registry import get_constraint_validator
from .models import ConstraintViolation

MAX_PATTERN_LENGTH = 500

logger = logging.getLogger(__name__)

NUMERIC_RULES = [
    ("minimum", (int, float), lambda c, v: col(c) < v),
    ("maximum", (int, float), lambda c, v: col(c) > v),
    ("minInclusive", (int, float), lambda c, v: col(c) < v),
    ("maxInclusive", (int, float), lambda c, v: col(c) > v),
    ("minExclusive", (int, float), lambda c, v: col(c) <= v),
    ("maxExclusive", (int, float), lambda c, v: col(c) >= v),
]

LENGTH_RULES = [
    ("minLength", int, lambda c, v: length(col(c)) < v),
    ("maxLength", int, lambda c, v: length(col(c)) > v),
]


def _validate_column_constraints(
    df: DataFrame, column_name: str, column_def: dict[str, Any]
) -> list[ConstraintViolation]:
    constraints = column_def.get("constraints", {})
    violations = []

    violations.extend(_validate_batched(df, column_name, column_def, constraints))
    violations.extend(_validate_individual(df, column_name, constraints))

    return violations


def _validate_batched(
    df: DataFrame, column_name: str, column_def: dict, constraints: dict
) -> list[ConstraintViolation]:
    checks = []

    if column_def.get("required", False):
        checks.append(("required", col(column_name).isNull()))

    for name, types, condition_fn in NUMERIC_RULES + LENGTH_RULES:
        if name in constraints and isinstance(constraints[name], types):
            checks.append((name, condition_fn(column_name, constraints[name])))

    if not checks:
        return []

    agg_exprs = [count("*").alias("total")]
    for name, condition in checks:
        if name == "required":
            agg_exprs.append(
                spark_sum(when(col(column_name).isNull(), 1).otherwise(0)).alias(name)
            )
        else:
            agg_exprs.append(
                spark_sum(
                    when(col(column_name).isNotNull() & condition, 1).otherwise(0)
                ).alias(name)
            )

    result = df.select(*agg_exprs).collect()[0]
    total_rows = result["total"]

    violations = []
    for name, _ in checks:
        if result[name] > 0:
            violations.append(
                _create_violation(
                    column_name, name, result[name], total_rows, constraints
                )
            )
    return violations


def _validate_individual(
    df: DataFrame, column_name: str, constraints: dict
) -> list[ConstraintViolation]:
    if not constraints:
        return []

    violations = []

    if "pattern" in constraints and isinstance(constraints["pattern"], str):
        violations.extend(_validate_pattern(df, column_name, constraints["pattern"]))

    if "enum" in constraints and isinstance(constraints["enum"], list):
        violations.extend(_validate_enum(df, column_name, constraints["enum"]))

    if constraints.get("unique", False):
        violations.extend(_validate_unique(df, column_name))

    for name, config in constraints.items():
        validator = get_constraint_validator(name)
        if validator:
            violations.extend(validator(df, column_name, config))

    return violations


def _create_violation(
    column_name: str,
    constraint_type: str,
    violating_rows: int,
    total_rows: int,
    constraints: dict,
) -> ConstraintViolation:
    templates = {
        "required": "has null values but is marked as required",
        "minimum": "has values below minimum {}",
        "maximum": "has values above maximum {}",
        "minInclusive": "has values below minInclusive {}",
        "maxInclusive": "has values above maxInclusive {}",
        "minExclusive": "has values at or below minExclusive {}",
        "maxExclusive": "has values at or above maxExclusive {}",
        "minLength": "has values shorter than minLength {}",
        "maxLength": "has values longer than maxLength {}",
    }

    template = templates.get(constraint_type, f"violates {constraint_type}")
    value = constraints.get(constraint_type, "")
    message = (
        f"{column_name} {template.format(value) if '{}' in template else template}"
    )

    return ConstraintViolation(
        column=column_name,
        constraint_type=constraint_type,
        message=message,
        violating_rows=violating_rows,
        total_rows=total_rows,
    )


def _check_violation(
    df: DataFrame,
    column_name: str,
    constraint_type: str,
    condition,
    message: str,
) -> list[ConstraintViolation]:
    result = df.select(
        count("*").alias("total"),
        spark_sum(when(condition, 1).otherwise(0)).alias("violations"),
    ).collect()[0]

    if result["violations"] > 0:
        return [
            ConstraintViolation(
                column=column_name,
                constraint_type=constraint_type,
                message=message,
                violating_rows=result["violations"],
                total_rows=result["total"],
            )
        ]
    return []


def _validate_pattern(
    df: DataFrame, column_name: str, pattern: str
) -> list[ConstraintViolation]:
    if len(pattern) > MAX_PATTERN_LENGTH:
        logger.error(
            "Pattern too long for %s: %d chars (max %d)",
            column_name,
            len(pattern),
            MAX_PATTERN_LENGTH,
        )
        return []

    try:
        condition = col(column_name).isNotNull() & (
            regexp_extract(col(column_name), pattern, 0) == ""
        )
        return _check_violation(
            df,
            column_name,
            "pattern",
            condition,
            f"{column_name} has values not matching pattern: {pattern}",
        )
    except Exception as e:
        from ..exceptions import ValidationException

        raise ValidationException(
            f"Invalid regex pattern for column '{column_name}': {pattern}",
            "invalid_pattern",
            str(e),
        ) from e


def _validate_enum(
    df: DataFrame, column_name: str, enum_values: list
) -> list[ConstraintViolation]:
    if not enum_values:
        logger.warning("Empty enum list for %s, skipping validation", column_name)
        return []

    condition = col(column_name).isNotNull() & ~col(column_name).isin(enum_values)
    return _check_violation(
        df,
        column_name,
        "enum",
        condition,
        f"{column_name} has values not in allowed set: {enum_values}",
    )


def _validate_unique(df: DataFrame, column_name: str) -> list[ConstraintViolation]:
    result = df.select(
        count("*").alias("total"),
        count(when(col(column_name).isNotNull(), 1)).alias("non_null"),
        countDistinct(col(column_name)).alias("distinct"),
    ).collect()[0]

    total_rows = result["total"]
    non_null_count = result["non_null"]
    distinct_count = result["distinct"]
    duplicates = non_null_count - distinct_count

    if duplicates > 0:
        return [
            ConstraintViolation(
                column=column_name,
                constraint_type="unique",
                message=f"{column_name} has duplicate values but is marked as unique",
                violating_rows=duplicates,
                total_rows=total_rows,
            )
        ]
    return []
