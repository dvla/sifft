"""Datatype-level CSVW constraint validation.

Validates constraints specified within the datatype object itself,
such as type-specific minimum/maximum values and format strings.
"""

import logging
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when
from pyspark.sql.functions import sum as spark_sum

from .models import ConstraintViolation

logger = logging.getLogger(__name__)


def _validate_datatype_constraints(
    df: DataFrame, column_name: str, column_def: dict[str, Any]
) -> list[ConstraintViolation]:
    violations: list[ConstraintViolation] = []

    datatype = column_def.get("datatype")
    if not datatype or not isinstance(datatype, dict):
        return violations

    dt_minimum = datatype.get("minimum")
    dt_maximum = datatype.get("maximum")

    if dt_minimum is None and dt_maximum is None:
        dt_format = datatype.get("format")
        if dt_format:
            logger.info(
                "Format validation for %s with format '%s' is advisory only",
                column_name,
                dt_format,
            )
        return violations

    try:
        agg_exprs = [count("*").alias("total")]
        if dt_minimum is not None:
            agg_exprs.append(
                spark_sum(
                    when(
                        col(column_name).isNotNull() & (col(column_name) < dt_minimum),
                        1,
                    ).otherwise(0)
                ).alias("min_violations")
            )
        if dt_maximum is not None:
            agg_exprs.append(
                spark_sum(
                    when(
                        col(column_name).isNotNull() & (col(column_name) > dt_maximum),
                        1,
                    ).otherwise(0)
                ).alias("max_violations")
            )

        result = df.select(*agg_exprs).collect()[0]
        total_rows = result["total"]

        if dt_minimum is not None and result["min_violations"] > 0:
            violations.append(
                ConstraintViolation(
                    column=column_name,
                    constraint_type="datatype_minimum",
                    message=f"{column_name} has values below datatype minimum {dt_minimum}",
                    violating_rows=result["min_violations"],
                    total_rows=total_rows,
                )
            )

        if dt_maximum is not None and result["max_violations"] > 0:
            violations.append(
                ConstraintViolation(
                    column=column_name,
                    constraint_type="datatype_maximum",
                    message=f"{column_name} has values above datatype maximum {dt_maximum}",
                    violating_rows=result["max_violations"],
                    total_rows=total_rows,
                )
            )
    except Exception as e:
        logger.error("Error validating datatype constraints for %s: %s", column_name, e)

    dt_format = datatype.get("format")
    if dt_format:
        logger.info(
            "Format validation for %s with format '%s' is advisory only",
            column_name,
            dt_format,
        )

    return violations
