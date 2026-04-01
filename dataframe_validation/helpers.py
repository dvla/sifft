import logging
from typing import TYPE_CHECKING

from pyspark.sql.types import (
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    TimestampType,
)

if TYPE_CHECKING:
    from .csvw.models import CSVWConstraintReport

logger = logging.getLogger(__name__)


def is_potential_lossy_cast(actual_type: DataType, expected_type: DataType) -> bool:
    # Check DecimalType precision loss
    if isinstance(actual_type, DecimalType) and isinstance(expected_type, DecimalType):
        if (
            actual_type.precision > expected_type.precision
            or actual_type.scale > expected_type.scale
        ):
            return True

    # Decimal to integer/float loses precision
    if isinstance(actual_type, DecimalType) and isinstance(
        expected_type, (IntegerType, LongType, FloatType, DoubleType)
    ):
        return True

    # Timestamp to Date loses time component
    if isinstance(actual_type, TimestampType) and isinstance(expected_type, DateType):
        return True

    # Double/Float to String may lose precision in representation
    if isinstance(actual_type, (DoubleType, FloatType)) and isinstance(
        expected_type, StringType
    ):
        return True

    lossy_conversions = [
        (LongType(), IntegerType()),
        (DoubleType(), FloatType()),
        (DoubleType(), IntegerType()),
        (FloatType(), IntegerType()),
        (LongType(), ShortType()),
        (IntegerType(), ShortType()),
        (LongType(), ByteType()),
        (IntegerType(), ByteType()),
        (ShortType(), ByteType()),
        (DoubleType(), LongType()),
        (FloatType(), LongType()),
    ]

    for actual, expected in lossy_conversions:
        if isinstance(actual_type, type(actual)) and isinstance(
            expected_type, type(expected)
        ):
            return True

    return False


def format_violations(report: "CSVWConstraintReport") -> str:
    """Format validation violations into readable string.

    Args:
        report: CSVWConstraintReport with violations

    Returns:
        Formatted string with violation details

    Example:
        >>> report = validate_csvw_constraints(df, metadata)
        >>> if not report.valid:
        ...     print(format_violations(report))
    """
    if report.valid:
        return "All constraints passed"

    lines = [f"Found {report.total_violations} violations:\n"]

    for v in report.violations:
        lines.append(f"  Column: {v.column}")
        lines.append(f"  Constraint: {v.constraint_type}")
        lines.append(f"  Message: {v.message}")
        lines.append(f"  Affected rows: {v.violating_rows}")
        if v.total_rows > 0:
            percentage = (v.violating_rows / v.total_rows) * 100
            lines.append(f"  Percentage: {percentage:.1f}%")
        lines.append("")

    return "\n".join(lines)
