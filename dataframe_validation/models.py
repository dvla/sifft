from dataclasses import dataclass

from pyspark.sql.types import DataType


@dataclass
class ValidationLog:
    """Result of validating a single column against expected schema.

    Attributes:
        message: Human-readable validation result
        matching: Whether column matches expected schema
        missing_column_name: Column is missing from DataFrame
        type_mismatch: Column exists but has wrong type
        expected_type: Expected Spark DataType
        actual_type: Actual Spark DataType found
    """

    message: str
    matching: bool
    missing_column_name: bool = False
    type_mismatch: bool = False
    expected_type: DataType | None = None
    actual_type: DataType | None = None
