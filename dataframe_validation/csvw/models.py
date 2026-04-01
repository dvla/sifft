"""Data models for CSVW constraint validation results."""

from dataclasses import dataclass


@dataclass
class ConstraintViolation:
    """Represents a single constraint violation found in data.

    Attributes:
        column: Name of column with violation
        constraint_type: Type of constraint violated (e.g., "minimum", "pattern")
        message: Human-readable description of violation
        violating_rows: Number of rows violating this constraint
        total_rows: Total number of rows in dataset (for context)
    """

    column: str
    constraint_type: str
    message: str
    violating_rows: int
    total_rows: int = 0


@dataclass
class CSVWConstraintReport:
    """Overall constraint validation report for a dataset.

    Attributes:
        valid: Whether all constraints passed (True) or some failed (False)
        violations: List of all constraint violations found
        total_violations: Total number of violating rows across all constraints
    """

    valid: bool
    violations: list[ConstraintViolation]
    total_violations: int

    def __repr__(self) -> str:
        """Human-readable summary of validation results."""
        if self.valid:
            return "CSVWConstraintReport(VALID, 0 violations)"
        return (
            f"CSVWConstraintReport(INVALID, {self.total_violations} violations "
            f"across {len(self.violations)} constraints)"
        )
