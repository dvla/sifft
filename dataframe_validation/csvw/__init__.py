"""CSVW constraint validation.

Validates DataFrames against CSVW metadata constraints including:
- Column constraints (min/max, length, pattern, enum, unique)
- Table constraints (primary keys)
- Datatype constraints (type-specific min/max)
"""

from .models import ConstraintViolation, CSVWConstraintReport
from .validator import validate_csvw_constraints

__all__ = ["ConstraintViolation", "CSVWConstraintReport", "validate_csvw_constraints"]
