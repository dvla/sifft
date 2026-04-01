"""DataFrame validation module.

Provides schema validation and CSVW constraint validation.
"""

from .constraint_registry import (
    get_constraint_validator,
    list_registered_constraints,
    register_constraint_validator,
    unregister_constraint_validator,
)
from .csvw import ConstraintViolation, CSVWConstraintReport, validate_csvw_constraints
from .dataframe_validation import apply_schema, validate_schema
from .exceptions import ValidationException
from .helpers import format_violations
from .models import ValidationLog

__all__ = [
    "validate_schema",
    "apply_schema",
    "ValidationLog",
    "ValidationException",
    "validate_csvw_constraints",
    "CSVWConstraintReport",
    "ConstraintViolation",
    "format_violations",
    # Constraint registry
    "register_constraint_validator",
    "unregister_constraint_validator",
    "get_constraint_validator",
    "list_registered_constraints",
]
