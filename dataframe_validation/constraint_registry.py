"""Registry for custom constraint validators."""

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from pyspark.sql import DataFrame

if TYPE_CHECKING:
    from .csvw.models import ConstraintViolation

ConstraintValidator = Callable[[DataFrame, str, Any], "list[ConstraintViolation]"]

_CONSTRAINT_VALIDATORS: dict[str, ConstraintValidator] = {}


def register_constraint_validator(name: str, validator: ConstraintValidator) -> None:
    """Register a custom constraint validator.

    Args:
        name: Constraint name (e.g., "crossField", "businessRule")
        validator: Function with signature:
            (df, column_name, constraint_config) -> list[ConstraintViolation]

    Example:
        >>> def validate_positive_balance(df, column_name, config):
        ...     # Custom validation logic
        ...     violations = []
        ...     # ... check constraint ...
        ...     return violations
        >>> register_constraint_validator("positiveBalance", validate_positive_balance)
    """
    _CONSTRAINT_VALIDATORS[name] = validator


def unregister_constraint_validator(name: str) -> None:
    """Remove a constraint validator from registry."""
    _CONSTRAINT_VALIDATORS.pop(name, None)


def get_constraint_validator(name: str) -> ConstraintValidator | None:
    """Get validator for constraint name."""
    return _CONSTRAINT_VALIDATORS.get(name)


def list_registered_constraints() -> list[str]:
    """List all registered custom constraint names."""
    return list(_CONSTRAINT_VALIDATORS.keys())
