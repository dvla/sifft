"""PySpark version compatibility helpers."""

import logging

from pyspark.sql import Column

logger = logging.getLogger(__name__)

_HAS_TRY_CAST = hasattr(Column, "try_cast")

if _HAS_TRY_CAST:
    logger.debug("PySpark 4.0+ detected — using try_cast()")
else:
    logger.debug("PySpark 3.x detected — falling back to cast()")


def safe_cast(column: Column, datatype) -> Column:
    """Cast a column, using try_cast() on PySpark 4.0+ and cast() on 3.x."""
    if _HAS_TRY_CAST:
        return column.try_cast(datatype)
    return column.cast(datatype)
