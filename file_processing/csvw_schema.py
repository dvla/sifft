"""CSVW to Spark schema conversion.

Converts CSVW tableSchema definitions into Spark StructType schemas,
mapping CSVW datatypes to appropriate Spark types with nullability.
"""

import logging
from typing import Any

from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .csvw_metadata import get_table_schema

logger = logging.getLogger(__name__)


def csvw_to_spark_schema(metadata: dict[str, Any]) -> StructType | None:
    """Convert CSVW tableSchema to Spark StructType."""
    table_schema = get_table_schema(metadata)

    if not table_schema or "columns" not in table_schema:
        logger.warning("No tableSchema.columns found in CSVW metadata")
        return None

    columns = table_schema["columns"]

    if not columns:
        logger.warning("Empty columns list in CSVW metadata")
        return None

    fields = []

    for col in columns:
        field = _convert_column_to_field(col)
        if field:
            fields.append(field)

    if not fields:
        logger.warning("No valid columns converted from CSVW metadata")
        return None

    return StructType(fields)


def _convert_column_to_field(column: dict[str, Any]) -> StructField | None:
    name = column.get("name") or column.get("titles")

    if not name:
        logger.warning("Column missing name/title: %s", column)
        return None

    if isinstance(name, list):
        name = name[0] if name else None

    if not name or not isinstance(name, str):
        logger.warning("Column name must be a string: %s", name)
        return None

    datatype = column.get("datatype", {})

    if isinstance(datatype, str):
        base_type = datatype
    elif isinstance(datatype, dict):
        base_type = datatype.get("base", "string")
    else:
        base_type = "string"

    spark_type = _map_csvw_type_to_spark(base_type, datatype)

    required = column.get("required", False)
    nullable = not required

    return StructField(name, spark_type, nullable)


def _map_csvw_type_to_spark(csvw_type: str, datatype_obj: Any) -> DataType:
    csvw_type = csvw_type.lower().strip()

    if ":" in csvw_type:
        csvw_type = csvw_type.split(":")[-1]

    type_map = {
        "string": StringType(),
        "integer": IntegerType(),
        "int": IntegerType(),
        "long": LongType(),
        "number": DoubleType(),
        "float": DoubleType(),
        "double": DoubleType(),
        "decimal": DecimalType(38, 10),
        "boolean": BooleanType(),
        "bool": BooleanType(),
        "date": DateType(),
        "datetime": TimestampType(),
        "timestamp": TimestampType(),
        "time": StringType(),
    }

    spark_type = type_map.get(csvw_type)

    if csvw_type == "decimal" and isinstance(datatype_obj, dict):
        precision = datatype_obj.get("precision", 38)
        scale = datatype_obj.get("scale", 10)

        if not isinstance(precision, int) or not isinstance(scale, int):
            logger.warning(
                "Invalid decimal precision/scale types: %s/%s, using defaults",
                type(precision),
                type(scale),
            )
            precision, scale = 38, 10
        elif precision < 1 or precision > 38 or scale < 0 or scale > precision:
            logger.warning(
                "Invalid decimal precision/scale values: %s/%s, using defaults",
                precision,
                scale,
            )
            precision, scale = 38, 10

        spark_type = DecimalType(precision, scale)

    if spark_type is None:
        logger.warning("Unknown CSVW type '%s', defaulting to StringType", csvw_type)
        spark_type = StringType()

    return spark_type
