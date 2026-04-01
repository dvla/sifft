"""Constants for table writing operations."""

FORMATS = frozenset(["delta", "parquet", "orc"])
MODES = frozenset(["append", "overwrite", "merge", "errorIfExists"])
SCHEMA_MODES = frozenset(["strict", "merge", "overwrite"])

FORMAT_DELTA = "delta"
FORMAT_PARQUET = "parquet"
FORMAT_ORC = "orc"

MODE_APPEND = "append"
MODE_OVERWRITE = "overwrite"
MODE_MERGE = "merge"
MODE_ERROR_IF_EXISTS = "errorIfExists"

SCHEMA_MODE_STRICT = "strict"
SCHEMA_MODE_MERGE = "merge"
SCHEMA_MODE_OVERWRITE = "overwrite"
