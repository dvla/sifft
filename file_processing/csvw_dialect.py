"""CSVW dialect extraction for CSV parsing configuration."""

import logging
from typing import Any

logger = logging.getLogger(__name__)


def extract_csvw_dialect(metadata: dict[str, Any]) -> dict[str, Any]:
    """Extract CSV dialect settings from CSVW metadata."""
    defaults = {
        "delimiter": ",",
        "quote_char": '"',
        "escape_char": "\\",
        "skip_rows": 0,
        "encoding": "UTF-8",
        "header": True,
    }

    dialect = metadata.get("dialect") or (
        metadata.get("tables", [{}])[0].get("dialect") if metadata.get("tables") else {}
    )

    if not dialect:
        return defaults

    result = defaults.copy()
    result["delimiter"] = dialect.get("delimiter", result["delimiter"])
    result["quote_char"] = dialect.get("quoteChar", result["quote_char"])
    result["skip_rows"] = dialect.get("skipRows", result["skip_rows"])
    result["encoding"] = dialect.get("encoding", result["encoding"])

    if dialect.get("doubleQuote"):
        result["escape_char"] = result["quote_char"]

    if "header" in dialect:
        result["header"] = dialect["header"]
    elif "headerRowCount" in dialect:
        result["header"] = dialect["headerRowCount"] > 0

    return result


def extract_csvw_null_values(metadata: dict[str, Any]) -> list[str]:
    """Extract null value representations from CSVW metadata."""
    dialect = metadata.get("dialect", {})
    null_value = dialect.get("null")

    if not null_value:
        return []

    return null_value if isinstance(null_value, list) else [str(null_value)]
