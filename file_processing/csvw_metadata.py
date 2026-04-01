"""CSVW metadata file discovery, loading, and parsing.

Handles JSON-LD processing to normalize CSVW metadata from various formats
into a consistent structure for downstream processing.
"""

import json
import logging
from pathlib import Path
from typing import Any

from pyld import jsonld

logger = logging.getLogger(__name__)


def load_csvw_metadata(
    csv_path: str, metadata_path: str | None = None
) -> dict[str, Any] | None:
    """Load CSVW metadata with explicit path or auto-discovery."""
    if metadata_path:
        if not Path(metadata_path).exists():
            logger.warning("Specified metadata file not found: %s", metadata_path)
            return None
        return _parse_metadata_file(metadata_path)

    return _discover_metadata(csv_path)


def _discover_metadata(csv_path: str) -> dict[str, Any] | None:
    csv_file = Path(csv_path)

    search_paths = [
        csv_file.parent / f"{csv_file.stem}.csv-metadata.json",
        csv_file.parent / f"{csv_file.stem}-metadata.json",
        csv_file.parent / "metadata.json",
    ]

    for metadata_file in search_paths:
        if metadata_file.exists():
            logger.info("Found CSVW metadata: %s", metadata_file)
            return _parse_metadata_file(str(metadata_file))

    logger.debug("No CSVW metadata found for %s", csv_path)
    return None


def _parse_metadata_file(metadata_path: str) -> dict[str, Any] | None:
    try:
        with open(metadata_path, encoding="utf-8") as f:
            metadata = json.load(f)

        if not isinstance(metadata, dict):
            logger.warning("Invalid metadata format in %s", metadata_path)
            return None

        try:
            expanded = jsonld.expand(metadata)

            if expanded:
                compacted = jsonld.compact(expanded, "http://www.w3.org/ns/csvw")
                logger.debug("JSON-LD processing successful")
                return compacted  # type: ignore[no-any-return]
        except (OSError, TimeoutError) as e:
            logger.info(
                "JSON-LD processing failed (network issue), using raw metadata: %s", e
            )
        except Exception as e:
            logger.info("JSON-LD processing failed, using raw metadata: %s", e)

        return metadata

    except json.JSONDecodeError as e:
        logger.error("Failed to parse metadata file %s: %s", metadata_path, e)
        return None
    except Exception as e:
        logger.error("Error reading metadata file %s: %s", metadata_path, e)
        return None


def get_table_schema(metadata: dict[str, Any]) -> dict[str, Any] | None:
    """Extract tableSchema from CSVW metadata."""
    if "tableSchema" in metadata:
        return metadata["tableSchema"]  # type: ignore[no-any-return]

    if "tables" in metadata and len(metadata["tables"]) > 0:
        return metadata["tables"][0].get("tableSchema")  # type: ignore[no-any-return]

    return None
