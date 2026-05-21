"""Utility functions for file validation, header detection, and delimiter detection."""

import logging
from collections import Counter
from pathlib import Path
from typing import Any

import fsspec

logger = logging.getLogger(__name__)

CANDIDATE_DELIMITERS = [",", "\t", "|", ";"]


def check_file_exists(file_path: str) -> str | None:
    """Check if a file exists and is valid. Returns error message if invalid, None if valid."""
    path = Path(file_path)

    if not path.exists():
        return f"File not found: {file_path}"

    if not path.is_file():
        return f"Path is not a file: {file_path}"

    if path.stat().st_size == 0:
        return f"File is empty: {file_path}"

    return None


def _read_sample_lines(
    file_path: str, n: int = 20, storage_options: dict[str, Any] | None = None
) -> list[str]:
    with fsspec.open(file_path, "r", encoding="utf-8", errors="ignore", **(storage_options or {})) as f:
        lines = []
        for i, line in enumerate(f):
            if i >= n:
                break
            lines.append(line.rstrip("\n\r"))
        return lines


def detect_delimiter(
    file_path: str, storage_options: dict[str, Any] | None = None
) -> str:
    """Detect the delimiter of a delimited file by inspecting content.

    Reads 20 lines and scores each candidate delimiter by column count consistency.
    Tie-breaking priority: comma > tab > pipe > semicolon.
    Falls back to comma if no candidate produces consistent results.
    """
    lines = _read_sample_lines(file_path, n=20, storage_options=storage_options)
    lines = [line for line in lines if line.strip()]

    if len(lines) < 2:
        logger.warning("Too few lines to detect delimiter for %s, defaulting to ','", file_path)
        return ","

    best_delimiter = ","
    best_score = 0

    for delimiter in CANDIDATE_DELIMITERS:
        counts = [len(line.split(delimiter)) for line in lines]

        if counts[0] <= 1:
            continue

        most_common_count, frequency = Counter(counts).most_common(1)[0]

        if most_common_count <= 1:
            continue

        score = frequency / len(counts)

        if score > best_score:
            best_score = score
            best_delimiter = delimiter

    if best_score < 0.5:
        logger.warning(
            "No delimiter produced consistent columns for %s, defaulting to ','",
            file_path,
        )
        return ","

    logger.info("Detected delimiter %r for %s", best_delimiter, file_path)
    return best_delimiter


def detect_header(
    file_path: str, delimiter: str, storage_options: dict[str, Any] | None = None
) -> bool:
    """Detect if a delimited file has a header row using heuristics."""
    try:
        lines = _read_sample_lines(file_path, n=2, storage_options=storage_options)

        if len(lines) < 2:
            return True

        first_line = lines[0].strip()
        second_line = lines[1].strip()

        if not first_line or not second_line:
            return True

        first_values = first_line.split(delimiter)
        second_values = second_line.split(delimiter)

        if len(first_values) != len(second_values):
            return True

        first_numeric_count = sum(1 for v in first_values if _is_numeric(v.strip()))
        second_numeric_count = sum(1 for v in second_values if _is_numeric(v.strip()))

        if first_numeric_count == 0 and second_numeric_count > 0:
            return True

        if first_numeric_count > len(first_values) * 0.5:
            return False

        return True

    except Exception:
        return True


def _is_numeric(value: str) -> bool:
    if not value:
        return False
    try:
        float(value)
        return True
    except ValueError:
        return False
