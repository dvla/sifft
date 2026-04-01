"""Utility functions for file validation and header detection."""

from pathlib import Path


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


def detect_header(file_path: str, delimiter: str) -> bool:
    """Detect if a delimited file has a header row using heuristics."""
    try:
        with open(file_path, encoding="utf-8", errors="ignore") as f:
            first_line = f.readline().strip()
            second_line = f.readline().strip()

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
