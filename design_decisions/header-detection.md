# Header Detection Heuristic

**Date:** 2026-06-15
**Status:** Accepted — supersedes header detection section in file-processing-api.md

## Context

The original `detect_header()` compared only 2 lines and defaulted to `True` (assume header) when uncertain. For all-text CSV files (codes, descriptions, date strings), both rows scored zero numerics, so the heuristic always assumed a header was present — silently consuming the first data row as column names.

## Decision

- Compare row 1 against up to 19 data rows (not just row 2) to establish whether row 1 is an outlier.
- When the heuristic cannot distinguish row 1 from data rows (same numeric profile), default to `False` (no header). Header-as-data is visible and recoverable; a silently lost first row is not.
- Single-column files are explicitly handled: log a warning that detection is ambiguous and return `False`. Callers should pass `header` explicitly for single-column files.
- The "preserve data" default aligns with the framework's philosophy that silent data loss is worse than requiring explicit configuration.

## Consequences

- All-text files without headers now correctly retain their first data row.
- Files with genuine all-text headers (e.g. `colour,shape,size` followed by `red,circle,large`) will no longer auto-detect the header. Callers must pass `header=True` explicitly for these cases.
- Single-column files emit a warning, making the ambiguity visible rather than silently guessing.
- Existing files with text headers + numeric data are unaffected (the primary heuristic still fires correctly).
