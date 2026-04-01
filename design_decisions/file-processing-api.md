# File Processing API

**Date:** 2025-02-23
**Status:** Accepted

## Context

The framework needs to convert files from various formats (CSV, TSV, DAT, OUT, Excel, CSVW) into Spark DataFrames with minimal user configuration, while handling messy real-world data gracefully.

## Decision

- Use file extension to determine default delimiter and format. Users can override via `read_options`.
- When CSVW metadata exists, use it instead of schema inference. Metadata is auto-discovered using standard naming conventions (`.csv-metadata.json`, `-metadata.json`, `metadata.json`).
- Accept files with up to 10% corrupt records by default (configurable via `max_corrupt_records_percent`). Reject the file if corruption exceeds the threshold.
- Return `FileProcessingResult` objects by default. Raise `FileProcessingException` only when `raise_on_error=True`.
- Reject Excel files over 100MB by default (configurable) because pandas loads the entire file into memory.
- Auto-detect headers using heuristics (numeric vs text first row) when not explicitly specified.
- Keep file processing separate from validation — this module only creates DataFrames, it does not validate data quality.
- Support custom file format handlers via a priority-based registry (`register_handler`).

## Consequences

- Extension-based detection is simple and predictable but assumes extensions match content. Users must override for mismatched files.
- The 10% corruption threshold means some bad data gets through by default. Users in strict environments should set this to 0.
- CSVW metadata adds a network dependency (JSON-LD context resolution) which falls back to raw metadata if the network is unavailable.
- Excel support is local-only (uses `Path.stat()`) and does not support cloud storage paths.
- Header auto-detection can misidentify edge cases like all-numeric headers.
