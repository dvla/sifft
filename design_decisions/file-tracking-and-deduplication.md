# File Tracking and Deduplication

**Date:** 2025-03-12
**Status:** Accepted

## Context

Scheduled batch jobs processing landing zones need to avoid reprocessing files that have already been successfully ingested. The question was how to detect duplicates and where to store tracking state.

## Decision

- Use MD5 checksums of file content for duplicate detection, not timestamps or filenames. A file modified then reverted should be detected as unchanged. MD5 is sufficient for deduplication (not used for security).
- Deduplication key is checksum + filename (basename only). Same file with same content is skipped. Same filename with different content is processed. Different filename with same content is also processed and logged.
- Two tracking backends: marker files and Delta tables. Users choose based on needs.
- Marker files use bucketed paths (`/markers/{checksum[:2]}/{checksum[2:4]}/{checksum}_{filename}.processed`) to prevent any single directory from growing huge. Contents are JSON with source file, checksum, timestamp, and row count.
- Delta table tracking uses soft deletes (`is_active=False`) rather than row deletion, preserving audit history.
- `tracking_location` is always required when tracking is enabled. No "marker next to file" mode — it pollutes data directories and complicates cloud storage permissions.
- Unified interface via `tracking.py` that delegates to `checksum.py` (marker files) or `delta_tracking.py` (Delta tables).
- Delta table names are validated with regex `^[a-zA-Z0-9_\.]+$` before use in SQL to prevent injection.

## Consequences

- Checksum computation requires reading the entire file, adding I/O overhead. Acceptable because Spark reads the file anyway for processing.
- MD5 is not collision-resistant, but collisions are astronomically unlikely for deduplication purposes.
- Marker file cleanup requires filesystem walks. Delta table cleanup is a simple SQL delete.
- The basename-only filename matching means `2026/03/01/data.csv` and `2026/03/02/data.csv` share the same dedup key — they're treated as the same logical file. This handles date-partitioned landing zones correctly.
