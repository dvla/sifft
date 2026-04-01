# File Management API

**Date:** 2025-03-02
**Status:** Accepted

## Context

Processed files need to be moved between directories (e.g., from an input landing zone to a processed archive). This needs to work across local filesystem and cloud storage (S3, Azure, GCS).

## Decision

- Two operations: `safe_move` and `list_files_in_directory`. Deliberately minimal scope — this is not a general-purpose filesystem library.
- Use `fsspec` for storage abstraction. Path format determines storage type automatically (`s3://`, `az://`, `gs://`, or local).
- `safe_move` checks source exists and destination does not exist before moving. No overwrite mode — prevents accidental data loss.
- Authentication is external to the API. Users authenticate via CLI tools (`az login`, `aws configure`) or environment variables before calling the API. Optional `storage_options` dict for explicit credentials.
- Return `FileOperationResult` objects by default. Raise `FileManagementException` only when `raise_on_error=True`.
- `_get_filesystem` is duplicated from `file_processing.storage` to avoid cross-package dependency.

## Consequences

- No overwrite on move means users must handle destination conflicts themselves. This is safer but less convenient.
- `list_files_in_directory` is non-recursive by design — keeps behaviour predictable.
- The fsspec abstraction means any storage backend fsspec supports could work, but only S3, Azure, GCS, and local are tested.
- No path traversal protection — the API trusts that callers provide valid paths.
