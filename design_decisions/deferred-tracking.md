# Deferred Tracking Confirmation

**Date:** 2026-06-02
**Status:** Accepted

## Context

`process_file()` wrote a tracking record immediately on successful read, before the caller validated or wrote downstream. If anything failed after read (validation, table write), the file was permanently marked as processed and skipped on subsequent runs. This created silent data gaps requiring manual intervention to recover.

## Decision

- `process_file()` no longer writes tracking records on success. Instead it stores tracking context on the result object (`result.tracking_context`).
- The caller must explicitly call `confirm_processed(result, spark)` after their full pipeline succeeds (validation, table write, etc.).
- If `confirm_processed` is never called (e.g. downstream failure), no tracking record exists and the file is reprocessed on the next run.
- `storage_options` (which may contain cloud credentials) is accepted as a parameter on `confirm_processed()`, not stored on the result object.
- `tracking_context` is `None` when tracking is not enabled or processing failed, making `confirm_processed` a safe no-op in those cases.

## Consequences

- Callers must update to call `confirm_processed` — this is a breaking change (version bumped to 0.9.0).
- Files that fail downstream are automatically eligible for retry without manual intervention.
- The tracking record is only written once the full pipeline is confirmed successful, eliminating false-positive skips.
- Slight increase in caller responsibility, but the failure mode is now visible (reprocessing) rather than silent (data loss).
