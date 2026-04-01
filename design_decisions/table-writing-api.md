# Table Writing API

**Date:** 2025-03-02
**Status:** Accepted

## Context

Validated DataFrames need to be persisted to Databricks tables with support for different formats, write modes, and schema evolution strategies.

## Decision

- Support Delta, Parquet, and ORC formats. Merge mode is restricted to Delta only (native merge support).
- Four write modes: append, overwrite, merge (upsert via merge keys), and errorIfExists.
- Three schema modes: strict (default, fail on mismatch), merge (add new columns), overwrite (replace schema).
- Auto-create tables by default (`create_if_not_exists=True`).
- Run pre-write validation before attempting any write — catch configuration errors (invalid table names, missing merge keys, incompatible options) before expensive operations.
- Table name validation uses regex `^[a-zA-Z0-9_\.]+$` to prevent SQL injection in merge conditions built with f-strings.
- Return `TableWriteResult` with metrics (rows written, updated, deleted, duration). Raise `TableWritingException` only when `raise_on_error=True`.
- Support custom format handlers (`register_format_handler`) and mode handlers (`register_mode_handler`) via registries.
- Optional source metadata columns (file path, filename, timestamp, size) added to the DataFrame before writing.

## Consequences

- Strict schema mode by default means first-time users may hit errors if their DataFrame doesn't exactly match an existing table. This is intentional — silent schema drift is worse.
- Merge mode requires explicit merge keys (not inferred) to prevent accidental full-table updates.
- Pre-write validation adds overhead but prevents partial writes from bad configuration.
- Mode handlers are registered at module import time as a side effect, which means all built-in modes are always available.
