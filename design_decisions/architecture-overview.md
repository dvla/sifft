# Architecture Overview

**Date:** 2025-02-23
**Status:** Accepted

## Context

SIFFT needs to handle file ingestion, validation, table writing, and file management. The question was whether to build a single monolithic API or separate focused modules.

## Decision

Four independent packages, each with a single responsibility:

- `file_processing` — converts files to DataFrames
- `dataframe_validation` — validates DataFrame structure and data quality
- `table_writing` — persists DataFrames to tables
- `file_management` — handles file operations (move, list)

Each package is independently importable with no cross-package dependencies. Shared logic (e.g., filesystem helpers, table schema extraction) is duplicated intentionally to maintain independence.

The internal architecture follows a pipes and filters pattern — file discovery → format detection → DataFrame conversion → error handling → output.

## Consequences

- Users compose only the packages they need rather than importing a monolith
- Each package can be tested in isolation
- Adding new file formats only affects `file_processing`, new validation rules only affect `dataframe_validation`, etc.
- Duplicated utility code between packages must be kept in sync manually
- All packages follow the same conventions: result objects, `raise_on_error` flags, custom exception classes with `to_dict()`, and extensible registries
