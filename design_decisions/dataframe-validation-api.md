# DataFrame Validation API

**Date:** 2025-02-23
**Status:** Accepted

## Context

After file processing creates a DataFrame, users need to validate that the schema and data meet expectations before writing to tables. The question was whether validation should be built into file processing or kept separate.

## Decision

- Validation is a separate module. Not all use cases need it, and it can be expensive (full DataFrame scans).
- Two types of validation: schema validation (`validate_schema`/`apply_schema`) and CSVW constraint validation (`validate_csvw_constraints`).
- Use `type()` for exact type matching, not `isinstance()`. Spark types don't have meaningful inheritance — `IntegerType` is not `LongType`.
- Casts that fail produce nulls by default (Spark's native behaviour). Users opt into strict mode with `raise_on_cast_failure=True`.
- Invalid inputs (None DataFrame, bad metadata) return `valid=False` with error violations, not `valid=True`.
- Invalid regex patterns raise `ValidationException` rather than returning empty results, because empty results look like "validation passed".
- Batch all numeric/length constraints into a single DataFrame scan per column for performance.
- Limit regex patterns to 500 characters to prevent ReDoS.
- Support custom constraint validators via a registry (`register_constraint_validator`).

## Consequences

- Exact type matching is strict — users must cast explicitly if they want looser matching. This prevents subtle bugs from "compatible" types.
- Validation is opt-in, so users can skip it entirely for speed.
- Single-scan optimisation means adding new constraint types must fit the batched aggregation pattern or use a separate scan.
- The 500-char pattern limit may reject legitimate long patterns, but prevents catastrophic backtracking.
