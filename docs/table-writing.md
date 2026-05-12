# Table Writing

Write validated data to Delta, Parquet, or ORC tables with support for append, overwrite, and merge operations.

## Functions

`write_table(df, table_name, spark, options=None)`
- Write DataFrame to table
- Returns: `TableWriteResult` with `success`, `rows_written`, `duration_seconds`, `message`, `error`

## Write Modes

- `append` - Add rows to existing table (default)
- `overwrite` - Replace all data
- `merge` - Upsert based on merge keys (Delta only)
- `errorIfExists` - Fail if table exists

## Formats

- `delta` (default)
- `parquet`
- `orc`

## Schema Modes

- `strict` - Exact schema match required (default)
- `merge` - Add new columns from DataFrame
- `overwrite` - Replace table schema

## TableWriteOptions

```python
TableWriteOptions(
    format="delta",              # Table format
    mode="append",               # Write mode
    partition_by=None,           # List of partition columns
    schema_mode="strict",        # Schema evolution mode
    merge_keys=None,             # Columns for merge mode
    create_if_not_exists=True,   # Auto-create table
    add_source_metadata=False,   # Add source file metadata columns
    extra_metadata=None          # Additional custom metadata columns
)
```

## Examples

**Simple append:**
```python
result = write_table(df, "my_table", spark)
```

**Overwrite with partitioning:**
```python
options = TableWriteOptions(
    mode="overwrite",
    partition_by=["year", "month"]
)
result = write_table(df, "my_table", spark, options)
```

**With source metadata and custom columns:**
```python
options = TableWriteOptions(
    add_source_metadata=True,
    extra_metadata={
        "batch_id": "2026-03-10-001",
        "checksum": result.checksum,
        "environment": "production"
    }
)
result = write_table(
    df, "my_table", spark, options,
    source_file_path="s3://bucket/data.csv"
)
```

**Merge/upsert (Delta only):**
```python
options = TableWriteOptions(
    format="delta",
    mode="merge",
    merge_keys=["id"]
)
result = write_table(df, "my_table", spark, options)
```

## TableWriteResult

```python
result.success              # bool - True if write succeeded
result.rows_written         # int - Number of rows written
result.duration_seconds     # float - Time taken
result.message              # str - Success or error message
result.error                # dict - Error details if failed
```
