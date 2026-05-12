# SIFFT and Databricks AutoLoader

SIFFT and [AutoLoader](https://docs.databricks.com/en/ingestion/auto-loader/index.html) solve different problems and work well together.

**AutoLoader** answers: *"Which files are new?"*
It watches cloud storage for new files, tracks what's been seen, and incrementally feeds them into your pipeline using Structured Streaming.

**SIFFT** answers: *"What's in this file, and can I trust it?"*
It parses file contents with format detection, validates data against CSVW constraints, handles schema mismatches, and writes to tables with merge support.

| Concern | AutoLoader | SIFFT |
|---------|-----------|-------|
| File discovery | ✅ Cloud file notification/listing | ❌ You provide file paths |
| Incremental processing | ✅ Streaming with checkpointing | ✅ Checksum-based deduplication |
| Format detection | ❌ You configure the schema | ✅ Auto-detects delimiters, headers, types |
| Schema validation | ❌ Relies on Spark inference or manual schema | ✅ CSVW metadata with constraint checking |
| Data quality checks | ❌ Not its job | ✅ Required fields, patterns, ranges, enums, primary keys |
| Schema evolution | ✅ `mergeSchema` on read | ✅ `schema_mode` on write (strict/merge/overwrite) |
| Write modes | ❌ Append only (you handle merge downstream) | ✅ Append, overwrite, merge/upsert, errorIfExists |

## Using Them Together

AutoLoader discovers and delivers files. SIFFT validates and transforms the contents before they hit your tables.

### Pattern 1: SIFFT handles everything, AutoLoader triggers the pipeline

```python
from file_processing import process_file
from dataframe_validation import validate_csvw_constraints
from table_writing import write_table, TableWriteOptions

def process_new_file(file_path, spark):
    """Called per file from AutoLoader's foreachBatch or downstream processing."""
    result = process_file(file_path, spark)
    if not result.success:
        return

    if result.metadata:
        report = validate_csvw_constraints(result.dataframe, result.metadata)
        if not report.valid:
            return  # Quarantine bad data

    options = TableWriteOptions(mode="append", add_source_metadata=True)
    write_table(result.dataframe, "catalog.schema.target", spark, options,
                source_file_path=file_path)
```

### Pattern 2: AutoLoader reads the file, SIFFT validates and writes

```python
from dataframe_validation import validate_csvw_constraints
from table_writing import write_table, TableWriteOptions

def validate_and_write(batch_df, batch_id):
    """Used as a foreachBatch function with AutoLoader's streaming DataFrame."""
    metadata = load_csvw_metadata("path/to/metadata.json")

    if metadata:
        report = validate_csvw_constraints(batch_df, metadata)
        if not report.valid:
            return  # Quarantine bad data

    options = TableWriteOptions(mode="append")
    write_table(batch_df, "catalog.schema.target", spark, options)

(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(my_schema)
    .load("s3://bucket/landing/")
    .writeStream
    .foreachBatch(validate_and_write)
    .option("checkpointLocation", "/checkpoints/pipeline")
    .start()
)
```

## When to Use Which

- **AutoLoader alone**: Simple ingestion where files have consistent schemas and you trust the data quality.
- **SIFFT alone**: Batch processing, backfills, ad-hoc loads, or environments without Databricks. SIFFT's own checksum tracking handles deduplication.
- **Both together**: Production pipelines where files arrive continuously AND you need data quality gates before writing.
