# File Processing

Format is detected by file extension. Default delimiters are applied automatically.

## Functions

`process_file(file_path, spark, read_options=None, metadata_path=None, raise_on_error=False, tracking=None, tracking_location=None, storage_options=None)`
- Process a single file
- `raise_on_error`: If True, raises FileProcessingException instead of returning error result
- `tracking`: Tracking method - `"marker_file"` or `"delta_table"`. If None, no tracking.
- `tracking_location`: Directory for markers or Delta table name. Required if tracking is set.
- `storage_options`: Optional dict for cloud storage authentication
- Returns: `FileProcessingResult` with `success`, `dataframe`, `rows_processed`, `metadata`, `checksum`

`process_files_batch(file_paths, spark, read_options=None, raise_on_error=False)`
- Process multiple files
- `raise_on_error`: If True, raises FileProcessingException after processing all files if any failed
- Returns: List of `FileProcessingResult`

`process_directory(directory_path, spark, read_options=None, raise_on_error=False)`
- Process all files in a directory
- `raise_on_error`: If True, raises FileProcessingException after processing all files if any failed
- Returns: List of `FileProcessingResult`

## Read Options

```python
read_options = {
    "delimiter": "|",                        # Override default delimiter
    "header": "true",                        # First row is header (default: "true")
                                             # Options: "true", "false", "auto"
    "encoding": "utf-8",                     # File encoding
    "nullValue": "NULL",                     # String to treat as null
    "max_file_size_mb": 100,                 # Maximum Excel file size in MB
    "max_corrupt_records_percent": 0.1,      # Maximum corruption threshold (10%)
    "sheet_name": 0,                         # Excel sheet to read
}
```

**Header Detection:**
- Default: `"true"` (assumes first row is header - most common case)
- `"false"`: No header row, Spark generates column names (_c0, _c1, etc.)
- `"auto"`: Automatically detect if header exists (uses heuristics)

## File Tracking and Deduplication

Skip already-processed files using checksum-based tracking:

```python
from file_processing import process_file, clear_tracking

# First run - processes file, creates marker
result = process_file(
    "data.csv",
    spark,
    tracking="marker_file",
    tracking_location="/markers"
)
print(result.rows_processed)  # 1000

# Second run - skips (same file, same content)
result = process_file(
    "data.csv",
    spark,
    tracking="marker_file",
    tracking_location="/markers"
)
print(result.rows_processed)  # 0 (skipped)

# Force reprocessing
clear_tracking("data.csv", "marker_file", "/markers", spark)
```

**Delta table tracking:**

```python
result = process_file(
    "data.csv",
    spark,
    tracking="delta_table",
    tracking_location="catalog.schema.file_tracking"
)

# Query what's been processed
spark.sql("SELECT * FROM catalog.schema.file_tracking").show()
```

### Tracking Methods

| Method | Location | Use Case |
|--------|----------|----------|
| `"marker_file"` | `/markers/ab/c1/{checksum}_{filename}.processed` | Simple file-based tracking |
| `"delta_table"` | `catalog.schema.tracking_table` | Queryable audit trail |

### Deduplication Logic

Files are skipped only when both checksum AND full path match a previous record:
- Same path, same content → skipped
- Same path, different content → processed (file was updated)
- Different path, same content → both processed
- Different path, different content → both processed

### Tracking Functions

```python
from file_processing import check_already_processed, record_processed, clear_tracking

# Check if file was processed
check_already_processed(file_path, "marker_file", "/markers", spark)

# Manually record processing
record_processed(file_path, "marker_file", "/markers", spark, rows_processed=100)

# Clear tracking to allow reprocessing
clear_tracking(file_path, "marker_file", "/markers", spark)
```

### Cloud Storage

```python
result = process_file(
    "s3://bucket/data.csv",
    spark,
    tracking="marker_file",
    tracking_location="s3://bucket/markers",
    storage_options={"key": "...", "secret": "..."}
)
```

## Supported Input Formats

- **CSV** (`.csv`, `.txt`, `.eot`) - Comma-delimited files following [RFC 4180](https://tools.ietf.org/html/rfc4180)
- **TSV** (`.tsv`) - Tab-delimited files
- **Pipe-delimited** (`.dat`, `.out`) - Pipe-separated values
- **Excel** (`.xlsx`, `.xls`) - Microsoft Excel files via [openpyxl](https://openpyxl.readthedocs.io/)
