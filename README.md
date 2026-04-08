# SIFFT

File ingestion pipelines are repetitive and fragile. **SIFFT** (Spark Ingestion Framework For Tables) is a Python library that aims to:

* Make file reading format-agnostic with automatic delimiter and header detection
* Validate data quality with CSVW metadata and constraint checking before it enters your tables
* Provide consistent error handling across CSV, Excel, TSV, and pipe-delimited files
* Support schema evolution and merge/upsert operations for Delta tables
* Reduce boilerplate code for common Spark ingestion patterns

## Use cases

### Data engineers
* Ingest files from multiple sources without writing custom parsers for each format
* Validate data constraints (required fields, patterns, ranges) before writing to tables
* Handle schema mismatches and type conversions safely with detailed error reporting

### Data platform teams
* Standardize file ingestion patterns across projects with SIFFT
* Leverage CSVW metadata for self-documenting data pipelines
* Support both batch processing and single-file workflows

## SIFFT and Databricks AutoLoader

SIFFT and [AutoLoader](https://docs.databricks.com/en/ingestion/auto-loader/index.html) solve different problems and work well together.

**AutoLoader** answers: *"Which files are new?"*
It watches cloud storage for new files, tracks what's been seen, and incrementally feeds them into your pipeline using Structured Streaming. It handles file discovery and delivery at scale.

**SIFFT** answers: *"What's in this file, and can I trust it?"*
It parses file contents with format detection, validates data against CSVW constraints, handles schema mismatches, and writes to tables with merge support. It handles data quality and transformation.

| Concern | AutoLoader | SIFFT |
|---------|-----------|-------|
| File discovery | ✅ Cloud file notification/listing | ❌ You provide file paths |
| Incremental processing | ✅ Streaming with checkpointing | ✅ Checksum-based deduplication |
| Format detection | ❌ You configure the schema | ✅ Auto-detects delimiters, headers, types |
| Schema validation | ❌ Relies on Spark inference or manual schema | ✅ CSVW metadata with constraint checking |
| Data quality checks | ❌ Not its job | ✅ Required fields, patterns, ranges, enums, primary keys |
| Schema evolution | ✅ `mergeSchema` on read | ✅ `schema_mode` on write (strict/merge/overwrite) |
| Write modes | ❌ Append only (you handle merge downstream) | ✅ Append, overwrite, merge/upsert, errorIfExists |

### Using them together

AutoLoader discovers and delivers files. SIFFT validates and transforms the contents before they hit your tables.

**Pattern 1: SIFFT handles everything, AutoLoader triggers the pipeline.**

AutoLoader discovers new files and passes paths to SIFFT, which reads, validates, and writes:

```python
from file_processing import process_file
from dataframe_validation import validate_csvw_constraints
from table_writing import write_table, TableWriteOptions

def process_new_file(file_path, spark):
    """Called per file from AutoLoader's foreachBatch or downstream processing."""
    result = process_file(file_path, spark)
    if not result.success:
        return  # Log and handle

    if result.metadata:
        report = validate_csvw_constraints(result.dataframe, result.metadata)
        if not report.valid:
            return  # Quarantine bad data

    options = TableWriteOptions(mode="append", add_source_metadata=True)
    write_table(result.dataframe, "catalog.schema.target", spark, options,
                source_file_path=file_path)
```

**Pattern 2: AutoLoader reads the file, SIFFT validates and writes.**

When AutoLoader has already parsed the file into a DataFrame, skip `process_file` and use SIFFT for validation and writing only:

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

# AutoLoader discovers and reads files, SIFFT validates each micro-batch
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

### When to use which

- **AutoLoader alone**: Simple ingestion where files have consistent schemas and you trust the data quality. AutoLoader + `COPY INTO` or `readStream` is sufficient.
- **SIFFT alone**: Batch processing, backfills, ad-hoc loads, or environments without Databricks. SIFFT's own checksum tracking handles deduplication.
- **Both together**: Production pipelines where files arrive continuously AND you need data quality gates before writing. AutoLoader handles the "when", SIFFT handles the "what".

## Requirements

- Python 3.10+
- PySpark 3.5.x, 4.0.x, or 4.1.x (peer dependency — not installed automatically)
- Optional: pandas, openpyxl (for Excel support)

## Installation

```bash
# Install SIFFT (without PySpark — use the version provided by your Databricks runtime)
pip install sifft

# Or install with a specific PySpark version
pip install sifft[pyspark3]   # PySpark 3.5 + Delta Lake 3.x
pip install sifft[pyspark4]   # PySpark 4.x + Delta Lake 4.x
```

## Quick Start

### 1. Read a File

The toolkit automatically detects file format by extension, applies default delimiters, and handles header inference and schema parsing:

```python
from pyspark.sql import SparkSession
from file_processing import process_file

spark = SparkSession.builder.appName("Demo").getOrCreate()

result = process_file("data.csv", spark)
if result.success:
    df = result.dataframe
    print(f"Loaded {result.rows_processed} rows")
    df.show()
```

**Exception mode (fail-fast):**

For pipelines where you want to fail immediately on errors:

```python
from file_processing import process_file, FileProcessingException

try:
    result = process_file("data.csv", spark, raise_on_error=True)
    df = result.dataframe  # No need to check success
except FileProcessingException as e:
    print(f"Error: {e.message}")
```

**Skip already-processed files:**

For scheduled jobs that shouldn't reprocess the same files:

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
result = process_file(
    "data.csv",
    spark,
    tracking="marker_file",
    tracking_location="/markers"
)
print(result.rows_processed)  # 1000 (reprocessed)
```

**Delta table tracking:**

For queryable audit trails and easier cleanup:

```python
# Track processing in a Delta table
result = process_file(
    "data.csv",
    spark,
    tracking="delta_table",
    tracking_location="catalog.schema.file_tracking"
)

# Query what's been processed
spark.sql("SELECT * FROM catalog.schema.file_tracking").show()
```

**Deduplication behaviour:**

Files are skipped only if the same path AND same content have been processed before:

```python
# Same path, same content - skipped
process_file("landing/sales.csv", spark, tracking="marker_file", tracking_location="/markers")
process_file("landing/sales.csv", spark, tracking="marker_file", tracking_location="/markers")
# Second call skipped - same path and content

# Different paths, same content - both processed
process_file("2026/03/01/sales.csv", spark, tracking="marker_file", tracking_location="/markers")
process_file("2026/03/02/sales.csv", spark, tracking="marker_file", tracking_location="/markers")
# Both processed - different paths
```

For cloud storage (S3, Azure, GCS):

```python
result = process_file(
    "s3://bucket/data.csv",
    spark,
    tracking="marker_file",
    tracking_location="s3://bucket/markers",
    storage_options={"key": "...", "secret": "..."}
)
```

### 2. Validate Data

If your file has CSVW metadata, validate data quality constraints before loading into tables:

```python
from dataframe_validation import validate_csvw_constraints

# Validates if CSVW metadata exists
if result.metadata:
    report = validate_csvw_constraints(result.dataframe, result.metadata)
    
    if report.valid:
        print("✅ All constraints passed")
    else:
        for violation in report.violations:
            print(f"{violation.column}: {violation.message}")
```

### 3. Write to Table

Write validated data to Delta, Parquet, or ORC tables with support for append, overwrite, and merge operations:

```python
from table_writing import write_table, TableWriteOptions

# Simple append
result = write_table(df, "my_table", spark)

# With options
options = TableWriteOptions(
    format="delta",
    mode="overwrite",
    partition_by=["date"]
)
result = write_table(df, "my_table", spark, options)

if result.success:
    print(f"Wrote {result.rows_written} rows in {result.duration_seconds:.2f}s")
```

## CSVW Metadata Example

CSVW (CSV on the Web) metadata allows you to define schemas and validation constraints for your CSV files. Place a metadata file alongside your CSV with the naming pattern `{filename}-metadata.json`:

**employees.csv:**
```csv
id,name,age,department,salary
1,Alice,30,Engineering,75000
2,Bob,25,Sales,50000
3,Charlie,35,Engineering,85000
```

**employees-metadata.json:**
```json
{
  "@context": "http://www.w3.org/ns/csvw",
  "tableSchema": {
    "columns": [
      {
        "name": "id",
        "datatype": "integer",
        "required": true,
        "constraints": {
          "unique": true
        }
      },
      {
        "name": "name",
        "datatype": "string",
        "required": true,
        "constraints": {
          "minLength": 2,
          "maxLength": 50
        }
      },
      {
        "name": "age",
        "datatype": "integer",
        "constraints": {
          "minimum": 18,
          "maximum": 65
        }
      },
      {
        "name": "department",
        "datatype": "string",
        "constraints": {
          "enum": ["Engineering", "Sales", "Marketing", "HR"]
        }
      },
      {
        "name": "salary",
        "datatype": "integer",
        "constraints": {
          "minimum": 30000
        }
      }
    ],
    "primaryKey": ["id"]
  }
}
```

**Using CSVW metadata:**
```python
from file_processing import process_file
from dataframe_validation import validate_csvw_constraints

# Metadata is automatically discovered
result = process_file("employees.csv", spark)

if result.metadata:
    # Schema is applied from metadata
    print(f"Schema: {result.dataframe.schema}")
    
    # Validate constraints
    report = validate_csvw_constraints(result.dataframe, result.metadata)
    
    if not report.valid:
        for violation in report.violations:
            print(f"❌ {violation.column}: {violation.message}")
            print(f"   Violating rows: {violation.violating_rows}")
```

**Supported constraints:**
- `required` - No null values
- `unique` - No duplicates
- `minimum` / `maximum` - Numeric bounds
- `minLength` / `maxLength` - String length
- `pattern` - Regex validation
- `enum` - Allowed values
- `primaryKey` - Composite uniqueness

## Complete Pipeline Example

```python
from pyspark.sql import SparkSession
from file_processing import process_file
from dataframe_validation import validate_csvw_constraints
from table_writing import write_table, TableWriteOptions

spark = SparkSession.builder.appName("Pipeline").getOrCreate()

# Step 1: Read file
result = process_file("data.csv", spark)

# Step 2: Validate
if result.metadata:
    report = validate_csvw_constraints(result.dataframe, result.metadata)
    if not report.valid:
        print(f"Validation failed: {len(report.violations)} violations")
        exit(1)

# Step 3: Write to table
options = TableWriteOptions(format="delta", mode="overwrite")
write_result = write_table(result.dataframe, "output_table", spark, options)

print(f"Pipeline complete: {write_result.rows_written} rows written")
```


## Extensibility

The toolkit supports custom handlers and validators across all modules, allowing you to extend functionality for your specific needs.

### File Processing - Custom Format Handlers

Register custom handlers for file formats:

```python
from file_processing import register_handler, FileProcessingResult

def custom_dat_handler(file_path, spark, options):
    """Handle .dat files with summary rows."""
    lines = open(file_path).readlines()[1:]  # Skip first line
    df = spark.createDataFrame(...)
    
    return FileProcessingResult(
        success=True,
        file=file_path,
        dataframe=df,
        message="Processed .dat file",
        rows_processed=df.count()
    )

register_handler(".dat", custom_dat_handler, priority=10)
```

**Registry functions:**
```python
from file_processing import register_handler, unregister_handler, list_registered_extensions
```

### Table Writing - Custom Mode and Format Handlers

Register custom write modes or table formats:

```python
from table_writing import register_mode_handler, TableWriteResult

def incremental_mode(df, table_name, spark, options, raise_on_error):
    """Custom incremental load logic."""
    # Your implementation
    return TableWriteResult(
        success=True,
        table_name=table_name,
        rows_written=df.count(),
        message="Incremental load complete"
    )

register_mode_handler("incremental", incremental_mode)

# Use it
options = TableWriteOptions(mode="incremental", format="parquet")
result = write_table(df, "my_table", spark, options)
```

**Custom format handler:**
```python
from table_writing import register_format_handler

class IcebergFormatHandler:
    def supports_merge(self):
        return True
    
    def write(self, df, table_name, mode, options):
        df.write.format("iceberg").mode(mode).saveAsTable(table_name)

register_format_handler("iceberg", IcebergFormatHandler())
```

**Registry functions:**
```python
from table_writing import (
    register_mode_handler, unregister_mode_handler, list_registered_modes,
    register_format_handler, get_format_handler
)
```

### DataFrame Validation - Custom Constraint Validators

Register custom validation constraints for CSVW validation:

```python
from pyspark.sql.functions import col, count, when
from pyspark.sql.functions import sum as spark_sum
from dataframe_validation import register_constraint_validator, ConstraintViolation

def balance_limit_validator(df, column_name, config):
    """Check that balance doesn't exceed credit_limit."""
    limit_col = config.get("limitColumn", "credit_limit")
    
    result = df.select(
        count("*").alias("total"),
        spark_sum(when(col(column_name) > col(limit_col), 1).otherwise(0)).alias("violations"),
    ).collect()[0]
    
    if result["violations"] > 0:
        return [ConstraintViolation(
            column=column_name,
            constraint_type="balanceLimit",
            message=f"{column_name} exceeds {limit_col}",
            violating_rows=result["violations"],
            total_rows=result["total"],
        )]
    return []

register_constraint_validator("balanceLimit", balance_limit_validator)
```

**Use in CSVW metadata:**
```json
{
  "tableSchema": {
    "columns": [{
      "name": "balance",
      "constraints": {
        "balanceLimit": {"limitColumn": "credit_limit"}
      }
    }]
  }
}
```

**Registry functions:**
```python
from dataframe_validation import (
    register_constraint_validator, unregister_constraint_validator, list_registered_constraints
)
```

## Supported Formats

### Input Formats

The toolkit supports reading the following file formats:

- **CSV** (`.csv`, `.txt`, `.eot`) - Comma-delimited files following [RFC 4180](https://tools.ietf.org/html/rfc4180)
- **TSV** (`.tsv`) - Tab-delimited files
- **Pipe-delimited** (`.dat`, `.out`) - Pipe-separated values
- **Excel** (`.xlsx`, `.xls`) - Microsoft Excel files via [openpyxl](https://openpyxl.readthedocs.io/)

### Metadata Standards

- **CSVW** - [CSV on the Web](https://www.w3.org/TR/tabular-data-primer/) (W3C Recommendation) for schema and constraint definitions

### Output Formats

The toolkit supports writing to the following table formats:

- **Delta Lake** - [Delta Lake](https://delta.io/) open-source storage layer with ACID transactions
- **Parquet** - [Apache Parquet](https://parquet.apache.org/) columnar storage format
- **ORC** - [Apache ORC](https://orc.apache.org/) optimized row columnar format

## API Reference

### File Processing

Format is detected by file extension. Default delimiters are applied automatically.

**Functions:**

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

**Tracking Methods:**

| Method | Location | Use Case |
|--------|----------|----------|
| `"marker_file"` | `/markers/ab/c1/{checksum}_{filename}.processed` | Simple file-based tracking |
| `"delta_table"` | `catalog.schema.tracking_table` | Queryable audit trail |

**Deduplication Logic:**

Files are skipped only when both checksum AND full path match a previous record:
- Same path, same content → skipped
- Same path, different content → processed (file was updated)
- Different path, same content → both processed
- Different path, different content → both processed

**Tracking Functions:**

```python
from file_processing import check_already_processed, record_processed, clear_tracking

# Check if file was processed
check_already_processed(file_path, "marker_file", "/markers", spark)

# Manually record processing
record_processed(file_path, "marker_file", "/markers", spark, rows_processed=100)

# Clear tracking to allow reprocessing
clear_tracking(file_path, "marker_file", "/markers", spark)
```

**Options:**
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

**CSVW Metadata:**
Place a `{filename}-metadata.json` file alongside your CSV to define schema and constraints. Alternatively, specify a custom path:

```python
result = process_file("data.csv", spark, metadata_path="/path/to/metadata.json")
```

### Data Validation

**Supported Constraints:**

Column-level:
- `required` - No null values
- `unique` - No duplicates
- `minimum` / `maximum` - Numeric bounds
- `minLength` / `maxLength` - String length
- `pattern` - Regex validation
- `enum` - Allowed values

Table-level:
- `primaryKey` - Composite uniqueness

**Functions:**

`validate_csvw_constraints(df, metadata)`
- Validate DataFrame against CSVW metadata
- Returns: `CSVWConstraintReport` with `valid`, `violations`, `total_violations`

`validate_schema(df, expected_schema)`
- Validate DataFrame schema against expected schema
- Returns: Dictionary mapping column names to `ValidationLog` objects

`apply_schema(df, expected_schema, raise_on_cast_failure=False)`
- Apply expected schema to DataFrame (rename columns and cast types)
- `raise_on_cast_failure`: If True, raises ValueError if any casts would produce nulls
- Returns: Transformed DataFrame

`format_violations(report)`
- Format validation violations into readable string
- Returns: Formatted string with violation details

**Example:**
```python
from dataframe_validation import validate_csvw_constraints, format_violations

report = validate_csvw_constraints(df, metadata)

if not report.valid:
    print(format_violations(report))
```

### Table Writing

**Write Modes:**
- `append` - Add rows to existing table (default)
- `overwrite` - Replace all data
- `merge` - Upsert based on merge keys (Delta only)
- `errorIfExists` - Fail if table exists

**Formats:**
- `delta` (default)
- `parquet`
- `orc`

**Schema Modes:**
- `strict` - Exact schema match required (default)
- `merge` - Add new columns from DataFrame
- `overwrite` - Replace table schema

**Functions:**

`write_table(df, table_name, spark, options=None)`
- Write DataFrame to table
- Returns: `TableWriteResult` with `success`, `rows_written`, `duration_seconds`, `message`, `error`

**TableWriteOptions:**
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

**Examples:**

Simple append:
```python
result = write_table(df, "my_table", spark)
```

Overwrite with partitioning:
```python
options = TableWriteOptions(
    mode="overwrite",
    partition_by=["year", "month"]
)
result = write_table(df, "my_table", spark, options)
```

With source metadata and custom columns:
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

Merge/upsert (Delta only):
```python
options = TableWriteOptions(
    format="delta",
    mode="merge",
    merge_keys=["id"]
)
result = write_table(df, "my_table", spark, options)
```

**TableWriteResult:**
```python
result.success              # bool - True if write succeeded
result.rows_written         # int - Number of rows written
result.duration_seconds     # float - Time taken
result.message              # str - Success or error message
result.error                # dict - Error details if failed
```

### File Management

**Functions:**

`safe_move(src_path, dst_path, storage_options=None, raise_on_error=False)`
- Move a file safely with validation checks
- `storage_options`: Optional dict for custom authentication credentials
- Supports local and cloud storage (S3, Azure, GCS)
- `raise_on_error`: If True, raises FileManagementException on failure
- Returns: `FileOperationResult` with `success`, `source_path`, `destination_path`, `error`

`list_files_in_directory(directory, storage_options=None, raise_on_error=False)`
- `storage_options`: Optional dict for custom authentication credentials
- List all files in a directory (non-recursive)
- Supports local and cloud storage (S3, Azure, GCS)
- `raise_on_error`: If True, raises FileManagementException on failure

**Supported Storage:**
- Local filesystem: `/path/to/file.csv`
- AWS S3: `s3://bucket/path/to/file.csv`
- Azure Blob: `az://container/path/to/file.csv`
- Google Cloud Storage: `gs://bucket/path/to/file.csv`
- Returns: `FileOperationResult` with `success`, `files` list, `error`

**Example:**
```python
from file_management import safe_move, list_files_in_directory

# Move a file
result = safe_move("input/data.csv", "processed/data.csv")
if result.success:
    print("File moved successfully")

# Move from S3 to S3
result = safe_move("s3://bucket/input/data.csv", "s3://bucket/processed/data.csv")

# S3 with custom credentials
storage_options = {
    "key": "AWS_ACCESS_KEY_ID",
    "secret": "AWS_SECRET_ACCESS_KEY"
}
result = safe_move(
    "s3://bucket/input/data.csv",
    "s3://bucket/processed/data.csv",
    storage_options=storage_options
)

# List files in a directory
result = list_files_in_directory("input/")
if result.success:
    for file in result.files:
        print(f"{file['name']}: {file['size']} bytes")

# List files in S3 bucket
result = list_files_in_directory("s3://bucket/input/")
if result.success:
    for file in result.files:
        print(f"{file['name']}: {file['size']} bytes")
```

## Development

### Setup

```bash
# Clone the repository
git clone <repository-url>
cd file-ingestion-framework

# Install dependencies with uv (recommended)
uv sync

# Or with pip
pip install -e ".[dev]"

# Install pre-commit hooks
pre-commit install
```

### Running Tests

The project uses Docker to provide a consistent Spark environment for testing. This ensures tests run the same way locally and in CI/CD pipelines without requiring a local Spark installation.

```bash
# Run all tests with Docker (recommended)
just test

# Run tests against all supported PySpark versions
just test-all

# Run tests with a specific PySpark version
PYSPARK_VERSION=3.5 DELTA_VERSION=3.3 PYTHON_VERSION=3.12 just test

# Run tests locally (requires local Spark installation)
just test-local

# Run specific test file
just test-file tests/test_file_processing.py
```

**Docker setup:**
- `docker-compose.yml` defines the Spark test environment
- `Dockerfile` accepts `PYTHON_VERSION`, `PYSPARK_VERSION`, and `DELTA_VERSION` build args
- Tests run in an isolated container to avoid environment conflicts

### Available Commands

See `justfile` for all available commands:

```bash
just --list
```

Common commands:
- `just format` - Format code with ruff
- `just lint` - Run linting checks
- `just build` - Build package
- `just demo` - Run demo examples

## Design Decisions

Architectural Decision Records (ADRs) are in the [design_decisions](design_decisions/) directory:

- [Architecture Overview](design_decisions/architecture-overview.md) — why four independent packages
- [File Processing API](design_decisions/file-processing-api.md) — file-to-DataFrame conversion choices
- [DataFrame Validation API](design_decisions/dataframe-validation-api.md) — schema and constraint validation choices
- [Table Writing API](design_decisions/table-writing-api.md) — DataFrame-to-table persistence choices
- [File Management API](design_decisions/file-management-api.md) — file operations and storage abstraction choices
- [File Tracking and Deduplication](design_decisions/file-tracking-and-deduplication.md) — duplicate detection and tracking choices

## Compatibility Notes

### Databricks / Unity Catalog

- **ANSI mode cast behaviour**: Databricks clusters run with ANSI mode enabled by default. This means `apply_schema()` with incompatible type casts (e.g., `"Alice"` to `IntegerType`) will raise a Spark `AnalysisException` rather than silently producing nulls. The `raise_on_cast_failure` flag is effectively redundant on ANSI-mode clusters since Spark enforces strict casting natively.

- **Schema strictness on overwrite**: When using `mode="overwrite"` with `schema_mode="strict"` (the default), Delta Lake on Databricks will reject writes where the DataFrame schema doesn't exactly match the existing table schema (e.g., `IntegerType` vs `LongType` from inference differences). Use `schema_mode="overwrite"` if the incoming schema may differ.

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Contributors

- Iwan Dyke
- Fahad Khan
- Michal Poreba
