# Getting Started

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

### Installing from TestPyPI (Pre-release)

To test the latest pre-release version from [TestPyPI](https://test.pypi.org/project/sifft/):

**Using pip:**
```bash
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ sifft
```

**Using uv:**
```bash
uv add --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ sifft
```

**Using poetry:**
```bash
# Add the TestPyPI source
poetry source add --priority=supplemental testpypi https://test.pypi.org/simple/
# Add the package from the source
poetry add --source testpypi sifft
```

## Quick Start

### 1. Read a File

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

```python
from file_processing import process_file, FileProcessingException

try:
    result = process_file("data.csv", spark, raise_on_error=True)
    df = result.dataframe
except FileProcessingException as e:
    print(f"Error: {e.message}")
```

### 2. Validate Data

```python
from dataframe_validation import validate_csvw_constraints

if result.metadata:
    report = validate_csvw_constraints(result.dataframe, result.metadata)

    if report.valid:
        print("✅ All constraints passed")
    else:
        for violation in report.violations:
            print(f"{violation.column}: {violation.message}")
```

### 3. Write to Table

```python
from table_writing import write_table, TableWriteOptions

options = TableWriteOptions(
    format="delta",
    mode="overwrite",
    partition_by=["date"]
)
result = write_table(df, "my_table", spark, options)

if result.success:
    print(f"Wrote {result.rows_written} rows in {result.duration_seconds:.2f}s")
```

## Complete Pipeline

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
