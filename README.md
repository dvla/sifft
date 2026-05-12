# SIFFT

**Spark Ingestion Framework For Tables** — format-agnostic file reading, data validation, and table writing for PySpark.

## Install

```bash
pip install sifft              # Without PySpark (use your Databricks runtime)
pip install sifft[pyspark3]    # With PySpark 3.5 + Delta Lake 3.x
pip install sifft[pyspark4]    # With PySpark 4.x + Delta Lake 4.x
```

## Quick Start

```python
from pyspark.sql import SparkSession
from file_processing import process_file
from dataframe_validation import validate_csvw_constraints
from table_writing import write_table, TableWriteOptions

spark = SparkSession.builder.appName("Pipeline").getOrCreate()

# Read (auto-detects format, delimiter, header)
result = process_file("data.csv", spark)

# Validate (if CSVW metadata exists alongside the file)
if result.metadata:
    report = validate_csvw_constraints(result.dataframe, result.metadata)
    assert report.valid, f"{len(report.violations)} violations"

# Write
write_table(result.dataframe, "catalog.schema.target", spark,
            TableWriteOptions(format="delta", mode="append"))
```

## Features

- **File Processing** — CSV, TSV, pipe-delimited, Excel. Auto-detection of delimiters and headers. Checksum-based deduplication.
- **Data Validation** — CSVW constraint checking: required, unique, min/max, pattern, enum, primary keys.
- **Table Writing** — Delta, Parquet, ORC. Append, overwrite, merge/upsert, schema evolution.
- **File Management** — Safe move/list across local, S3, Azure, GCS.
- **Extensibility** — Custom format handlers, write modes, and constraint validators.

## Documentation

- [Getting Started](docs/getting-started.md) — installation, requirements, full quick start
- [File Processing](docs/file-processing.md) — reading files, tracking, deduplication
- [Data Validation](docs/data-validation.md) — schema validation and CSVW constraints
- [Table Writing](docs/table-writing.md) — write modes, formats, schema evolution
- [File Management](docs/file-management.md) — safe move/list with cloud support
- [CSVW Metadata](docs/csvw-metadata.md) — metadata format and constraint reference
- [Extensibility](docs/extensibility.md) — custom handlers and validators
- [AutoLoader Integration](docs/autoloader-integration.md) — using SIFFT with Databricks AutoLoader

## Compatibility

- Python 3.10+
- PySpark 3.5.x, 4.0.x, or 4.1.x
- Databricks / Unity Catalog compatible

## Development

```bash
just test        # Run tests in Docker
just test-local  # Run tests locally (requires Java)
just --list      # All available commands
```

## Design Decisions

Architecture Decision Records are in [design_decisions/](design_decisions/).

## License

MIT — see [LICENSE](LICENSE).

## Contributors

- Iwan Dyke
- Fahad Khan
- Michal Poreba
