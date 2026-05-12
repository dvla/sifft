# Extensibility

SIFFT supports custom handlers and validators across all modules.

## File Processing — Custom Format Handlers

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

## Table Writing — Custom Mode and Format Handlers

```python
from table_writing import register_mode_handler, TableWriteResult

def incremental_mode(df, table_name, spark, options, raise_on_error):
    """Custom incremental load logic."""
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

## DataFrame Validation — Custom Constraint Validators

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
