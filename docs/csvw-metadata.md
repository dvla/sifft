# CSVW Metadata

[CSVW](https://www.w3.org/TR/tabular-data-primer/) (CSV on the Web) metadata allows you to define schemas and validation constraints for your CSV files. Place a metadata file alongside your CSV with the naming pattern `{filename}-metadata.json`.

## Example

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

## Usage

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

You can also specify a custom metadata path:

```python
result = process_file("data.csv", spark, metadata_path="/path/to/metadata.json")
```

## Supported Constraints

- `required` - No null values
- `unique` - No duplicates
- `minimum` / `maximum` - Numeric bounds
- `minLength` / `maxLength` - String length
- `pattern` - Regex validation
- `enum` - Allowed values
- `primaryKey` - Composite uniqueness
