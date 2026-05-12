# Data Validation

## Functions

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

## Supported Constraints

**Column-level:**
- `required` - No null values
- `unique` - No duplicates
- `minimum` / `maximum` - Numeric bounds
- `minLength` / `maxLength` - String length
- `pattern` - Regex validation
- `enum` - Allowed values

**Table-level:**
- `primaryKey` - Composite uniqueness

## Example

```python
from dataframe_validation import validate_csvw_constraints, format_violations

report = validate_csvw_constraints(df, metadata)

if not report.valid:
    print(format_violations(report))
```
