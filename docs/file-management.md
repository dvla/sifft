# File Management

## Functions

`safe_move(src_path, dst_path, storage_options=None, raise_on_error=False)`
- Move a file safely with validation checks
- Supports local and cloud storage (S3, Azure, GCS)
- `raise_on_error`: If True, raises FileManagementException on failure
- Returns: `FileOperationResult` with `success`, `source_path`, `destination_path`, `error`

`list_files_in_directory(directory, storage_options=None, raise_on_error=False)`
- List all files in a directory (non-recursive)
- Supports local and cloud storage (S3, Azure, GCS)
- `raise_on_error`: If True, raises FileManagementException on failure
- Returns: `FileOperationResult` with `success`, `files` list, `error`

## Supported Storage

- Local filesystem: `/path/to/file.csv`
- AWS S3: `s3://bucket/path/to/file.csv`
- Azure Blob: `az://container/path/to/file.csv`
- Google Cloud Storage: `gs://bucket/path/to/file.csv`

## Examples

```python
from file_management import safe_move, list_files_in_directory

# Move a file locally
result = safe_move("input/data.csv", "processed/data.csv")

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
```
