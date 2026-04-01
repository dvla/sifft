"""
Test utilities to eliminate duplication in BDD tests - Consolidated
"""

import os


def get_test_path(path: str) -> str:
    """Convert relative test_data path to tests/test_data path"""
    if path.startswith("test_data/"):
        return "tests/" + path
    return path


def ensure_test_directory(path: str) -> str:
    """Ensure test directory exists and return the path"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return path


def create_test_file(path: str, content: str) -> str:
    """Create a test file with given content"""
    ensure_test_directory(path)
    with open(path, "w") as f:
        f.write(content)
    return path


def create_simple_csv(path: str, name_age_data: bool = True) -> str:
    """Create a simple CSV file with standard test data"""
    if name_age_data:
        content = "name,age\nAlice,25\nBob,30\n"
    else:
        content = "id,value\n1,test_1\n2,test_2\n"
    return create_test_file(path, content)


def create_corrupted_csv(path: str) -> str:
    """Create a corrupted CSV file with unclosed quotes"""
    return create_test_file(path, 'invalid,csv,content\n"unclosed quote\n')


def create_empty_file(path: str) -> str:
    """Create an empty file"""
    return create_test_file(path, "")


def create_headers_only_csv(path: str) -> str:
    """Create CSV with only headers, no data"""
    return create_test_file(path, "name,age,city\n")


def create_inconsistent_columns_csv(path: str) -> str:
    """Create CSV with inconsistent column counts"""
    content = """col1,col2,col3
val1,val2,val3
val1,val2
val1,val2,val3,val4
"""
    return create_test_file(path, content)
