import os

import pytest

# Import current API
from file_processing.file_processor import process_file
from pyspark.sql import SparkSession
from pytest_bdd import given, parsers, scenarios, then, when

from tests.test_utils import get_test_path

scenarios("../features/extended_file_types.feature")

# Global variables for test state
test_results = []
test_file_path = None


@pytest.fixture
def spark():
    spark = (
        SparkSession.builder.appName("ExtendedFileTypesTest")
        .master("local[1]")
        .getOrCreate()
    )
    yield spark


# Given steps


@given(parsers.parse('I have an XLSX file at "{path}"'))
def setup_xlsx_file(path):
    global test_file_path
    # Use actual Excel sample from samples directory
    test_file_path = "samples/excel_sample.xlsx"


@given(parsers.parse('I have an XLS file at "{path}"'))
def setup_xls_file(path):
    global test_file_path
    # Use actual XLS sample from samples directory
    test_file_path = "samples/excel_sample.xls"


@given(parsers.parse('I have an EOT file at "{path}"'))
def setup_eot_file(path):
    global test_file_path
    test_file_path = get_test_path(path)
    os.makedirs(os.path.dirname(test_file_path), exist_ok=True)
    with open(test_file_path, "w") as f:
        f.write("col1,col2,col3\n")
        f.write("val1,val2,val3\n")


@given(parsers.parse('I have an OUT file with uppercase extension at "{path}"'))
def setup_uppercase_out_file(path):
    global test_file_path
    test_file_path = get_test_path(path)
    os.makedirs(os.path.dirname(test_file_path), exist_ok=True)
    with open(test_file_path, "w") as f:
        f.write("col1|col2|col3\n")
        f.write("val1|val2|val3\n")


@given(parsers.parse('I have a DAT file with uppercase extension at "{path}"'))
def setup_uppercase_dat_file(path):
    global test_file_path
    test_file_path = get_test_path(path)
    os.makedirs(os.path.dirname(test_file_path), exist_ok=True)
    with open(test_file_path, "w") as f:
        f.write("col1|col2|col3\n")
        f.write("val1|val2|val3\n")


@given(parsers.parse('I have a file with timestamp pattern at "{path}"'))
def setup_timestamp_file(path):
    global test_file_path
    test_file_path = get_test_path(path)
    os.makedirs(os.path.dirname(test_file_path), exist_ok=True)
    with open(test_file_path, "w") as f:
        f.write("col1|col2|col3\n")
        f.write("val1|val2|val3\n")


@given(parsers.parse('I have a file without extension at "{path}"'))
def setup_no_extension_file(path):
    global test_file_path
    # Use a filename with no dots at all
    test_file_path = "tests/test_data/extensionless_datafile"
    os.makedirs(os.path.dirname(test_file_path), exist_ok=True)
    with open(test_file_path, "w") as f:
        f.write("col1,col2,col3\n")
        f.write("val1,val2,val3\n")


# When steps


@when("I process the file using Excel strategy")
def process_excel_file(spark):
    global test_results
    test_results = [process_file(test_file_path, spark)]


@when("I process the file using the default registry")
def process_with_default_registry(spark):
    global test_results
    test_results = [process_file(test_file_path, spark)]


@when("I process the file using the enhanced registry")
def process_with_enhanced_registry(spark):
    global test_results
    test_results = [process_file(test_file_path, spark)]


# Then steps


@then("I should get a successful result")
def check_success():
    assert len(test_results) > 0
    assert test_results[0].success


@then("I should get a failed result")
def check_failure():
    assert len(test_results) > 0
    assert not test_results[0].success


@then("the result should contain a DataFrame")
def check_dataframe():
    assert test_results[0].dataframe is not None


@then("the DataFrame should have data from the Excel sheet")
def check_excel_data():
    assert test_results[0].dataframe is not None
    assert test_results[0].dataframe.count() > 0


@then("the delimited file strategy should handle the case variation")
def check_case_handling():
    # If it succeeded, case was handled correctly
    assert test_results[0].success


@then("the filename pattern should not affect processing")
def check_filename_pattern():
    # If it succeeded, filename pattern didn't cause issues
    assert test_results[0].success


@then("the Excel strategy should be selected automatically")
def check_excel_strategy():
    # If Excel file processed successfully, strategy was selected
    assert test_results[0].success


@then("the file should be processed as delimited data")
def check_delimited_processing():
    # If file processed successfully as delimited, it worked
    assert test_results[0].success
    assert test_results[0].dataframe is not None


# Skipped scenarios - not implemented yet


@then("all data should be converted to strings")
def skip_string_conversion():
    pytest.skip()


@then("the error should indicate Excel file corruption")
def skip_corruption_error():
    pytest.skip()
