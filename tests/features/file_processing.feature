Feature: File Processing API
  As a data engineer
  I want to process files from various storage sources
  So that I can get standardized DataFrames for Databricks

  Scenario: Process a single CSV file successfully
    Given I have a CSV file at "test_data/sample.csv"
    When I process the file
    Then I should get a successful result
    And the result should contain a DataFrame
    And the DataFrame should have the expected columns

  Scenario: Process multiple files with pattern filter
    Given I have multiple CSV files in "test_data/"
    When I process files with pattern "*.csv" and max_files 2
    Then I should get 2 successful results
    And each result should contain a DataFrame

  Scenario: Handle corrupted file gracefully
    Given I have a corrupted file at "test_data/bad.csv"
    When I process the file
    Then I should get a failed result
    And the error message should indicate the file is corrupted

  Scenario: Process files from different storage types
    Given I have files at various paths
    When I process files from "s3://bucket/file.csv"
    And I process files from "/local/path/file.csv"
    Then both should be processed using the same interface

  Scenario: Process a TXT file with comma delimiter
    Given I have a comma-delimited TXT file at "test_data/sample.txt"
    When I process the file
    Then I should get a successful result
    And the result should contain a DataFrame

  Scenario: Validate pipe delimiter parsing
    Given I have a pipe-delimited file with 3 columns at "test_data/pipe_test.txt"
    When I process the file
    Then I should get a successful result
    And the DataFrame should have 3 columns

  Scenario: Process an OUT file with comma delimiter
    Given I have a comma-delimited OUT file at "test_data/sample.out"
    When I process the file
    Then I should get a successful result
    And the result should contain a DataFrame

  Scenario: Validate comma delimiter parsing
    Given I have a comma-delimited file with 5 columns at "test_data/comma_test.csv"
    When I process the file
    Then I should get a successful result
    And the DataFrame should have 5 columns

  Scenario: Validate tab delimiter parsing
    Given I have a tab-delimited file with 4 columns at "test_data/tab_test.txt"
    When I process the file
    Then I should get a successful result
    And the DataFrame should have 4 columns

  Scenario: Process a DAT file
    Given I have a DAT file at "test_data/sample.dat"
    When I process the file
    Then I should get a successful result
    And the result should contain a DataFrame

  Scenario: Process a file without extension
    Given I have a file without extension at "test_data/datafile"
    When I process the file
    Then I should get a successful result
    And the result should contain a DataFrame

  Scenario: Process an empty file
    Given I have an empty file at "test_data/empty.csv"
    When I process the file
    Then I should get a failed result
    And the error message should indicate the file is empty

  Scenario: Process a file with only headers
    Given I have a CSV file with only headers at "test_data/headers_only.csv"
    When I process the file
    Then I should get a failed result
    And the DataFrame should have zero rows

  Scenario: Handle file with inconsistent column counts
    Given I have a file with inconsistent columns at "test_data/inconsistent.csv"
    When I process the file
    Then I should get a successful result
    And the DataFrame should have padded columns

  Scenario: Filter by specific extension
    Given I have multiple files with different extensions in "test_data/"
    When I process files with pattern "*.txt" and max_files 10
    Then I should only get TXT files

  Scenario: No files match pattern
    Given I have CSV files in "test_data/"
    When I process files with pattern "*.json" and max_files 10
    Then I should get zero results
