Feature: Extended File Type Support
  As a data engineer
  I want to process Excel files, EOT files, and handle case variations
  So that I can handle all file types comprehensively

  Scenario: Process XLSX file successfully
    Given I have an XLSX file at "test_data/sample.xlsx"
    When I process the file using Excel strategy
    Then I should get a successful result
    And the result should contain a DataFrame
    And the DataFrame should have data from the Excel sheet

  Scenario: Process XLS file successfully
    Given I have an XLS file at "test_data/sample.xls"
    When I process the file using Excel strategy
    Then I should get a successful result
    And the result should contain a DataFrame
    And the DataFrame should have data from the Excel sheet

  Scenario: Process EOT file successfully
    Given I have an EOT file at "test_data/sample.EOT"
    When I process the file using the default registry
    Then I should get a successful result
    And the result should contain a DataFrame

  Scenario: Handle case-insensitive file extensions
    Given I have an OUT file with uppercase extension at "test_data/sample.OUT"
    When I process the file using the default registry
    Then I should get a successful result
    And the delimited file strategy should handle the case variation

  Scenario: Handle DAT file case variations
    Given I have a DAT file with uppercase extension at "test_data/sample.DAT"
    When I process the file using the default registry
    Then I should get a successful result
    And the delimited file strategy should handle the case variation

  Scenario: Process timestamp-based filename
    Given I have a file with timestamp pattern at "test_data/EXPORT_DATA_20251214_172949.dat"
    When I process the file using the default registry
    Then I should get a successful result
    And the filename pattern should not affect processing

  Scenario: Process files without extensions
    Given I have a file without extension at "test_data/extensionless_datafile"
    When I process the file using the enhanced registry
    Then I should get a successful result
    And the file should be processed as delimited data

  Scenario: Registry selects Excel strategy for XLSX files
    Given I have an XLSX file at "test_data/sample.xlsx"
    When I process the file using the enhanced registry
    Then the Excel strategy should be selected automatically
    And I should get a successful result
