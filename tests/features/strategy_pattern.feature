Feature: Strategy Pattern Architecture
  As a developer
  I want to use pluggable strategies for file processing and DataFrame creation
  So that I can extend the framework with new file types and DataFrame implementations

  Scenario: File processor registry selects correct strategy for CSV
    Given I have a CSV file at "test_data/sample.csv"
    When I process the file using the default registry
    Then I should get a successful result
    And the delimited file strategy should be used

  Scenario: File processor registry selects correct strategy for TXT
    Given I have a TXT file at "test_data/sample.txt"
    When I process the file using the default registry
    Then I should get a successful result
    And the delimited file strategy should be used

  Scenario: File processor registry selects correct strategy for DAT
    Given I have a DAT file at "test_data/sample.dat"
    When I process the file using the default registry
    Then I should get a successful result
    And the delimited file strategy should be used

  Scenario: Unsupported file type with no strategy
    Given I have a binary file at "test_data/image.png"
    When I process the file using the default registry
    Then I should get a failed result
    And the error should indicate no strategy found

  Scenario: PySpark strategy dependency injection
    Given I have a PySpark strategy with custom Spark session
    And I have a CSV file at "test_data/sample.csv"
    When I process the file with the injected PySpark strategy
    Then I should get a successful result
    And the DataFrame should be a real PySpark DataFrame

  Scenario: Backward compatibility with spark parameter
    Given I have a Spark session
    And I have a CSV file at "test_data/sample.csv"
    When I process the file using the deprecated spark parameter
    Then I should get a successful result
    And the processing should work as before

  Scenario: Registry modification at runtime
    Given I have a default file processor registry
    When I register a new delimited file strategy for LOG files
    And I process a LOG file at "test_data/sample.log"
    Then the new strategy should handle the LOG file
    And I should get a successful result

  Scenario: Strategy error handling in file processing
    Given I have a corrupted CSV file at "test_data/bad.csv"
    When I process the file using the default registry
    Then I should get a failed result
    And the error should be properly handled by the strategy

  Scenario: Default strategy handles files without extensions
    Given I have a delimited file without extension at "test_data/datafile"
    When I process the file using the default registry
    Then I should get a successful result
    And the delimited file strategy should be used

  Scenario: Strategy processes different delimiters correctly
    Given I have a pipe-delimited file at "test_data/pipe.txt"
    And I have a comma-delimited file at "test_data/comma.csv"
    When I process both files using the default registry
    Then both files should be processed successfully
    And each should use the appropriate delimiter detection
