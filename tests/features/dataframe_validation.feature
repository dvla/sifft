Feature: DataFrame Validation API - flat schema presence, casting, enforcement, and diagnostics

  Background:
    Given a target schema "TARGET_SCHEMA" with fields:
      | name        | type                 |
      | id          | IntegerType          |
      | name        | StringType           |
      | age         | IntegerType          |
      | salary      | DecimalType(18,2)    |
      | start_date  | DateType             |
      | updated_at  | TimestampType        |

  Scenario: Validate and cast a fully compliant flat DataFrame
    Given an incoming DataFrame "df_ok" with StringType columns:
      | id  | name   | age | salary   | start_date  | updated_at           |
      | 1   | Alice  | 30  | 1234.50  | 2025-01-01  | 2025-01-01 10:00:00  |
      | 2   | Bob    | 40  | 999.99   | 2024-12-31  | 2024-12-31 23:59:59  |
    When I validate and cast "df_ok" using TARGET_SCHEMA with options:
      | enforce_types         | true    |
      | allow_missing_as_null | false   |
      | reorder_columns       | true    |
    Then the result should have success = true
    And the casted DataFrame schema should match TARGET_SCHEMA exactly
    And the details.casting columns should have fail_count = 0 for:
      | column     |
      | id         |
      | name       |
      | age        |
      | salary     |
      | start_date |
      | updated_at |

  Scenario: Add missing columns as null to achieve schema compatibility
    Given an incoming DataFrame "df_missing" with StringType columns:
      | id  | name |
      | 10  | Carol|
      | 11  | Dave |
    When I validate and cast "df_missing" using TARGET_SCHEMA with options:
      | enforce_types         | true    |
      | allow_missing_as_null | true    |
      | reorder_columns       | true    |
    Then the result should have success = true
    And the casted DataFrame should include columns added as null:
      | column     |
      | age        |
      | salary     |
      | start_date |
      | updated_at |
    And the casted DataFrame schema should match TARGET_SCHEMA exactly

  Scenario: Fail when required columns are missing and allow_missing_as_null is false
    Given an incoming DataFrame "df_missing_required" with StringType columns:
      | id  | age |
      | 1   | 20  |
    When I validate and cast "df_missing_required" using TARGET_SCHEMA with options:
      | enforce_types         | true    |
      | allow_missing_as_null | false   |
      | reorder_columns       | true    |
    Then the result should have success = false
    And details.schema.missing_columns should contain:
      | column     |
      | name       |
      | salary     |
      | start_date |
      | updated_at |
    And no casted DataFrame should be returned

  Scenario: Ignore extra columns not in schema but report them in details
    Given an incoming DataFrame "df_extras" with StringType columns:
      | id | name | extra_col | another_extra |
      | 1  | Foo  | x         | y             |
      | 2  | Bar  | p         | q             |
    When I validate and cast "df_extras" using TARGET_SCHEMA with options:
      | enforce_types         | true    |
      | allow_missing_as_null | false   |
      | reorder_columns       | true    |
    Then the result should have success = true
    And details.schema.extra_columns should contain:
      | column        |
      | extra_col     |
      | another_extra |
    And the casted DataFrame should only include columns from TARGET_SCHEMA in order:
      | column     |
      | id         |
      | name       |
      | age        |
      | salary     |
      | start_date |
      | updated_at |

  Scenario: Casting invalid string values produces null and is counted in diagnostics
    Given a target schema "SIMPLE_SCHEMA" with fields:
      | name | type        |
      | id   | IntegerType |
      | age  | IntegerType |
    And an incoming DataFrame "df_bad_age" with StringType columns:
      | id | age |
      | 1  | abc |
      | 2  | 30  |
      | 3  |     |
    When I validate and cast "df_bad_age" using SIMPLE_SCHEMA with options:
      | enforce_types         | true    |
      | allow_missing_as_null | false   |
      | reorder_columns       | true    |
    Then the result should have success = true
    And details.casting.columns should include:
      | column | fail_count |
      | age    | 2          |
    And the casted DataFrame column "age" should contain values:
      | row | value |
      | 1   | null  |
      | 2   | 30    |
      | 3   | null  |

  Scenario: Column order enforcement for flat schema
    Given a target schema "ORDER_SCHEMA_FLAT" with fields in order:
      | name | type        |
      | id   | IntegerType |
      | name | StringType  |
      | age  | IntegerType |
    And an incoming DataFrame "df_out_of_order" with StringType columns in order:
      | name | age | id |
      | Bob  | 40  | 2  |
      | Eve  | 21  | 3  |
    When I validate and cast "df_out_of_order" using ORDER_SCHEMA_FLAT with options:
      | enforce_types         | true    |
      | allow_missing_as_null | false   |
      | reorder_columns       | true    |
    Then the result should have success = true
    And the casted DataFrame columns should be ordered as:
      | column |
      | id     |
      | name   |
      | age    |

  Scenario: Diagnostics include casting info, missing/extra columns, and enforcement flag
    Given an incoming DataFrame "df_diag" with StringType columns:
      | id | name  | age  | extra |
      | 1  | John  | 20   | x     |
      | 2  | Jane  | abc  | y     |
    When I validate and cast "df_diag" using TARGET_SCHEMA with options:
      | enforce_types         | true    |
      | allow_missing_as_null | false   |
      | reorder_columns       | true    |
    Then the result should have success = true
    And details.casting.columns should include:
      | column | fail_count |
      | age    | 1          |
    And details.schema.extra_columns should contain:
      | column |
      | extra  |
    And details.schema_enforced should be true
