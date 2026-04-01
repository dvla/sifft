"""End-to-end pipeline demo - complete workflow from file to table."""

import pytest
from file_processing import process_file
from pyspark.sql import SparkSession
from table_writing import TableWriteOptions, write_table

from dataframe_validation import validate_csvw_constraints


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("PipelineDemo").master("local[1]").getOrCreate()


@pytest.fixture(autouse=True)
def cleanup_tables(spark):
    """Clean up demo tables before each test."""
    import shutil
    from pathlib import Path

    # Cleanup before test
    try:
        spark.sql("DROP TABLE IF EXISTS demo_pipeline_output")
    except Exception:
        pass

    # Also remove warehouse directory
    warehouse_path = Path("/app/spark-warehouse/demo_pipeline_output")
    if warehouse_path.exists():
        shutil.rmtree(warehouse_path)

    yield


def test_complete_pipeline(spark):
    """Demo: Complete pipeline - read, validate, write."""
    print("\n" + "=" * 70)
    print("🔗 END-TO-END PIPELINE DEMO")
    print("=" * 70)
    print("Pipeline: Read File → Validate Data → Write to Table → Verify")
    print("=" * 70)

    # Step 1: Read file
    print("\n📄 STEP 1: Reading CSV file with CSVW metadata...")
    print("-" * 70)
    result = process_file("samples/csvw_advanced.csv", spark)
    assert result.success

    print("✅ Successfully loaded file")
    print(f"   • Rows: {result.rows_processed}")
    print(f"   • Columns: {len(result.dataframe.columns)}")
    print(f"   • Metadata: {'Found' if result.metadata else 'Not found'}")

    print("\n📊 Input Data Preview:")
    result.dataframe.show(5, truncate=False)

    print("\n📐 Schema:")
    result.dataframe.printSchema()

    # Step 2: Validate
    print("\n🔍 STEP 2: Validating data against CSVW constraints...")
    print("-" * 70)

    if result.metadata:
        report = validate_csvw_constraints(result.dataframe, result.metadata)

        if report.valid:
            print("✅ ALL VALIDATION CHECKS PASSED!")
            print("   • All required fields present")
            print("   • All data types correct")
            print("   • All constraints satisfied")
        else:
            print(f"⚠️  Found {len(report.violations)} constraint violations:")
            for i, v in enumerate(report.violations[:5], 1):
                print(f"   {i}. {v.column}: {v.message} ({v.violating_rows} rows)")
            if len(report.violations) > 5:
                print(f"   ... and {len(report.violations) - 5} more")
    else:
        print("ℹ️  No metadata found - skipping validation")

    # Step 3: Write to table
    print("\n💾 STEP 3: Writing DataFrame to Spark table...")
    print("-" * 70)

    table_name = "demo_pipeline_output"
    options = TableWriteOptions(
        format="parquet", mode="overwrite", add_source_metadata=True
    )

    print(f"   • Target table: {table_name}")
    print(f"   • Format: {options.format}")
    print(f"   • Mode: {options.mode}")

    print("   • Source metadata: Enabled")

    write_result = write_table(
        result.dataframe,
        table_name,
        spark,
        options,
        source_file_path="samples/csvw_advanced.csv",
        file_size=1024,
    )

    assert write_result.success
    print("\n✅ Write completed successfully!")
    print(f"   • Rows written: {write_result.rows_written}")
    print(f"   • Duration: {write_result.duration_seconds:.2f} seconds")

    # Step 4: Verify
    print("\n📊 STEP 4: Verifying table contents...")
    print("-" * 70)

    written_df = spark.table(table_name)
    row_count = written_df.count()

    print("✅ Table verification complete")
    print(f"   • Table name: {table_name}")
    print(f"   • Rows in table: {row_count}")
    print(
        f"   • Match original: {'✅ Yes' if row_count == result.rows_processed else '❌ No'}"
    )
    print(
        "   • Metadata columns: dp_metadata_directory_path, dp_metadata_filename, dp_metadata_timestamp, dp_metadata_size"
    )

    print("\n📊 Table Contents (showing metadata columns):")
    written_df.select(
        "product_id",
        "name",
        "category",
        "dp_metadata_directory_path",
        "dp_metadata_filename",
        "dp_metadata_size",
    ).show(3, truncate=False)

    print("\n📐 Table Schema:")
    written_df.printSchema()

    # Summary
    print("\n" + "=" * 70)
    print("✅ PIPELINE EXECUTION COMPLETE")
    print("=" * 70)
    print("Summary:")
    print(f"  • Input: {result.rows_processed} rows from CSV")
    print(
        f"  • Validation: {'Passed' if result.metadata and report.valid else 'Skipped/Failed'}"
    )
    print(f"  • Output: {write_result.rows_written} rows to {table_name} table")
    print(f"  • Total time: {write_result.duration_seconds:.2f}s")
    print("=" * 70)
