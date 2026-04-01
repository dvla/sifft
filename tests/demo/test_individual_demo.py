"""Individual feature demos - showcases each module independently."""

import pytest
from file_processing import process_file
from pyspark.sql import SparkSession

from dataframe_validation import validate_csvw_constraints


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("Demo").master("local[1]").getOrCreate()


def test_csv_processing(spark):
    """Demo: Process CSV file."""
    print("\n" + "=" * 70)
    print("📄 DEMO: CSV File Processing")
    print("=" * 70)

    result = process_file("samples/data_comma.txt", spark)
    assert result.success

    print(
        f"\n✅ Loaded {result.rows_processed} rows, {len(result.dataframe.columns)} columns"
    )
    print(f"📋 Columns: {result.dataframe.columns}")
    print("\n📊 Sample Data:")
    result.dataframe.show(5, truncate=False)
    print("\n📐 Schema:")
    result.dataframe.printSchema()


def test_pipe_delimited(spark):
    """Demo: Process pipe-delimited file."""
    print("\n" + "=" * 70)
    print("📄 DEMO: Pipe-Delimited File")
    print("=" * 70)

    result = process_file("samples/data_pipe.dat", spark)
    assert result.success

    print(f"\n✅ Loaded {result.rows_processed} rows")
    print("\n📊 Data Preview:")
    result.dataframe.show(3, truncate=False)


def test_excel_xlsx(spark):
    """Demo: Process Excel XLSX file."""
    print("\n" + "=" * 70)
    print("📄 DEMO: Excel XLSX File")
    print("=" * 70)

    result = process_file("samples/excel_sample.xlsx", spark)
    assert result.success

    print(f"\n✅ Loaded {result.rows_processed} rows from Excel")
    print("\n📊 Excel Data:")
    result.dataframe.show(5, truncate=False)


def test_no_header_detection(spark):
    """Demo: File without header using explicit option."""
    print("\n" + "=" * 70)
    print("📄 DEMO: File Without Header")
    print("=" * 70)

    result = process_file("samples/data_no_header.csv", spark, {"header": "false"})
    assert result.success

    print(f"\n✅ Loaded {result.rows_processed} rows (no header)")
    print(f"📋 Auto-generated columns: {result.dataframe.columns}")
    print("\n📊 Data Preview:")
    result.dataframe.show(truncate=False)


def test_csvw_with_metadata(spark):
    """Demo: Process CSVW file with metadata."""
    print("\n" + "=" * 70)
    print("📄 DEMO: CSVW with Metadata")
    print("=" * 70)

    result = process_file("samples/csvw_simple.csv", spark)
    assert result.success
    assert result.metadata is not None

    print(f"\n✅ Loaded {result.rows_processed} rows with CSVW metadata")
    print(f"📋 Metadata found: {result.metadata.get('dc:title', 'N/A')}")
    print("\n📊 Data with Schema:")
    result.dataframe.show(truncate=False)
    result.dataframe.printSchema()


def test_csvw_validation_pass(spark):
    """Demo: CSVW validation - all constraints pass."""
    print("\n" + "=" * 70)
    print("🔍 DEMO: CSVW Validation - Clean Data")
    print("=" * 70)

    result = process_file("samples/csvw_validation_pass.csv", spark)
    assert result.success

    print(f"\n📊 Input Data ({result.rows_processed} rows):")
    result.dataframe.show(truncate=False)

    if result.metadata:
        print("\n🔎 Running validation...")
        report = validate_csvw_constraints(result.dataframe, result.metadata)

        if report.valid:
            print("✅ ALL CONSTRAINTS PASSED!")
            print("   • No null values in required fields")
            print("   • All unique constraints satisfied")
            print("   • All data types correct")
        else:
            print(f"❌ Found {len(report.violations)} violations")


def test_csvw_validation_fail(spark):
    """Demo: CSVW validation - constraint violations detected."""
    print("\n" + "=" * 70)
    print("🔍 DEMO: CSVW Validation - Bad Data (Expected Failures)")
    print("=" * 70)

    result = process_file("samples/csvw_validation_fail.csv", spark)
    assert result.success

    print(f"\n📊 Input Data ({result.rows_processed} rows):")
    result.dataframe.show(truncate=False)

    if result.metadata:
        print("\n🔎 Running validation...")
        report = validate_csvw_constraints(result.dataframe, result.metadata)

        if not report.valid:
            print(f"\n⚠️  DETECTED {len(report.violations)} CONSTRAINT VIOLATIONS:")
            for i, v in enumerate(report.violations, 1):
                print(f"\n   {i}. Column: {v.column}")
                print(f"      Constraint: {v.constraint_type}")
                print(f"      Issue: {v.message}")
                print(f"      Affected rows: {v.violating_rows}")

            print(f"\n📊 Total violating rows: {report.total_violations}")


def test_custom_handler_extensibility(spark):
    """Demo: Register custom handler for special file format."""
    print("\n" + "=" * 70)
    print("🔧 DEMO: Custom Handler - Extensibility")
    print("=" * 70)

    from file_processing import (
        FileProcessingResult,
        register_handler,
        unregister_handler,
    )

    def custom_format_handler(file_path, spark, options):
        print(f"   🎯 Custom handler processing: {file_path}")

        data = [
            ("Custom", "Format", 100),
            ("Special", "Processing", 200),
            ("Extended", "Functionality", 300),
        ]

        df = spark.createDataFrame(data, ["Type", "Feature", "Value"])

        return FileProcessingResult(
            success=True,
            file=file_path,
            dataframe=df,
            message="Processed with custom handler",
            rows_processed=len(data),
        )

    print("\n📝 Registering custom handler for .custom extension...")
    register_handler(".custom", custom_format_handler, priority=10)

    print("✅ Handler registered!")
    print("\n📄 Processing file with custom extension...")

    import tempfile
    from pathlib import Path

    with tempfile.NamedTemporaryFile(suffix=".custom", delete=False) as f:
        f.write(b"dummy content")
        temp_file = f.name

    try:
        result = process_file(temp_file, spark)
        assert result.success

        print(f"\n✅ {result.message}")
        print(f"📊 Processed {result.rows_processed} rows")
        print("\n📋 Custom Data:")
        result.dataframe.show(truncate=False)

        print("\n💡 Use Cases:")
        print("   • Files with summary rows or footers")
        print("   • Non-standard delimiters or formats")
        print("   • Compressed files (.csv.gz)")
        print("   • New file types (.json, .xml, .parquet)")
        print("   • Pre/post processing hooks")

    finally:
        Path(temp_file).unlink()
        unregister_handler(".custom")
        print("\n🧹 Cleaned up: Handler unregistered")


def test_checksum_duplicate_detection(spark):
    """Demo: Skip already-processed files using checksum tracking."""
    print("\n" + "=" * 70)
    print("🔁 DEMO: Duplicate Detection with Checksums")
    print("=" * 70)

    import shutil
    import tempfile
    from pathlib import Path

    from file_processing import clear_marker, compute_file_checksum, is_file_processed

    with tempfile.TemporaryDirectory() as tmpdir:
        src = "samples/csvw_simple.csv"
        dst = Path(tmpdir) / "data.csv"
        shutil.copy(src, dst)
        shutil.copy(f"{src}-metadata.json", f"{dst}-metadata.json")

        print(f"\n📄 Test file: {dst.name}")
        checksum = compute_file_checksum(str(dst))
        print(f"🔑 Checksum (MD5): {checksum}")

        print("\n--- First Run ---")
        result1 = process_file(str(dst), spark, track_processed=True)
        print(f"✅ Processed: {result1.rows_processed} rows")
        print(f"📝 Message: {result1.message}")

        print("\n--- Second Run (same file) ---")
        result2 = process_file(str(dst), spark, track_processed=True)
        print(f"⏭️  Skipped: {result2.rows_processed} rows")
        print(f"📝 Message: {result2.message}")

        print("\n--- Check Status ---")
        print(f"🔍 is_file_processed: {is_file_processed(str(dst))}")

        print("\n--- Modify File ---")
        with open(dst, "a") as f:
            f.write("4,New Row,new@example.com\n")
        new_checksum = compute_file_checksum(str(dst))
        print(f"🔑 New checksum: {new_checksum}")
        print(f"🔍 is_file_processed: {is_file_processed(str(dst))}")

        print("\n--- Third Run (modified file) ---")
        result3 = process_file(str(dst), spark, track_processed=True)
        print(f"✅ Processed: {result3.rows_processed} rows (file changed)")

        print("\n--- Clear Marker (force reprocess) ---")
        clear_marker(str(dst))
        print(f"🔍 is_file_processed: {is_file_processed(str(dst))}")

        print("\n💡 Use Cases:")
        print("   • Scheduled jobs that shouldn't reprocess same files")
        print("   • Landing zone processing with idempotency")
        print("   • Cloud storage (S3/Azure) with storage_options")


def test_bucketed_dedup_across_paths(spark):
    """Demo: Content-based deduplication across date-partitioned paths."""
    print("\n" + "=" * 70)
    print("🗂️  DEMO: Bucketed Dedup Across Paths")
    print("=" * 70)

    import os
    import shutil
    import tempfile

    from file_processing import compute_file_checksum

    with tempfile.TemporaryDirectory() as tmpdir:
        marker_dir = os.path.join(tmpdir, "markers")
        day1_dir = os.path.join(tmpdir, "landing", "2026", "03", "01")
        day2_dir = os.path.join(tmpdir, "landing", "2026", "03", "02")
        os.makedirs(day1_dir)
        os.makedirs(day2_dir)

        src = "samples/csvw_simple.csv"
        file1 = os.path.join(day1_dir, "sales.csv")
        file2 = os.path.join(day2_dir, "sales.csv")

        shutil.copy(src, file1)
        shutil.copy(src, file2)
        shutil.copy(f"{src}-metadata.json", f"{file1}-metadata.json")
        shutil.copy(f"{src}-metadata.json", f"{file2}-metadata.json")

        checksum = compute_file_checksum(file1)

        print("\n📁 Simulating date-partitioned landing zone:")
        print("   Day 1: landing/2026/03/01/sales.csv")
        print("   Day 2: landing/2026/03/02/sales.csv (same content)")
        print(f"\n🔑 Both files have checksum: {checksum[:16]}...")

        print("\n--- Without marker_dir (path-based) ---")
        result1 = process_file(file1, spark, track_processed=True)
        print(f"   File 1: Processed {result1.rows_processed} rows")

        # Reset for demo
        from file_processing import clear_marker

        clear_marker(file1)

        print("\n--- With marker_dir (content-based) ---")
        print(f"   Marker directory: {marker_dir}")

        result1 = process_file(
            file1, spark, track_processed=True, marker_dir=marker_dir
        )
        print(f"   Day 1 file: Processed {result1.rows_processed} rows")

        result2 = process_file(
            file2, spark, track_processed=True, marker_dir=marker_dir
        )
        print(f"   Day 2 file: Skipped ({result2.rows_processed} rows) - duplicate!")

        print("\n📂 Marker structure (git-style bucketing):")
        for root, _dirs, files in os.walk(marker_dir):
            level = root.replace(marker_dir, "").count(os.sep)
            indent = "   " * (level + 1)
            print(f"{indent}{os.path.basename(root)}/")
            for file in files:
                print(f"{indent}   {file}")

        print("\n💡 Benefits:")
        print("   • Same file arriving on different days = processed once")
        print("   • Scales to millions of files (65k buckets)")
        print("   • Audit trail: each unique version has its own marker")
        print("   • GDPR-friendly: never deletes, just tracks what was processed")
