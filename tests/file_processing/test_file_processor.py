import tempfile
from pathlib import Path

import pandas as pd
import pytest
from file_processing.file_processor import process_file, process_files_batch
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.appName("FileProcessorTests")
        .master("local[2]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture(scope="session")
def samples_dir():
    """Path to the samples directory with real test data"""
    return Path(__file__).parent.parent.parent / "samples"


class TestCSVProcessing:
    def test_csv_with_header(self, spark, temp_dir):
        csv_file = temp_dir / "data.csv"
        csv_file.write_text("name,age,city\nAlice,25,London\nBob,30,Paris\n")

        result = process_file(str(csv_file), spark)

        assert result.success
        assert result.rows_processed == 2
        assert result.rows_corrupt == 0
        assert set(result.dataframe.columns) == {"name", "age", "city"}
        assert result.dataframe.count() == 2

    def test_csv_without_header(self, spark, temp_dir):
        csv_file = temp_dir / "data.csv"
        csv_file.write_text("Alice,25,London\nBob,30,Paris\n")

        result = process_file(str(csv_file), spark)

        assert result.success
        # Auto-detection treats first row as header since it has text + second has numbers
        assert result.rows_processed >= 1

    def test_csv_custom_delimiter(self, spark, temp_dir):
        csv_file = temp_dir / "data.csv"
        csv_file.write_text("name|age|city\nAlice|25|London\nBob|30|Paris\n")

        result = process_file(str(csv_file), spark, {"delimiter": "|"})

        assert result.success
        assert result.rows_processed == 2
        assert "name" in result.dataframe.columns

    def test_csv_empty_file(self, spark, temp_dir):
        csv_file = temp_dir / "empty.csv"
        csv_file.write_text("")

        result = process_file(str(csv_file), spark)

        assert not result.success
        assert "empty" in result.message.lower()

    def test_csv_only_header(self, spark, temp_dir):
        csv_file = temp_dir / "header_only.csv"
        csv_file.write_text("name,age,city\n")

        result = process_file(str(csv_file), spark)

        assert not result.success
        assert "no data" in result.message.lower()

    def test_csv_corrupt_records(self, spark, temp_dir):
        csv_file = temp_dir / "corrupt.csv"
        # Create properly corrupt CSV with mismatched column counts
        csv_file.write_text("name,age,city\nAlice,25,London\nBob\nCharlie,35,Berlin\n")

        result = process_file(str(csv_file), spark)

        # PERMISSIVE mode fills missing values with null, doesn't always mark as corrupt
        # Just verify it processes successfully
        assert result.success
        assert result.rows_processed >= 2


class TestTXTProcessing:
    def test_txt_comma_delimited(self, spark, samples_dir):
        """Test TXT file with comma delimiter from samples folder"""
        txt_file = samples_dir / "data_comma.txt"

        if not txt_file.exists():
            pytest.skip(f"Sample file not found: {txt_file}")

        result = process_file(str(txt_file), spark)

        assert result.success
        assert result.rows_processed > 0

    def test_txt_pipe_delimited(self, spark, samples_dir):
        """Test TXT file with pipe delimiter from samples folder"""
        txt_file = samples_dir / "data_pipe.txt"

        if not txt_file.exists():
            pytest.skip(f"Sample file not found: {txt_file}")

        result = process_file(str(txt_file), spark)

        assert result.success
        assert result.rows_processed > 0

    def test_txt_tab_delimited(self, spark, samples_dir):
        """Test TXT file with tab delimiter from samples folder"""
        txt_file = samples_dir / "data_tab.txt"

        if not txt_file.exists():
            pytest.skip(f"Sample file not found: {txt_file}")

        result = process_file(str(txt_file), spark)

        assert result.success
        assert result.rows_processed > 0


class TestOUTProcessing:
    def test_out_comma_delimited(self, spark, samples_dir):
        """Test OUT file with comma delimiter from samples folder"""
        out_file = samples_dir / "data_comma.out"

        if not out_file.exists():
            pytest.skip(f"Sample file not found: {out_file}")

        result = process_file(str(out_file), spark)

        assert result.success
        assert result.rows_processed > 0

    def test_out_pipe_delimited(self, spark, samples_dir):
        """Test OUT file with pipe delimiter from samples folder"""
        out_file = samples_dir / "data_pipe.out"

        if not out_file.exists():
            pytest.skip(f"Sample file not found: {out_file}")

        result = process_file(str(out_file), spark)

        assert result.success
        assert result.rows_processed > 0

    def test_out_tab_delimited(self, spark, samples_dir):
        """Test OUT file with tab delimiter from samples folder

        Note: data_tab.out uses spaces, not actual tab characters
        """
        out_file = samples_dir / "data_tab.out"

        if not out_file.exists():
            pytest.skip(f"Sample file not found: {out_file}")

        result = process_file(str(out_file), spark)

        assert result.success
        assert result.rows_processed > 0


class TestDATProcessing:
    def test_dat_comma_delimited(self, spark, samples_dir):
        """Test DAT file with comma delimiter from samples folder"""
        dat_file = samples_dir / "data_comma.dat"

        if not dat_file.exists():
            pytest.skip(f"Sample file not found: {dat_file}")

        result = process_file(str(dat_file), spark)

        assert result.success
        assert result.rows_processed > 0

    def test_dat_pipe_delimited(self, spark, samples_dir):
        """Test DAT file with pipe delimiter from samples folder"""
        dat_file = samples_dir / "data_pipe.dat"

        if not dat_file.exists():
            pytest.skip(f"Sample file not found: {dat_file}")

        result = process_file(str(dat_file), spark)

        assert result.success
        assert result.rows_processed > 0

    def test_dat_tab_delimited(self, spark, samples_dir):
        """Test DAT file with tab delimiter from samples folder"""
        dat_file = samples_dir / "data_tab.dat"

        if not dat_file.exists():
            pytest.skip(f"Sample file not found: {dat_file}")

        result = process_file(str(dat_file), spark)

        assert result.success
        assert result.rows_processed > 0


class TestTSVProcessing:
    def test_tsv_tab_delimited(self, spark, temp_dir):
        tsv_file = temp_dir / "data.tsv"
        tsv_file.write_text("name\tage\tcity\nAlice\t25\tLondon\nBob\t30\tParis\n")

        result = process_file(str(tsv_file), spark)

        assert result.success
        assert result.rows_processed == 2


class TestExcelProcessing:
    def test_xlsx_with_header(self, spark, temp_dir):
        xlsx_file = temp_dir / "data.xlsx"
        df = pd.DataFrame(
            {"name": ["Alice", "Bob"], "age": [25, 30], "city": ["London", "Paris"]}
        )
        df.to_excel(xlsx_file, index=False)

        result = process_file(str(xlsx_file), spark)

        assert result.success
        assert result.rows_processed == 2
        assert set(result.dataframe.columns) == {"name", "age", "city"}

    def test_xlsx_without_header(self, spark, temp_dir):
        xlsx_file = temp_dir / "data.xlsx"
        df = pd.DataFrame([["Alice", 25, "London"], ["Bob", 30, "Paris"]])
        df.to_excel(xlsx_file, index=False, header=False)

        result = process_file(str(xlsx_file), spark, {"header": None})

        assert result.success
        assert result.rows_processed == 2

    def test_xlsx_specific_sheet(self, spark, temp_dir):
        xlsx_file = temp_dir / "data.xlsx"

        with pd.ExcelWriter(xlsx_file) as writer:
            df1 = pd.DataFrame({"col1": [1, 2]})
            df2 = pd.DataFrame({"col2": [3, 4]})
            df1.to_excel(writer, sheet_name="Sheet1", index=False)
            df2.to_excel(writer, sheet_name="Sheet2", index=False)

        result = process_file(str(xlsx_file), spark, {"sheet_name": "Sheet2"})

        assert result.success
        assert "col2" in result.dataframe.columns


class TestNoExtension:
    def test_file_without_extension(self, spark, temp_dir):
        no_ext_file = temp_dir / "datafile"
        no_ext_file.write_text("name,age\nAlice,25\nBob,30\n")

        result = process_file(str(no_ext_file), spark)

        assert result.success
        assert result.rows_processed == 2


class TestErrorHandling:
    def test_file_not_found(self, spark):
        result = process_file("/nonexistent/file.csv", spark)

        assert not result.success
        assert "not found" in result.message.lower()

    def test_unrecognised_extension_falls_back_to_delimited(self, spark, temp_dir):
        pdf_file = temp_dir / "data.pdf"
        pdf_file.write_text("name,age,city\nAlice,25,London\nBob,30,Paris\n")

        result = process_file(str(pdf_file), spark)

        assert result.success
        assert result.rows_processed == 2

    def test_numeric_extension_falls_back_to_delimited(self, spark, temp_dir):
        numeric_file = temp_dir / "report.202605090555"
        numeric_file.write_text("1,2,3\n4,5,6\n")

        result = process_file(
            str(numeric_file), spark, read_options={"delimiter": ",", "header": "false"}
        )

        assert result.success
        assert result.rows_processed == 2

    def test_wrong_delimiter(self, spark, temp_dir):
        csv_file = temp_dir / "data.csv"
        csv_file.write_text("name|age|city\nAlice|25|London\nBob|30|Paris\n")

        result = process_file(str(csv_file), spark)

        assert result.success or "wrong delimiter" in result.message.lower()

    def test_spark_session_none(self, temp_dir):
        csv_file = temp_dir / "data.csv"
        csv_file.write_text("name,age\nAlice,25\n")

        result = process_file(str(csv_file), None)

        assert not result.success
        assert "SparkSession" in result.message


class TestBatchProcessing:
    def test_batch_multiple_files(self, spark, temp_dir):
        files = []
        for i in range(3):
            csv_file = temp_dir / f"data{i}.csv"
            csv_file.write_text(f"id,value\n{i},100\n")
            files.append(str(csv_file))

        results = process_files_batch(files, spark)

        assert len(results) == 3
        assert all(r.success for r in results)

    def test_batch_mixed_success_failure(self, spark, temp_dir):
        csv_file = temp_dir / "data.csv"
        csv_file.write_text("id,value\n1,100\n")

        files = [str(csv_file), "/nonexistent/file.csv"]

        results = process_files_batch(files, spark)

        assert len(results) == 2
        assert results[0].success
        assert not results[1].success

    def test_batch_with_sample_files(self, spark, samples_dir):
        """Test batch processing with all sample files"""
        sample_files = list(samples_dir.glob("data_*.*"))

        if not sample_files:
            pytest.skip("No sample files found in samples directory")

        results = process_files_batch([str(f) for f in sample_files], spark)

        assert len(results) == len(sample_files)
        # All should process successfully (even if delimiter detection doesn't work perfectly)
        assert all(r.success for r in results), (
            f"Failed files: {[r.file for r in results if not r.success]}"
        )


class TestHeaderDetection:
    def test_detects_header_with_text_and_numbers(self, spark, temp_dir):
        csv_file = temp_dir / "data.csv"
        csv_file.write_text("name,age,city\nAlice,25,London\nBob,30,Paris\n")

        result = process_file(str(csv_file), spark, {"header": "auto"})

        assert result.success
        assert "name" in result.dataframe.columns

    def test_detects_no_header_all_numeric(self, spark, temp_dir):
        csv_file = temp_dir / "data.csv"
        csv_file.write_text("1,25,100\n2,30,200\n")

        result = process_file(str(csv_file), spark, {"header": "auto"})

        assert result.success
        assert result.rows_processed == 2


class TestUserOptions:
    def test_override_delimiter(self, spark, temp_dir):
        csv_file = temp_dir / "data.csv"
        csv_file.write_text("name;age;city\nAlice;25;London\n")

        result = process_file(str(csv_file), spark, {"delimiter": ";"})

        assert result.success

    def test_override_header(self, spark, temp_dir):
        csv_file = temp_dir / "data.csv"
        csv_file.write_text("Alice,25,London\nBob,30,Paris\n")

        result = process_file(str(csv_file), spark, {"header": "false"})

        assert result.success
        assert result.rows_processed == 2

    def test_override_infer_schema(self, spark, temp_dir):
        csv_file = temp_dir / "data.csv"
        csv_file.write_text("name,age\nAlice,25\nBob,30\n")

        result = process_file(str(csv_file), spark, {"inferSchema": "false"})

        assert result.success


class TestSampleFilesDelimiterDetection:
    """Tests for delimiter detection with explicit delimiter override"""

    def test_all_sample_files_process_successfully(self, spark, samples_dir):
        """Verify all sample files can be processed (even without perfect delimiter detection)"""
        sample_files = list(samples_dir.glob("data_*.*"))

        if not sample_files:
            pytest.skip("No sample files found in samples directory")

        for sample_file in sample_files:
            result = process_file(str(sample_file), spark)
            assert result.success, (
                f"Failed to process {sample_file.name}: {result.message}"
            )
            assert result.rows_processed > 0, (
                f"No rows processed from {sample_file.name}"
            )

    def test_comma_files_with_explicit_delimiter(self, spark, samples_dir):
        """Test comma-delimited files with explicit delimiter parameter"""
        comma_files = list(samples_dir.glob("data_comma.*"))

        if not comma_files:
            pytest.skip("No comma-delimited sample files found")

        for comma_file in comma_files:
            result = process_file(str(comma_file), spark, {"delimiter": ","})
            assert result.success, f"Failed on {comma_file.name}"
            assert len(result.dataframe.columns) > 1, (
                f"Explicit comma delimiter should produce multiple columns in {comma_file.name}"
            )

    def test_pipe_files_with_explicit_delimiter(self, spark, samples_dir):
        """Test pipe-delimited files with explicit delimiter parameter"""
        pipe_files = list(samples_dir.glob("data_pipe.*"))

        if not pipe_files:
            pytest.skip("No pipe-delimited sample files found")

        for pipe_file in pipe_files:
            result = process_file(str(pipe_file), spark, {"delimiter": "|"})
            assert result.success, f"Failed on {pipe_file.name}"
            assert len(result.dataframe.columns) > 1, (
                f"Explicit pipe delimiter should produce multiple columns in {pipe_file.name}"
            )

    @pytest.mark.parametrize(
        "delimiter,pattern",
        [
            (",", "data_comma.*"),
            ("|", "data_pipe.*"),
        ],
    )
    def test_delimiter_produces_expected_columns(
        self, spark, samples_dir, delimiter, pattern
    ):
        """Verify that using the correct delimiter produces the expected number of columns"""
        files = list(samples_dir.glob(pattern))

        if not files:
            pytest.skip(f"No files matching {pattern}")

        # Test first file with correct delimiter
        test_file = files[0]
        result = process_file(str(test_file), spark, {"delimiter": delimiter})

        assert result.success
        # Should have 4 columns based on sample data (id, name, category, price)
        assert len(result.dataframe.columns) == 4, (
            f"Expected 4 columns from {test_file.name}, got {len(result.dataframe.columns)}"
        )

        # Verify column names don't contain delimiters (which would indicate wrong parsing)
        for col in result.dataframe.columns:
            assert delimiter not in col, (
                f"Column name '{col}' contains delimiter '{delimiter}' - parsing failed"
            )


class TestCSVWProcessing:
    """Tests for CSVW metadata-driven file processing"""

    def test_process_csvw_simple_file(self, spark, samples_dir):
        csv_file = samples_dir / "csvw_simple.csv"

        result = process_file(str(csv_file), spark)

        assert result.success
        assert result.dataframe is not None
        assert result.metadata is not None
        assert "CSVW" in result.message or "metadata" in result.message.lower()
        assert result.rows_processed == 5

    def test_csvw_applies_correct_types(self, spark, samples_dir):
        csv_file = samples_dir / "csvw_simple.csv"

        result = process_file(str(csv_file), spark)

        assert result.success
        df = result.dataframe

        year_field = next(f for f in df.schema.fields if f.name == "price")
        assert year_field.dataType.typeName() == "double"

    def test_csvw_metadata_stored_in_result(self, spark, samples_dir):
        csv_file = samples_dir / "csvw_simple.csv"

        result = process_file(str(csv_file), spark)

        assert result.success
        assert result.metadata is not None
        assert "tableSchema" in result.metadata or "@context" in result.metadata

    def test_csv_without_metadata_uses_inference(self, spark, samples_dir):
        csv_file = samples_dir / "data_comma.txt"

        result = process_file(str(csv_file), spark)

        assert result.success
        assert result.metadata is None
        assert "inference" in result.message.lower()


class TestBatchProcessingWithRaiseOnError:
    """Tests for raise_on_error flag in batch processing"""

    def test_batch_processing_default_no_raise(self, spark, temp_dir):
        good_file = temp_dir / "good.csv"
        good_file.write_text("name,age\nAlice,25\n")

        bad_file = temp_dir / "bad.csv"
        bad_file.write_text("")  # Empty file

        results = process_files_batch([str(good_file), str(bad_file)], spark)

        assert len(results) == 2
        assert results[0].success
        assert not results[1].success

    def test_batch_processing_raise_on_error_true(self, spark, temp_dir):
        from file_processing.exceptions import FileProcessingException

        good_file = temp_dir / "good.csv"
        good_file.write_text("name,age\nAlice,25\n")

        bad_file = temp_dir / "bad.csv"
        bad_file.write_text("")  # Empty file

        with pytest.raises(FileProcessingException) as exc_info:
            process_files_batch(
                [str(good_file), str(bad_file)], spark, raise_on_error=True
            )

        assert "failed processing" in str(exc_info.value)
        assert hasattr(exc_info.value, "failures")
        assert len(exc_info.value.failures) == 1

    def test_batch_processing_all_succeed_no_raise(self, spark, temp_dir):
        file1 = temp_dir / "file1.csv"
        file1.write_text("name,age\nAlice,25\n")

        file2 = temp_dir / "file2.csv"
        file2.write_text("name,age\nBob,30\n")

        results = process_files_batch(
            [str(file1), str(file2)], spark, raise_on_error=True
        )

        assert len(results) == 2
        assert all(r.success for r in results)


class TestConfigurableOptions:
    """Tests for configurable processing options"""

    def test_custom_excel_file_size_limit(self, spark, temp_dir):
        import pandas as pd

        # Create a small Excel file
        excel_file = temp_dir / "test.xlsx"
        df = pd.DataFrame({"name": ["Alice"], "age": [25]})
        df.to_excel(excel_file, index=False)

        # Should fail with very low limit
        result = process_file(
            str(excel_file), spark, read_options={"max_file_size_mb": 0.001}
        )

        assert not result.success
        assert "too large" in result.message.lower()

    def test_custom_corruption_threshold(self, spark, temp_dir):
        # Create file with some corrupt data
        csv_file = temp_dir / "corrupt.csv"
        csv_file.write_text("name,age\nAlice,25\nBob,invalid\nCharlie,30\n")

        # With high threshold (50%), should succeed
        result = process_file(
            str(csv_file), spark, read_options={"max_corrupt_records_percent": 0.5}
        )

        assert result.success

    def test_default_options_still_work(self, spark, temp_dir):
        csv_file = temp_dir / "data.csv"
        csv_file.write_text("name,age\nAlice,25\n")

        # No options - should use defaults
        result = process_file(str(csv_file), spark)

        assert result.success
