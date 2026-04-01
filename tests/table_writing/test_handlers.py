"""Tests for mode and format handler registries."""

import tempfile

import pytest
from pyspark.sql import SparkSession
from table_writing import (
    TableWriteOptions,
    TableWriteResult,
    get_format_handler,
    list_registered_modes,
    register_format_handler,
    register_mode_handler,
    unregister_mode_handler,
    write_table,
)
from table_writing.exceptions import TableWritingException


@pytest.fixture(scope="session")
def spark():
    warehouse_dir = tempfile.mkdtemp()
    spark = (
        SparkSession.builder.appName("HandlerRegistryTests")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def sample_df(spark):
    data = [(1, "Alice"), (2, "Bob")]
    return spark.createDataFrame(data, ["id", "name"])


class TestModeHandlerRegistry:
    """Tests for mode handler registry."""

    def test_list_registered_modes(self):
        """Test listing registered modes includes built-in modes."""
        modes = list_registered_modes()
        assert "append" in modes
        assert "overwrite" in modes
        assert "merge" in modes
        assert "errorIfExists" in modes

    def test_register_custom_mode_handler(self, spark, sample_df):
        """Test registering and using a custom mode handler."""

        def custom_mode(df, table_name, spark_session, options, raise_on_error):
            row_count = df.count()
            df.write.format("parquet").mode("append").saveAsTable(table_name)
            return TableWriteResult(
                success=True,
                table_name=table_name,
                rows_written=row_count,
                message="Custom mode executed",
            )

        register_mode_handler("custom", custom_mode)

        try:
            assert "custom" in list_registered_modes()

            # Use parquet format to avoid validation error
            options = TableWriteOptions(mode="custom", format="parquet")
            result = write_table(sample_df, "test_custom_mode", spark, options)

            assert result.success
            assert result.message == "Custom mode executed"
            assert result.rows_written == 2
        finally:
            unregister_mode_handler("custom")

    def test_unregister_mode_handler(self):
        """Test unregistering a mode handler."""

        def dummy_handler(df, table_name, spark, options, raise_on_error):
            return TableWriteResult(success=True, table_name=table_name, rows_written=0)

        register_mode_handler("temp_mode", dummy_handler)
        assert "temp_mode" in list_registered_modes()

        unregister_mode_handler("temp_mode")
        assert "temp_mode" not in list_registered_modes()

    def test_unregister_nonexistent_mode(self):
        """Test unregistering a mode that doesn't exist doesn't raise error."""
        unregister_mode_handler("nonexistent_mode")

    def test_custom_mode_with_raise_on_error(self, spark, sample_df):
        """Test custom mode handler respects raise_on_error flag."""

        def failing_mode(df, table_name, spark_session, options, raise_on_error):
            exc = TableWritingException("Custom failure", "custom_error", "Test error")
            if raise_on_error:
                raise exc
            return TableWriteResult(
                success=False,
                table_name=table_name,
                rows_written=0,
                error=exc.to_dict(),
            )

        register_mode_handler("failing_mode", failing_mode)

        try:
            # Use parquet format to pass validation
            options = TableWriteOptions(mode="failing_mode", format="parquet")

            # Without raise_on_error
            result = write_table(
                sample_df, "test_fail", spark, options, raise_on_error=False
            )
            assert not result.success
            assert result.error["error_type"] == "custom_error"

            # With raise_on_error
            with pytest.raises(TableWritingException) as exc_info:
                write_table(sample_df, "test_fail", spark, options, raise_on_error=True)
            assert exc_info.value.error_type == "custom_error"
        finally:
            unregister_mode_handler("failing_mode")


class TestFormatHandlerRegistry:
    """Tests for format handler registry."""

    def test_get_builtin_format_handlers(self):
        """Test getting built-in format handlers."""
        delta_handler = get_format_handler("delta")
        assert delta_handler is not None
        assert delta_handler.supports_merge()

        parquet_handler = get_format_handler("parquet")
        assert parquet_handler is not None
        assert not parquet_handler.supports_merge()

        orc_handler = get_format_handler("orc")
        assert orc_handler is not None
        assert not orc_handler.supports_merge()

    def test_get_nonexistent_format_handler(self):
        """Test getting a format handler that doesn't exist."""
        handler = get_format_handler("nonexistent")
        assert handler is None

    def test_register_custom_format_handler(self, spark, sample_df):
        """Test registering a custom format handler."""

        class CustomFormatHandler:
            def supports_merge(self):
                return False

            def write(self, df, table_name, mode, options):
                df.write.format("parquet").mode(mode).saveAsTable(table_name)

        register_format_handler("custom_format", CustomFormatHandler())

        handler = get_format_handler("custom_format")
        assert handler is not None
        assert not handler.supports_merge()

        options = TableWriteOptions(format="custom_format", mode="append")
        result = write_table(sample_df, "test_custom_format", spark, options)

        assert result.success
        assert result.rows_written == 2


class TestMergeRaiseOnError:
    """Tests for raise_on_error behavior in merge mode."""

    def test_merge_table_not_exists_raise_on_error_false(self, spark, sample_df):
        """Test merge mode when table doesn't exist and create_if_not_exists=False."""
        options = TableWriteOptions(
            mode="merge",
            merge_keys=["id"],
            create_if_not_exists=False,
        )

        result = write_table(
            sample_df, "test_merge_no_create", spark, options, raise_on_error=False
        )

        assert not result.success
        assert result.error["error_type"] == "table_not_found"

    def test_merge_table_not_exists_raise_on_error_true(self, spark, sample_df):
        """Test merge mode raises exception when table doesn't exist."""
        options = TableWriteOptions(
            mode="merge",
            merge_keys=["id"],
            create_if_not_exists=False,
        )

        with pytest.raises(TableWritingException) as exc_info:
            write_table(
                sample_df, "test_merge_no_create", spark, options, raise_on_error=True
            )

        assert exc_info.value.error_type == "table_not_found"
        assert "create_if_not_exists" in exc_info.value.details

    def test_merge_creates_table_when_allowed(self, spark, sample_df):
        """Test merge mode creates table when create_if_not_exists=True."""
        options = TableWriteOptions(
            format="parquet",
            mode="append",  # Use append since merge requires Delta
            create_if_not_exists=True,
        )

        result = write_table(
            sample_df, "test_append_create", spark, options, raise_on_error=True
        )

        assert result.success
        assert result.rows_written == 2
        assert spark.catalog.tableExists("test_append_create")
