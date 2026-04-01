"""Tests for handler registry extensibility."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for tests."""
    spark = (
        SparkSession.builder.appName("RegistryTests").master("local[2]").getOrCreate()
    )
    yield spark
    spark.stop()


class TestHandlerRegistry:
    """Tests for custom handler registration."""

    def test_register_and_get_handler(self):
        from file_processing import register_handler, unregister_handler
        from file_processing.registry import get_handler

        def custom_handler(file_path, spark, options):
            return None

        # Register handler
        register_handler(".custom", custom_handler)

        # Retrieve handler
        handler = get_handler(".custom")
        assert handler == custom_handler

        # Cleanup
        unregister_handler(".custom")

    def test_handler_priority(self):
        from file_processing import register_handler, unregister_handler
        from file_processing.registry import get_handler

        def low_priority_handler(file_path, spark, options):
            return "low"

        def high_priority_handler(file_path, spark, options):
            return "high"

        # Register with different priorities
        register_handler(".test", low_priority_handler, priority=1)
        register_handler(".test", high_priority_handler, priority=10)

        # Should get high priority handler
        handler = get_handler(".test")
        assert handler == high_priority_handler

        # Cleanup
        unregister_handler(".test")

    def test_unregister_specific_handler(self):
        from file_processing import register_handler, unregister_handler
        from file_processing.registry import get_handler

        def handler1(file_path, spark, options):
            return "handler1"

        def handler2(file_path, spark, options):
            return "handler2"

        # Register two handlers
        register_handler(".test", handler1, priority=1)
        register_handler(".test", handler2, priority=2)

        # Remove specific handler
        unregister_handler(".test", handler2)

        # Should get remaining handler
        handler = get_handler(".test")
        assert handler == handler1

        # Cleanup
        unregister_handler(".test")

    def test_unregister_all_handlers(self):
        from file_processing import register_handler, unregister_handler
        from file_processing.registry import get_handler

        def handler1(file_path, spark, options):
            return None

        def handler2(file_path, spark, options):
            return None

        # Register multiple handlers
        register_handler(".test", handler1)
        register_handler(".test", handler2)

        # Remove all handlers for extension
        unregister_handler(".test")

        # Should return None
        handler = get_handler(".test")
        assert handler is None

    def test_list_registered_extensions(self):
        from file_processing import (
            list_registered_extensions,
            register_handler,
            unregister_handler,
        )

        def handler(file_path, spark, options):
            return None

        # Register handlers for multiple extensions
        register_handler(".ext1", handler)
        register_handler(".ext2", handler)

        extensions = list_registered_extensions()
        assert ".ext1" in extensions
        assert ".ext2" in extensions

        # Cleanup
        unregister_handler(".ext1")
        unregister_handler(".ext2")

    def test_custom_handler_integration(self, spark, tmp_path):
        from file_processing import (
            FileProcessingResult,
            process_file,
            register_handler,
            unregister_handler,
        )

        # Create test file
        test_file = tmp_path / "test.custom"
        test_file.write_text("custom data")

        # Define custom handler
        def custom_handler(file_path, spark, options):
            # Simple custom logic
            df = spark.createDataFrame([("custom", "data")], ["col1", "col2"])
            return FileProcessingResult(
                success=True,
                file=file_path,
                dataframe=df,
                message="Processed with custom handler",
                rows_processed=1,
            )

        # Register handler
        register_handler(".custom", custom_handler)

        # Process file with custom handler
        result = process_file(str(test_file), spark)

        assert result.success
        assert result.message == "Processed with custom handler"
        assert result.dataframe is not None
        assert result.dataframe.count() == 1

        # Cleanup
        unregister_handler(".custom")
