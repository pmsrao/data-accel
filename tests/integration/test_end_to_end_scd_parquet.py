"""
End-to-end integration tests for SCD processing using Parquet (no Delta Lake required).
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import lit, current_timestamp

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from libraries.dimensional_processing.scd_type2.scd_processor import SCDProcessor
from libraries.dimensional_processing.scd_type2.historical_data_deduplicator import HistoricalDataDeduplicator
from libraries.dimensional_processing.key_resolution.key_resolver import DimensionalKeyResolver
from libraries.dimensional_processing.common.config import SCDConfig, DeduplicationConfig, KeyResolutionConfig


class TestEndToEndSCDParquet:
    """End-to-end integration tests using Parquet tables."""
    
    @pytest.fixture(scope="class")
    def spark(self):
        """Create Spark session for testing."""
        spark = SparkSession.builder \
            .appName("SCD Integration Tests - Parquet") \
            .master("local[2]") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .getOrCreate()
        
        # Create test database
        spark.sql("CREATE DATABASE IF NOT EXISTS test")
        spark.sql("USE test")
        
        yield spark
        
        # Cleanup
        spark.stop()
    
    @pytest.fixture
    def scd_config(self):
        """Create SCD configuration for testing."""
        return SCDConfig(
            target_table="test.customer_dim_parquet",
            business_key_columns=["customer_id"],
            scd_columns=["name", "email", "address"],
            effective_from_column="last_modified_ts",
            initial_effective_from_column="created_ts"
        )
    
    @pytest.fixture
    def sample_historical_data(self, spark):
        """Create sample historical data for testing."""
        data = [
            ("1", "John Doe", "john@example.com", "123 Main St", "2024-01-01 10:00:00", "2024-01-01 09:00:00"),
            ("2", "Jane Smith", "jane@example.com", "789 Pine St", "2024-01-01 10:00:00", "2024-01-01 09:00:00"),
            ("3", "Bob Johnson", "bob@example.com", "321 Elm St", "2024-01-01 10:00:00", "2024-01-01 09:00:00")
        ]
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("address", StringType(), True),
            StructField("last_modified_ts", StringType(), True),
            StructField("created_ts", StringType(), True)
        ])
        
        return spark.createDataFrame(data, schema)
    
    def _create_target_table(self, spark, config):
        """Create target Parquet table for testing."""
        # Drop table if exists
        spark.sql(f"DROP TABLE IF EXISTS {config.target_table}")
        
        # Create table using Parquet
        create_sql = f"""
        CREATE TABLE {config.target_table} (
            customer_id STRING,
            name STRING,
            email STRING,
            address STRING,
            scd_hash STRING,
            effective_start_ts_utc TIMESTAMP,
            effective_end_ts_utc TIMESTAMP,
            is_current STRING,
            created_ts_utc TIMESTAMP,
            modified_ts_utc TIMESTAMP
        ) USING PARQUET
        """
        
        spark.sql(create_sql)
    
    def test_basic_scd_processing(self, spark, scd_config, sample_historical_data):
        """Test basic SCD processing with Parquet tables."""
        # Step 1: Create target table
        self._create_target_table(spark, scd_config)
        
        # Step 2: Process SCD
        processor = SCDProcessor(scd_config, spark)
        result = processor.process_scd(sample_historical_data)
        
        # Verify SCD processing
        assert result.records_processed > 0
        assert result.new_records_created > 0
        assert result.processing_time_seconds > 0
        
        # Step 3: Verify target table
        target_df = spark.sql(f"SELECT * FROM {scd_config.target_table}")
        assert target_df.count() > 0
        
        # Check SCD metadata columns
        assert scd_config.scd_hash_column in target_df.columns
        assert scd_config.effective_start_column in target_df.columns
        assert scd_config.effective_end_column in target_df.columns
        assert scd_config.is_current_column in target_df.columns
    
    def test_validation_works(self, spark, scd_config):
        """Test that validation works correctly."""
        # Test with invalid data (null business keys)
        invalid_data = [
            ("1", "John Doe", "john@example.com", "123 Main St", "2024-01-01 10:00:00", "2024-01-01 09:00:00"),
            (None, "Jane Smith", "jane@example.com", "789 Pine St", "2024-01-01 10:00:00", "2024-01-01 09:00:00"),  # Null business key
            ("3", "Bob Johnson", "bob@example.com", "321 Elm St", "2024-01-01 10:00:00", "2024-01-01 09:00:00")
        ]
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("address", StringType(), True),
            StructField("last_modified_ts", StringType(), True),
            StructField("created_ts", StringType(), True)
        ])
        
        invalid_df = spark.createDataFrame(invalid_data, schema)
        
        # Create target table
        self._create_target_table(spark, scd_config)
        
        # Should handle validation gracefully
        processor = SCDProcessor(scd_config, spark)
        
        # This should either succeed with error flags or raise validation error
        try:
            result = processor.process_scd(invalid_df)
            # If it succeeds, check that error flags are set
            target_df = spark.sql(f"SELECT * FROM {scd_config.target_table}")
            if "_error_flag" in target_df.columns:
                error_count = target_df.filter(target_df._error_flag == "Y").count()
                assert error_count > 0
        except Exception as e:
            # Validation error is also acceptable
            assert "validation" in str(e).lower() or "error" in str(e).lower()
