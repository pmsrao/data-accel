"""
End-to-end integration tests for SCD processing.
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


class TestEndToEndSCD:
    """End-to-end integration tests for SCD processing."""
    
    @pytest.fixture(scope="class")
    def spark(self):
        """Create Spark session for testing."""
        return SparkSession.builder.appName("test").master("local[2]").getOrCreate()
    
    @pytest.fixture
    def scd_config(self):
        """Create SCD configuration for testing."""
        return SCDConfig(
            target_table="test.customer_dim",
            business_key_columns=["customer_id"],
            scd_columns=["name", "email", "address"],
            effective_from_column="last_modified_ts",
            initial_effective_from_column="created_ts"
        )
    
    @pytest.fixture
    def dedup_config(self):
        """Create deduplication configuration for testing."""
        return DeduplicationConfig(
            business_key_columns=["customer_id"],
            scd_columns=["name", "email", "address"],
            effective_from_column="created_ts",
            deduplication_strategy="latest"
        )
    
    @pytest.fixture
    def key_config(self):
        """Create key resolution configuration for testing."""
        return KeyResolutionConfig(
            dimension_table="test.customer_dim",
            business_key_columns=["customer_id"],
            surrogate_key_column="customer_sk"
        )
    
    @pytest.fixture
    def sample_historical_data(self, spark):
        """Create sample historical data with duplicates for testing."""
        data = [
            ("1", "John Doe", "john@example.com", "123 Main St", "2024-01-01 10:00:00", "2024-01-01 09:00:00"),
            ("1", "John Doe", "john@example.com", "123 Main St", "2024-01-01 11:00:00", "2024-01-01 09:00:00"),  # Duplicate
            ("1", "John Smith", "john.smith@example.com", "456 Oak Ave", "2024-01-01 12:00:00", "2024-01-01 09:00:00"),  # Different SCD
            ("2", "Jane Smith", "jane@example.com", "789 Pine St", "2024-01-01 10:00:00", "2024-01-01 09:00:00"),
            ("2", "Jane Smith", "jane@example.com", "789 Pine St", "2024-01-01 11:00:00", "2024-01-01 09:00:00"),  # Duplicate
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
    
    @pytest.fixture
    def sample_fact_data(self, spark):
        """Create sample fact data for testing."""
        data = [
            ("1", "2024-01-01 10:00:00", 100.0),
            ("2", "2024-01-01 11:00:00", 200.0),
            ("3", "2024-01-01 12:00:00", 300.0)
        ]
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("transaction_date", StringType(), True),
            StructField("amount", StringType(), True)
        ])
        
        return spark.createDataFrame(data, schema)
    
    def test_historical_dimension_build_workflow(self, spark, scd_config, dedup_config, sample_historical_data):
        """Test complete historical dimension build workflow."""
        # Step 1: Create target table
        self._create_target_table(spark, scd_config)
        
        # Step 2: Deduplicate historical data
        deduplicator = HistoricalDataDeduplicator(dedup_config, spark)
        deduplicated_df = deduplicator.deduplicate_historical_data(sample_historical_data)
        
        # Verify deduplication
        assert deduplicated_df.count() < sample_historical_data.count()
        assert deduplicated_df.count() > 0
        
        # Step 3: Process SCD with deduplicated data
        processor = SCDProcessor(scd_config, spark)
        result = processor.process_scd(deduplicated_df)
        
        # Verify SCD processing
        assert result.records_processed > 0
        assert result.new_records_created > 0
        assert result.processing_time_seconds > 0
        
        # Step 4: Verify target table
        target_df = spark.sql(f"SELECT * FROM {scd_config.target_table}")
        assert target_df.count() > 0
        
        # Check SCD metadata columns
        assert scd_config.scd_hash_column in target_df.columns
        assert scd_config.effective_start_column in target_df.columns
        assert scd_config.effective_end_column in target_df.columns
        assert scd_config.is_current_column in target_df.columns
    
    def test_incremental_scd_processing(self, spark, scd_config, sample_historical_data):
        """Test incremental SCD processing."""
        # Step 1: Create target table
        self._create_target_table(spark, scd_config)
        
        # Step 2: Initial load
        processor = SCDProcessor(scd_config, spark)
        initial_result = processor.process_scd(sample_historical_data)
        
        # Step 3: Incremental update with new data
        new_data = [
            ("1", "John Doe Updated", "john.updated@example.com", "123 Main St Updated", "2024-01-02 10:00:00", "2024-01-01 09:00:00"),
            ("4", "Alice Brown", "alice@example.com", "999 Cedar St", "2024-01-02 10:00:00", "2024-01-02 09:00:00")
        ]
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("address", StringType(), True),
            StructField("last_modified_ts", StringType(), True),
            StructField("created_ts", StringType(), True)
        ])
        
        new_df = spark.createDataFrame(new_data, schema)
        
        # Process incremental data
        incremental_result = processor.process_scd(new_df)
        
        # Verify incremental processing
        assert incremental_result.records_processed > 0
        assert incremental_result.new_records_created > 0
        
        # Verify target table has both old and new records
        target_df = spark.sql(f"SELECT * FROM {scd_config.target_table}")
        assert target_df.count() > initial_result.records_processed
    
    def test_key_resolution_workflow(self, spark, scd_config, key_config, sample_historical_data, sample_fact_data):
        """Test complete key resolution workflow."""
        # Step 1: Create and populate dimension table
        self._create_target_table(spark, scd_config)
        processor = SCDProcessor(scd_config, spark)
        processor.process_scd(sample_historical_data)
        
        # Step 2: Resolve keys for fact table
        resolver = DimensionalKeyResolver(key_config, spark)
        resolved_df = resolver.resolve_keys(sample_fact_data, "transaction_date")
        
        # Verify key resolution
        assert resolved_df.count() == sample_fact_data.count()
        assert key_config.surrogate_key_column in resolved_df.columns
        
        # Check that surrogate keys are resolved
        resolved_count = resolved_df.filter(resolved_df[key_config.surrogate_key_column].isNotNull()).count()
        assert resolved_count > 0
    
    def test_error_handling_and_validation(self, spark, scd_config, sample_historical_data):
        """Test error handling and validation."""
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
        
        # Should raise validation error
        processor = SCDProcessor(scd_config, spark)
        with pytest.raises(Exception):  # Should raise SCDValidationError
            processor.process_scd(invalid_df)
    
    def test_performance_with_large_dataset(self, spark, scd_config, dedup_config):
        """Test performance with larger dataset."""
        # Create larger dataset
        data = []
        for i in range(1000):  # 1000 records
            data.append((
                str(i),
                f"Customer {i}",
                f"customer{i}@example.com",
                f"Address {i}",
                "2024-01-01 10:00:00",
                "2024-01-01 09:00:00"
            ))
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("address", StringType(), True),
            StructField("last_modified_ts", StringType(), True),
            StructField("created_ts", StringType(), True)
        ])
        
        large_df = spark.createDataFrame(data, schema)
        
        # Create target table
        self._create_target_table(spark, scd_config)
        
        # Test deduplication performance
        deduplicator = HistoricalDataDeduplicator(dedup_config, spark)
        deduplicated_df = deduplicator.deduplicate_historical_data(large_df)
        
        # Test SCD processing performance
        processor = SCDProcessor(scd_config, spark)
        result = processor.process_scd(deduplicated_df)
        
        # Verify performance metrics
        assert result.records_processed > 0
        assert result.processing_time_seconds > 0
        assert result.processing_time_seconds < 300  # Should complete within 5 minutes
    
    def _create_target_table(self, spark, scd_config):
        """Create target table for testing."""
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {scd_config.target_table} (
            customer_id STRING,
            name STRING,
            email STRING,
            address STRING,
            {scd_config.scd_hash_column} STRING,
            {scd_config.effective_start_column} TIMESTAMP,
            {scd_config.effective_end_column} TIMESTAMP,
            {scd_config.is_current_column} STRING,
            {scd_config.created_ts_column} TIMESTAMP,
            {scd_config.modified_ts_column} TIMESTAMP
        ) USING DELTA
        """
        
        spark.sql(create_sql)
        
        # Clear any existing data
        spark.sql(f"DELETE FROM {scd_config.target_table}")
    
    @pytest.fixture(autouse=True)
    def cleanup(self, spark, scd_config):
        """Clean up test data after each test."""
        yield
        try:
            spark.sql(f"DROP TABLE IF EXISTS {scd_config.target_table}")
        except:
            pass  # Ignore cleanup errors
