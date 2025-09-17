"""
End-to-end integration tests for SCD processing with proper Delta Lake configuration.
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


class TestEndToEndSCDFixed:
    """End-to-end integration tests for SCD processing with proper Delta Lake setup."""
    
    @pytest.fixture(scope="class")
    def spark(self):
        """Create Spark session with Delta Lake support."""
        spark = SparkSession.builder \
            .appName("SCD Integration Tests") \
            .master("local[2]") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
            .getOrCreate()
        
        # Enable Delta Lake
        spark.sql("CREATE DATABASE IF NOT EXISTS test")
        spark.sql("USE test")
        
        yield spark
        
        # Cleanup
        spark.stop()
    
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
    
    def _create_target_table(self, spark, config):
        """Create target Delta table for testing."""
        # Drop table if exists
        spark.sql(f"DROP TABLE IF EXISTS {config.target_table}")
        
        # Create table
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
        ) USING DELTA
        """
        
        spark.sql(create_sql)
    
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
        
        # Process incremental update
        incremental_result = processor.process_scd(new_df)
        
        # Verify incremental processing
        assert incremental_result.records_processed > 0
        assert incremental_result.new_records_created > 0
        
        # Verify target table has more records
        final_count = spark.sql(f"SELECT COUNT(*) as count FROM {scd_config.target_table}").collect()[0]["count"]
        assert final_count > initial_result.records_processed
    
    def test_key_resolution_workflow(self, spark, scd_config, key_config, sample_historical_data):
        """Test complete key resolution workflow."""
        # Step 1: Create and populate dimension table
        self._create_target_table(spark, scd_config)
        
        processor = SCDProcessor(scd_config, spark)
        processor.process_scd(sample_historical_data)
        
        # Step 2: Create fact data
        fact_data = [
            ("1", "2024-01-01 10:00:00", 100.0),
            ("2", "2024-01-01 11:00:00", 200.0),
            ("3", "2024-01-01 12:00:00", 300.0)
        ]
        
        fact_schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("transaction_date", StringType(), True),
            StructField("amount", StringType(), True)
        ])
        
        fact_df = spark.createDataFrame(fact_data, fact_schema)
        
        # Step 3: Resolve keys
        key_resolver = DimensionalKeyResolver(key_config, spark)
        resolved_df = key_resolver.resolve_dimensional_keys(fact_df, "transaction_date")
        
        # Verify key resolution
        assert resolved_df.count() == fact_df.count()
        assert "customer_sk" in resolved_df.columns
        
        # Verify all records have resolved keys
        null_keys = resolved_df.filter(resolved_df.customer_sk.isNull()).count()
        assert null_keys == 0
