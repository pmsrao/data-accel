"""
Unit tests for HistoricalDataDeduplicator.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from libraries.dimensional_processing.scd_type2.historical_data_deduplicator import HistoricalDataDeduplicator
from libraries.dimensional_processing.common.config import DeduplicationConfig
from libraries.dimensional_processing.common.exceptions import DeduplicationError


class TestHistoricalDataDeduplicator:
    """Test cases for HistoricalDataDeduplicator."""
    
    @pytest.fixture
    def spark(self):
        """Create Spark session for testing."""
        return SparkSession.builder.appName("test").master("local[2]").getOrCreate()
    
    @pytest.fixture
    def dedup_config(self):
        """Create deduplication configuration for testing."""
        return DeduplicationConfig(
            business_key_columns=["customer_id"],
            scd_columns=["name", "email"],
            effective_from_column="created_ts",
            deduplication_strategy="latest"
        )
    
    @pytest.fixture
    def sample_historical_data(self, spark):
        """Create sample historical data with duplicates for testing."""
        data = [
            ("1", "John Doe", "john@example.com", "2024-01-01 10:00:00"),
            ("1", "John Doe", "john@example.com", "2024-01-01 11:00:00"),  # Duplicate
            ("1", "John Smith", "john.smith@example.com", "2024-01-01 12:00:00"),  # Different SCD
            ("2", "Jane Smith", "jane@example.com", "2024-01-01 10:00:00"),
            ("2", "Jane Smith", "jane@example.com", "2024-01-01 11:00:00"),  # Duplicate
            ("3", "Bob Johnson", "bob@example.com", "2024-01-01 10:00:00")
        ]
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("created_ts", StringType(), True)
        ])
        
        return spark.createDataFrame(data, schema)
    
    @pytest.fixture
    def deduplicator(self, dedup_config, spark):
        """Create HistoricalDataDeduplicator instance for testing."""
        return HistoricalDataDeduplicator(dedup_config, spark)
    
    def test_init(self, dedup_config, spark):
        """Test HistoricalDataDeduplicator initialization."""
        deduplicator = HistoricalDataDeduplicator(dedup_config, spark)
        
        assert deduplicator.config == dedup_config
        assert deduplicator.spark == spark
        assert deduplicator.hash_manager is not None
        assert deduplicator.validator is not None
    
    def test_validate_input_data_success(self, deduplicator, sample_historical_data):
        """Test successful input data validation."""
        result = deduplicator._validate_input_data(sample_historical_data)
        
        assert result.is_valid is True
        assert len(result.errors) == 0
    
    def test_validate_input_data_missing_columns(self, deduplicator, sample_historical_data):
        """Test input data validation with missing columns."""
        # Remove a required column
        incomplete_data = sample_historical_data.drop("name")
        
        result = deduplicator._validate_input_data(incomplete_data)
        
        assert result.is_valid is False
        assert "Missing required columns" in result.errors[0]
    
    def test_validate_input_data_null_business_keys(self, deduplicator, spark):
        """Test input data validation with null business keys."""
        data = [
            ("1", "John Doe", "john@example.com", "2024-01-01 10:00:00"),
            (None, "Jane Smith", "jane@example.com", "2024-01-01 10:00:00"),  # Null business key
            ("3", "Bob Johnson", "bob@example.com", "2024-01-01 10:00:00")
        ]
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("created_ts", StringType(), True)
        ])
        
        df_with_nulls = spark.createDataFrame(data, schema)
        result = deduplicator._validate_input_data(df_with_nulls)
        
        assert result.is_valid is False
        assert "null values in business key column" in result.errors[0]
    
    def test_compute_scd_hashes(self, deduplicator, sample_historical_data):
        """Test SCD hash computation."""
        df_with_hash = deduplicator._compute_scd_hashes(sample_historical_data)
        
        assert deduplicator.config.scd_hash_column in df_with_hash.columns
        assert df_with_hash.count() == sample_historical_data.count()
        
        # Check that records with same SCD columns have same hash
        john_records = df_with_hash.filter(df_with_hash.customer_id == "1")
        john_hashes = john_records.select(deduplicator.config.scd_hash_column).distinct().count()
        
        # Should have 2 different hashes (one for each unique SCD combination)
        assert john_hashes == 2
    
    def test_apply_deduplication_strategy_latest(self, deduplicator, sample_historical_data):
        """Test 'latest' deduplication strategy."""
        df_with_hash = deduplicator._compute_scd_hashes(sample_historical_data)
        deduplicated_df = deduplicator._apply_deduplication_strategy(df_with_hash)
        
        # Should have fewer records after deduplication
        assert deduplicated_df.count() < df_with_hash.count()
        
        # Check that we kept the latest record for each SCD hash
        customer_1_records = deduplicated_df.filter(deduplicated_df.customer_id == "1")
        assert customer_1_records.count() == 2  # Two different SCD combinations
    
    def test_apply_deduplication_strategy_earliest(self, deduplicator, sample_historical_data):
        """Test 'earliest' deduplication strategy."""
        deduplicator.config.deduplication_strategy = "earliest"
        
        df_with_hash = deduplicator._compute_scd_hashes(sample_historical_data)
        deduplicated_df = deduplicator._apply_deduplication_strategy(df_with_hash)
        
        # Should have fewer records after deduplication
        assert deduplicated_df.count() < df_with_hash.count()
    
    def test_apply_deduplication_strategy_significant(self, deduplicator, spark):
        """Test 'significant' deduplication strategy."""
        # Create data with significance column
        data = [
            ("1", "John Doe", "john@example.com", "2024-01-01 10:00:00", "N"),
            ("1", "John Doe", "john@example.com", "2024-01-01 11:00:00", "Y"),  # Significant
            ("2", "Jane Smith", "jane@example.com", "2024-01-01 10:00:00", "N"),
            ("2", "Jane Smith", "jane@example.com", "2024-01-01 11:00:00", "N")
        ]
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("created_ts", StringType(), True),
            StructField("is_significant", StringType(), True)
        ])
        
        df_with_significance = spark.createDataFrame(data, schema)
        
        # Update config to include significance column
        deduplicator.config.deduplication_strategy = "significant"
        deduplicator.config.significance_column = "is_significant"
        
        df_with_hash = deduplicator._compute_scd_hashes(df_with_significance)
        deduplicated_df = deduplicator._apply_deduplication_strategy(df_with_hash)
        
        # Should keep only significant records
        assert deduplicated_df.count() == 1  # Only one significant record
    
    def test_apply_deduplication_strategy_custom(self, deduplicator, sample_historical_data):
        """Test 'custom' deduplication strategy."""
        # Define custom logic
        def custom_logic(df):
            return df.filter(df.customer_id == "1")  # Keep only customer 1
        
        deduplicator.config.deduplication_strategy = "custom"
        deduplicator.config.custom_deduplication_logic = custom_logic
        
        df_with_hash = deduplicator._compute_scd_hashes(sample_historical_data)
        deduplicated_df = deduplicator._apply_deduplication_strategy(df_with_hash)
        
        # Should apply custom logic
        assert deduplicated_df.count() == 3  # Only customer 1 records
    
    def test_apply_deduplication_strategy_invalid(self, deduplicator, sample_historical_data):
        """Test invalid deduplication strategy."""
        deduplicator.config.deduplication_strategy = "invalid_strategy"
        
        df_with_hash = deduplicator._compute_scd_hashes(sample_historical_data)
        
        with pytest.raises(DeduplicationError):
            deduplicator._apply_deduplication_strategy(df_with_hash)
    
    def test_keep_latest_per_scd_hash(self, deduplicator, sample_historical_data):
        """Test keeping latest record per SCD hash."""
        df_with_hash = deduplicator._compute_scd_hashes(sample_historical_data)
        deduplicated_df = deduplicator._keep_latest_per_scd_hash(df_with_hash)
        
        # Should have fewer records
        assert deduplicated_df.count() < df_with_hash.count()
        
        # Check that we kept the latest record for each SCD hash
        customer_1_records = deduplicated_df.filter(deduplicated_df.customer_id == "1")
        assert customer_1_records.count() == 2  # Two different SCD combinations
    
    def test_keep_earliest_per_scd_hash(self, deduplicator, sample_historical_data):
        """Test keeping earliest record per SCD hash."""
        df_with_hash = deduplicator._compute_scd_hashes(sample_historical_data)
        deduplicated_df = deduplicator._keep_earliest_per_scd_hash(df_with_hash)
        
        # Should have fewer records
        assert deduplicated_df.count() < df_with_hash.count()
    
    def test_keep_significant_records_with_significance_column(self, deduplicator, spark):
        """Test keeping significant records with significance column."""
        data = [
            ("1", "John Doe", "john@example.com", "2024-01-01 10:00:00", "Y"),
            ("1", "John Doe", "john@example.com", "2024-01-01 11:00:00", "N"),
            ("2", "Jane Smith", "jane@example.com", "2024-01-01 10:00:00", "N")
        ]
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("created_ts", StringType(), True),
            StructField("is_significant", StringType(), True)
        ])
        
        df_with_significance = spark.createDataFrame(data, schema)
        deduplicator.config.significance_column = "is_significant"
        
        result_df = deduplicator._keep_significant_records(df_with_significance)
        
        # Should keep only significant records
        assert result_df.count() == 1
        assert result_df.filter(result_df.customer_id == "1").count() == 1
    
    def test_keep_significant_records_without_significance_column(self, deduplicator, sample_historical_data):
        """Test keeping significant records without significance column."""
        with pytest.raises(DeduplicationError):
            deduplicator._keep_significant_records(sample_historical_data)
    
    def test_apply_custom_logic_success(self, deduplicator, sample_historical_data):
        """Test successful custom logic application."""
        def custom_logic(df):
            return df.filter(df.customer_id == "1")
        
        deduplicator.config.custom_deduplication_logic = custom_logic
        
        result_df = deduplicator._apply_custom_logic(sample_historical_data)
        
        assert result_df.count() == 3  # Only customer 1 records
    
    def test_apply_custom_logic_failure(self, deduplicator, sample_historical_data):
        """Test custom logic application failure."""
        def failing_logic(df):
            raise Exception("Custom logic failed")
        
        deduplicator.config.custom_deduplication_logic = failing_logic
        
        with pytest.raises(DeduplicationError):
            deduplicator._apply_custom_logic(sample_historical_data)
    
    def test_validate_deduplication_result_success(self, deduplicator, sample_historical_data):
        """Test successful deduplication result validation."""
        # Create a deduplicated DataFrame with fewer records
        deduplicated_df = sample_historical_data.limit(3)
        
        # Should not raise an exception
        deduplicator._validate_deduplication_result(sample_historical_data, deduplicated_df)
    
    def test_validate_deduplication_result_data_gain(self, deduplicator, sample_historical_data):
        """Test deduplication result validation with data gain (should not happen)."""
        # Create a DataFrame with more records (should not happen in deduplication)
        more_records_df = sample_historical_data.union(sample_historical_data)
        
        with pytest.raises(DeduplicationError):
            deduplicator._validate_deduplication_result(sample_historical_data, more_records_df)
    
    def test_validate_deduplication_result_complete_loss(self, deduplicator, sample_historical_data):
        """Test deduplication result validation with complete data loss."""
        # Create empty DataFrame
        empty_df = sample_historical_data.limit(0)
        
        with pytest.raises(DeduplicationError):
            deduplicator._validate_deduplication_result(sample_historical_data, empty_df)
    
    def test_deduplicate_historical_data_success(self, deduplicator, sample_historical_data):
        """Test successful historical data deduplication."""
        with patch.object(deduplicator, '_validate_input_data') as mock_validate:
            mock_validation_result = Mock()
            mock_validation_result.is_valid = True
            mock_validate.return_value = mock_validation_result
            
            result_df = deduplicator.deduplicate_historical_data(sample_historical_data)
            
            # Should return deduplicated DataFrame
            assert result_df.count() <= sample_historical_data.count()
            assert deduplicator.config.scd_hash_column in result_df.columns
    
    def test_deduplicate_historical_data_validation_failure(self, deduplicator, sample_historical_data):
        """Test historical data deduplication with validation failure."""
        with patch.object(deduplicator, '_validate_input_data') as mock_validate:
            mock_validation_result = Mock()
            mock_validation_result.is_valid = False
            mock_validation_result.errors = ["Validation failed"]
            mock_validate.return_value = mock_validation_result
            
            with pytest.raises(DeduplicationError):
                deduplicator.deduplicate_historical_data(sample_historical_data)
    
    def test_get_deduplication_stats(self, deduplicator, sample_historical_data):
        """Test getting deduplication statistics."""
        deduplicated_df = sample_historical_data.limit(3)
        
        stats = deduplicator.get_deduplication_stats(sample_historical_data, deduplicated_df)
        
        assert "original_records" in stats
        assert "deduplicated_records" in stats
        assert "duplicates_removed" in stats
        assert "deduplication_ratio" in stats
        assert "strategy_used" in stats
        
        assert stats["original_records"] == sample_historical_data.count()
        assert stats["deduplicated_records"] == deduplicated_df.count()
        assert stats["duplicates_removed"] == sample_historical_data.count() - deduplicated_df.count()
        assert stats["strategy_used"] == deduplicator.config.deduplication_strategy
