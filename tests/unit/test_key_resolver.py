"""
Unit tests for DimensionalKeyResolver.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from libraries.dimensional_processing.key_resolution.key_resolver import DimensionalKeyResolver
from libraries.dimensional_processing.common.config import KeyResolutionConfig
from libraries.dimensional_processing.common.exceptions import KeyResolutionError


class TestDimensionalKeyResolver:
    """Test cases for DimensionalKeyResolver."""
    
    @pytest.fixture
    def spark(self):
        """Create Spark session for testing."""
        return SparkSession.builder.appName("test").master("local[2]").getOrCreate()
    
    @pytest.fixture
    def key_config(self):
        """Create key resolution configuration for testing."""
        return KeyResolutionConfig(
            dimension_table="test.customer_dim",
            business_key_columns=["customer_id"],
            surrogate_key_column="customer_sk"
        )
    
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
    
    @pytest.fixture
    def sample_dimension_data(self, spark):
        """Create sample dimension data for testing."""
        data = [
            ("1", 1001, "2024-01-01 09:00:00", None, "Y"),
            ("2", 1002, "2024-01-01 10:00:00", None, "Y"),
            ("3", 1003, "2024-01-01 11:00:00", None, "Y")
        ]
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("customer_sk", StringType(), True),
            StructField("effective_start_ts_utc", StringType(), True),
            StructField("effective_end_ts_utc", StringType(), True),
            StructField("is_current", StringType(), True)
        ])
        
        return spark.createDataFrame(data, schema)
    
    @pytest.fixture
    def key_resolver(self, key_config, spark):
        """Create DimensionalKeyResolver instance for testing."""
        return DimensionalKeyResolver(key_config, spark)
    
    def test_init(self, key_config, spark):
        """Test DimensionalKeyResolver initialization."""
        resolver = DimensionalKeyResolver(key_config, spark)
        
        assert resolver.config == key_config
        assert resolver.spark == spark
        assert resolver.lookup_manager is not None
        assert resolver.cache_manager is not None
    
    def test_validate_input_success(self, key_resolver, sample_fact_data):
        """Test successful input validation."""
        # Should not raise an exception
        key_resolver._validate_input(sample_fact_data, "transaction_date")
    
    def test_validate_input_missing_business_date_column(self, key_resolver, sample_fact_data):
        """Test input validation with missing business date column."""
        with pytest.raises(KeyResolutionError):
            key_resolver._validate_input(sample_fact_data, "invalid_date_column")
    
    def test_validate_input_missing_business_key_columns(self, key_resolver, spark):
        """Test input validation with missing business key columns."""
        data = [
            ("1", "2024-01-01 10:00:00", 100.0)
        ]
        
        schema = StructType([
            StructField("invalid_id", StringType(), True),  # Wrong column name
            StructField("transaction_date", StringType(), True),
            StructField("amount", StringType(), True)
        ])
        
        invalid_fact_data = spark.createDataFrame(data, schema)
        
        with pytest.raises(KeyResolutionError):
            key_resolver._validate_input(invalid_fact_data, "transaction_date")
    
    def test_determine_resolution_strategy_current_only(self, key_resolver, spark):
        """Test determining resolution strategy for current-only data."""
        # Create data with only current dates
        data = [
            ("1", "2024-12-31 10:00:00", 100.0),  # Future date
            ("2", "2024-12-31 11:00:00", 200.0)
        ]
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("transaction_date", StringType(), True),
            StructField("amount", StringType(), True)
        ])
        
        current_fact_data = spark.createDataFrame(data, schema)
        strategy = key_resolver._determine_resolution_strategy(current_fact_data, "transaction_date")
        
        assert strategy == "current_only"
    
    def test_determine_resolution_strategy_historical_only(self, key_resolver, spark):
        """Test determining resolution strategy for historical-only data."""
        # Create data with only historical dates
        data = [
            ("1", "2020-01-01 10:00:00", 100.0),  # Past date
            ("2", "2020-01-01 11:00:00", 200.0)
        ]
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("transaction_date", StringType(), True),
            StructField("amount", StringType(), True)
        ])
        
        historical_fact_data = spark.createDataFrame(data, schema)
        strategy = key_resolver._determine_resolution_strategy(historical_fact_data, "transaction_date")
        
        assert strategy == "historical_only"
    
    def test_determine_resolution_strategy_mixed(self, key_resolver, sample_fact_data):
        """Test determining resolution strategy for mixed data."""
        strategy = key_resolver._determine_resolution_strategy(sample_fact_data, "transaction_date")
        
        assert strategy == "mixed"
    
    @patch('libraries.dimensional_processing.key_resolution.key_resolver.LookupManager')
    def test_resolve_current_only(self, mock_lookup_manager, key_resolver, sample_fact_data):
        """Test resolving keys for current-only data."""
        # Mock the lookup manager
        mock_manager_instance = Mock()
        mock_lookup_manager.return_value = mock_manager_instance
        
        # Mock current dimension data
        mock_dimension_df = Mock()
        mock_manager_instance.get_current_dimension_records.return_value = mock_dimension_df
        
        # Mock resolved data
        mock_resolved_df = Mock()
        mock_manager_instance.resolve_current_keys.return_value = mock_resolved_df
        
        # Mock cache
        key_resolver.cache_manager.get_cached_records.return_value = None
        key_resolver.cache_manager.cache_records.return_value = None
        
        result = key_resolver._resolve_current_only(sample_fact_data)
        
        assert result == mock_resolved_df
        mock_manager_instance.get_current_dimension_records.assert_called_once()
        mock_manager_instance.resolve_current_keys.assert_called_once()
    
    @patch('libraries.dimensional_processing.key_resolution.key_resolver.LookupManager')
    def test_resolve_historical_only(self, mock_lookup_manager, key_resolver, sample_fact_data):
        """Test resolving keys for historical-only data."""
        # Mock the lookup manager
        mock_manager_instance = Mock()
        mock_lookup_manager.return_value = mock_manager_instance
        
        # Mock resolved data
        mock_resolved_df = Mock()
        mock_manager_instance.resolve_historical_keys.return_value = mock_resolved_df
        
        result = key_resolver._resolve_historical_only(sample_fact_data, "transaction_date")
        
        assert result == mock_resolved_df
        mock_manager_instance.resolve_historical_keys.assert_called_once()
    
    @patch('libraries.dimensional_processing.key_resolution.key_resolver.LookupManager')
    def test_resolve_mixed(self, mock_lookup_manager, key_resolver, sample_fact_data):
        """Test resolving keys for mixed data."""
        # Mock the lookup manager
        mock_manager_instance = Mock()
        mock_lookup_manager.return_value = mock_manager_instance
        
        # Mock resolved data
        mock_resolved_df = Mock()
        mock_manager_instance.resolve_mixed_keys.return_value = mock_resolved_df
        
        result = key_resolver._resolve_mixed(sample_fact_data, "transaction_date")
        
        assert result == mock_resolved_df
        mock_manager_instance.resolve_mixed_keys.assert_called_once()
    
    @patch('libraries.dimensional_processing.key_resolution.key_resolver.LookupManager')
    def test_resolve_keys_success(self, mock_lookup_manager, key_resolver, sample_fact_data):
        """Test successful key resolution."""
        # Mock the lookup manager
        mock_manager_instance = Mock()
        mock_lookup_manager.return_value = mock_manager_instance
        
        # Mock resolved data
        mock_resolved_df = Mock()
        mock_resolved_df.count.return_value = 3
        
        # Mock validation results
        mock_validation_results = {
            "original_records": 3,
            "resolved_records": 3,
            "unresolved_records": 0,
            "resolution_rate": 1.0,
            "data_loss": False
        }
        mock_manager_instance.validate_resolution_results.return_value = mock_validation_results
        
        # Mock strategy determination and resolution
        key_resolver._determine_resolution_strategy = Mock(return_value="current_only")
        key_resolver._resolve_current_only = Mock(return_value=mock_resolved_df)
        
        result = key_resolver.resolve_keys(sample_fact_data, "transaction_date")
        
        assert result == mock_resolved_df
        mock_manager_instance.validate_resolution_results.assert_called_once()
    
    @patch('libraries.dimensional_processing.key_resolution.key_resolver.LookupManager')
    def test_resolve_keys_validation_failure(self, mock_lookup_manager, key_resolver, sample_fact_data):
        """Test key resolution with validation failure."""
        # Mock the lookup manager
        mock_manager_instance = Mock()
        mock_lookup_manager.return_value = mock_manager_instance
        
        # Mock resolved data
        mock_resolved_df = Mock()
        mock_resolved_df.count.return_value = 2  # Data loss
        
        # Mock validation results with data loss
        mock_validation_results = {
            "original_records": 3,
            "resolved_records": 2,
            "unresolved_records": 1,
            "resolution_rate": 0.67,
            "data_loss": True
        }
        mock_manager_instance.validate_resolution_results.return_value = mock_validation_results
        
        # Mock strategy determination and resolution
        key_resolver._determine_resolution_strategy = Mock(return_value="current_only")
        key_resolver._resolve_current_only = Mock(return_value=mock_resolved_df)
        
        # Should raise KeyResolutionError due to data loss
        with pytest.raises(KeyResolutionError):
            key_resolver.resolve_keys(sample_fact_data, "transaction_date")
    
    def test_resolve_keys_input_validation_failure(self, key_resolver, sample_fact_data):
        """Test key resolution with input validation failure."""
        # Should raise KeyResolutionError for invalid input
        with pytest.raises(KeyResolutionError):
            key_resolver.resolve_keys(sample_fact_data, "invalid_date_column")
    
    @patch('libraries.dimensional_processing.key_resolution.key_resolver.LookupManager')
    def test_get_resolution_stats(self, mock_lookup_manager, key_resolver, sample_fact_data):
        """Test getting resolution statistics."""
        # Mock the lookup manager
        mock_manager_instance = Mock()
        mock_lookup_manager.return_value = mock_manager_instance
        
        # Mock resolved data
        mock_resolved_df = Mock()
        
        # Mock validation results
        mock_validation_results = {
            "original_records": 3,
            "resolved_records": 3,
            "unresolved_records": 0,
            "resolution_rate": 1.0,
            "data_loss": False
        }
        mock_manager_instance.validate_resolution_results.return_value = mock_validation_results
        
        # Mock cache and dimension info
        key_resolver.cache_manager.get_cache_stats.return_value = {"cache_enabled": True}
        mock_manager_instance.get_dimension_info.return_value = {"total_records": 100}
        
        stats = key_resolver.get_resolution_stats(sample_fact_data, mock_resolved_df)
        
        assert "resolution_stats" in stats
        assert "cache_stats" in stats
        assert "dimension_info" in stats
        assert stats["resolution_stats"] == mock_validation_results
    
    def test_clear_cache(self, key_resolver):
        """Test clearing cache."""
        key_resolver.cache_manager.clear_cache = Mock()
        
        key_resolver.clear_cache()
        
        key_resolver.cache_manager.clear_cache.assert_called_once()
    
    def test_get_cache_stats(self, key_resolver):
        """Test getting cache statistics."""
        mock_cache_stats = {"cache_enabled": True, "total_entries": 5}
        key_resolver.cache_manager.get_cache_stats.return_value = mock_cache_stats
        
        stats = key_resolver.get_cache_stats()
        
        assert stats == mock_cache_stats
    
    @patch('libraries.dimensional_processing.key_resolution.key_resolver.LookupManager')
    def test_batch_resolve_keys_small_dataset(self, mock_lookup_manager, key_resolver, sample_fact_data):
        """Test batch key resolution with small dataset."""
        # Mock the lookup manager
        mock_manager_instance = Mock()
        mock_lookup_manager.return_value = mock_manager_instance
        
        # Mock resolved data
        mock_resolved_df = Mock()
        mock_resolved_df.count.return_value = 3
        
        # Mock validation results
        mock_validation_results = {
            "original_records": 3,
            "resolved_records": 3,
            "unresolved_records": 0,
            "resolution_rate": 1.0,
            "data_loss": False
        }
        mock_manager_instance.validate_resolution_results.return_value = mock_validation_results
        
        # Mock strategy determination and resolution
        key_resolver._determine_resolution_strategy = Mock(return_value="current_only")
        key_resolver._resolve_current_only = Mock(return_value=mock_resolved_df)
        
        # Should call regular resolve_keys for small dataset
        result = key_resolver.batch_resolve_keys(sample_fact_data, "transaction_date", batch_size=1000)
        
        assert result == mock_resolved_df
    
    @patch('libraries.dimensional_processing.key_resolution.key_resolver.LookupManager')
    def test_batch_resolve_keys_large_dataset(self, mock_lookup_manager, key_resolver, spark):
        """Test batch key resolution with large dataset."""
        # Create large dataset
        data = []
        for i in range(1500):  # Larger than batch size
            data.append((str(i), "2024-01-01 10:00:00", 100.0))
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("transaction_date", StringType(), True),
            StructField("amount", StringType(), True)
        ])
        
        large_fact_data = spark.createDataFrame(data, schema)
        
        # Mock the lookup manager
        mock_manager_instance = Mock()
        mock_lookup_manager.return_value = mock_manager_instance
        
        # Mock resolved data
        mock_resolved_df = Mock()
        mock_resolved_df.count.return_value = 1000
        
        # Mock validation results
        mock_validation_results = {
            "original_records": 1000,
            "resolved_records": 1000,
            "unresolved_records": 0,
            "resolution_rate": 1.0,
            "data_loss": False
        }
        mock_manager_instance.validate_resolution_results.return_value = mock_validation_results
        
        # Mock strategy determination and resolution
        key_resolver._determine_resolution_strategy = Mock(return_value="current_only")
        key_resolver._resolve_current_only = Mock(return_value=mock_resolved_df)
        
        # Should use batch processing
        result = key_resolver.batch_resolve_keys(large_fact_data, "transaction_date", batch_size=1000)
        
        # Should return a DataFrame (mocked)
        assert result is not None
