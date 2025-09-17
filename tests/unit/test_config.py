"""
Unit tests for configuration classes.
"""

import pytest
from unittest.mock import Mock

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from libraries.dimensional_processing.common.config import (
    SCDConfig, 
    DeduplicationConfig, 
    KeyResolutionConfig,
    ProcessingMetrics,
    ValidationResult,
    DeduplicationStrategy
)


class TestSCDConfig:
    """Test cases for SCDConfig."""
    
    def test_init_success(self):
        """Test successful SCDConfig initialization."""
        config = SCDConfig(
            target_table="test.customer_dim",
            business_key_columns=["customer_id"],
            scd_columns=["name", "email"]
        )
        
        assert config.target_table == "test.customer_dim"
        assert config.business_key_columns == ["customer_id"]
        assert config.scd_columns == ["name", "email"]
        assert config.scd_hash_column == "scd_hash"
        assert config.effective_start_column == "effective_start_ts_utc"
        assert config.effective_end_column == "effective_end_ts_utc"
        assert config.is_current_column == "is_current"
        assert config.created_ts_column == "created_ts_utc"
        assert config.modified_ts_column == "modified_ts_utc"
        assert config.batch_size == 100000
        assert config.enable_optimization is True
        assert config.hash_algorithm == "sha256"
    
    def test_init_with_optional_parameters(self):
        """Test SCDConfig initialization with optional parameters."""
        config = SCDConfig(
            target_table="test.customer_dim",
            business_key_columns=["customer_id"],
            scd_columns=["name", "email"],
            effective_from_column="last_modified_ts",
            initial_effective_from_column="created_ts",
            batch_size=50000,
            enable_optimization=False,
            hash_algorithm="md5"
        )
        
        assert config.effective_from_column == "last_modified_ts"
        assert config.initial_effective_from_column == "created_ts"
        assert config.batch_size == 50000
        assert config.enable_optimization is False
        assert config.hash_algorithm == "md5"
    
    def test_init_empty_target_table(self):
        """Test SCDConfig initialization with empty target table."""
        with pytest.raises(ValueError, match="target_table is required"):
            SCDConfig(
                target_table="",
                business_key_columns=["customer_id"],
                scd_columns=["name", "email"]
            )
    
    def test_init_empty_business_key_columns(self):
        """Test SCDConfig initialization with empty business key columns."""
        with pytest.raises(ValueError, match="business_key_columns cannot be empty"):
            SCDConfig(
                target_table="test.customer_dim",
                business_key_columns=[],
                scd_columns=["name", "email"]
            )
    
    def test_init_empty_scd_columns(self):
        """Test SCDConfig initialization with empty SCD columns."""
        with pytest.raises(ValueError, match="scd_columns cannot be empty"):
            SCDConfig(
                target_table="test.customer_dim",
                business_key_columns=["customer_id"],
                scd_columns=[]
            )
    
    def test_init_invalid_batch_size(self):
        """Test SCDConfig initialization with invalid batch size."""
        with pytest.raises(ValueError, match="batch_size must be positive"):
            SCDConfig(
                target_table="test.customer_dim",
                business_key_columns=["customer_id"],
                scd_columns=["name", "email"],
                batch_size=0
            )


class TestDeduplicationConfig:
    """Test cases for DeduplicationConfig."""
    
    def test_init_success(self):
        """Test successful DeduplicationConfig initialization."""
        config = DeduplicationConfig(
            business_key_columns=["customer_id"],
            scd_columns=["name", "email"],
            effective_from_column="created_ts"
        )
        
        assert config.business_key_columns == ["customer_id"]
        assert config.scd_columns == ["name", "email"]
        assert config.effective_from_column == "created_ts"
        assert config.deduplication_strategy == "latest"
        assert config.scd_hash_column == "scd_hash"
        assert config.significance_column is None
        assert config.custom_deduplication_logic is None
        assert config.batch_size == 100000
        assert config.enable_optimization is True
    
    def test_init_with_optional_parameters(self):
        """Test DeduplicationConfig initialization with optional parameters."""
        def custom_logic(df):
            return df
        
        config = DeduplicationConfig(
            business_key_columns=["customer_id"],
            scd_columns=["name", "email"],
            effective_from_column="created_ts",
            deduplication_strategy="significant",
            significance_column="is_significant",
            custom_deduplication_logic=custom_logic,
            batch_size=50000,
            enable_optimization=False
        )
        
        assert config.deduplication_strategy == "significant"
        assert config.significance_column == "is_significant"
        assert config.custom_deduplication_logic == custom_logic
        assert config.batch_size == 50000
        assert config.enable_optimization is False
    
    def test_init_empty_business_key_columns(self):
        """Test DeduplicationConfig initialization with empty business key columns."""
        with pytest.raises(ValueError, match="business_key_columns cannot be empty"):
            DeduplicationConfig(
                business_key_columns=[],
                scd_columns=["name", "email"],
                effective_from_column="created_ts"
            )
    
    def test_init_empty_scd_columns(self):
        """Test DeduplicationConfig initialization with empty SCD columns."""
        with pytest.raises(ValueError, match="scd_columns cannot be empty"):
            DeduplicationConfig(
                business_key_columns=["customer_id"],
                scd_columns=[],
                effective_from_column="created_ts"
            )
    
    def test_init_empty_effective_from_column(self):
        """Test DeduplicationConfig initialization with empty effective from column."""
        with pytest.raises(ValueError, match="effective_from_column is required"):
            DeduplicationConfig(
                business_key_columns=["customer_id"],
                scd_columns=["name", "email"],
                effective_from_column=""
            )
    
    def test_init_invalid_deduplication_strategy(self):
        """Test DeduplicationConfig initialization with invalid deduplication strategy."""
        with pytest.raises(ValueError, match="deduplication_strategy must be one of"):
            DeduplicationConfig(
                business_key_columns=["customer_id"],
                scd_columns=["name", "email"],
                effective_from_column="created_ts",
                deduplication_strategy="invalid_strategy"
            )
    
    def test_init_significant_strategy_without_significance_column(self):
        """Test DeduplicationConfig initialization with significant strategy but no significance column."""
        with pytest.raises(ValueError, match="significance_column is required for 'significant' strategy"):
            DeduplicationConfig(
                business_key_columns=["customer_id"],
                scd_columns=["name", "email"],
                effective_from_column="created_ts",
                deduplication_strategy="significant"
            )
    
    def test_init_invalid_batch_size(self):
        """Test DeduplicationConfig initialization with invalid batch size."""
        with pytest.raises(ValueError, match="batch_size must be positive"):
            DeduplicationConfig(
                business_key_columns=["customer_id"],
                scd_columns=["name", "email"],
                effective_from_column="created_ts",
                batch_size=-1
            )


class TestKeyResolutionConfig:
    """Test cases for KeyResolutionConfig."""
    
    def test_init_success(self):
        """Test successful KeyResolutionConfig initialization."""
        config = KeyResolutionConfig(
            dimension_table="test.customer_dim",
            business_key_columns=["customer_id"]
        )
        
        assert config.dimension_table == "test.customer_dim"
        assert config.business_key_columns == ["customer_id"]
        assert config.surrogate_key_column == "surrogate_key"
        assert config.effective_start_column == "effective_start_ts_utc"
        assert config.effective_end_column == "effective_end_ts_utc"
        assert config.is_current_column == "is_current"
        assert config.enable_caching is True
        assert config.cache_ttl_minutes == 60
        assert config.batch_size == 100000
    
    def test_init_with_optional_parameters(self):
        """Test KeyResolutionConfig initialization with optional parameters."""
        config = KeyResolutionConfig(
            dimension_table="test.customer_dim",
            business_key_columns=["customer_id"],
            surrogate_key_column="customer_sk",
            effective_start_column="start_date",
            effective_end_column="end_date",
            is_current_column="current_flag",
            enable_caching=False,
            cache_ttl_minutes=120,
            batch_size=50000
        )
        
        assert config.surrogate_key_column == "customer_sk"
        assert config.effective_start_column == "start_date"
        assert config.effective_end_column == "end_date"
        assert config.is_current_column == "current_flag"
        assert config.enable_caching is False
        assert config.cache_ttl_minutes == 120
        assert config.batch_size == 50000
    
    def test_init_empty_dimension_table(self):
        """Test KeyResolutionConfig initialization with empty dimension table."""
        with pytest.raises(ValueError, match="dimension_table is required"):
            KeyResolutionConfig(
                dimension_table="",
                business_key_columns=["customer_id"]
            )
    
    def test_init_empty_business_key_columns(self):
        """Test KeyResolutionConfig initialization with empty business key columns."""
        with pytest.raises(ValueError, match="business_key_columns cannot be empty"):
            KeyResolutionConfig(
                dimension_table="test.customer_dim",
                business_key_columns=[]
            )
    
    def test_init_invalid_cache_ttl(self):
        """Test KeyResolutionConfig initialization with invalid cache TTL."""
        with pytest.raises(ValueError, match="cache_ttl_minutes must be positive"):
            KeyResolutionConfig(
                dimension_table="test.customer_dim",
                business_key_columns=["customer_id"],
                cache_ttl_minutes=0
            )
    
    def test_init_invalid_batch_size(self):
        """Test KeyResolutionConfig initialization with invalid batch size."""
        with pytest.raises(ValueError, match="batch_size must be positive"):
            KeyResolutionConfig(
                dimension_table="test.customer_dim",
                business_key_columns=["customer_id"],
                batch_size=-1
            )


class TestProcessingMetrics:
    """Test cases for ProcessingMetrics."""
    
    def test_init_default(self):
        """Test ProcessingMetrics initialization with default values."""
        metrics = ProcessingMetrics()
        
        assert metrics.records_processed == 0
        assert metrics.new_records_created == 0
        assert metrics.existing_records_updated == 0
        assert metrics.records_with_errors == 0
        assert metrics.processing_time_seconds == 0.0
        assert metrics.memory_usage_mb == 0.0
        assert metrics.partition_count == 0
    
    def test_init_with_values(self):
        """Test ProcessingMetrics initialization with values."""
        metrics = ProcessingMetrics(
            records_processed=1000,
            new_records_created=500,
            existing_records_updated=300,
            records_with_errors=10,
            processing_time_seconds=30.5,
            memory_usage_mb=1024.0,
            partition_count=4
        )
        
        assert metrics.records_processed == 1000
        assert metrics.new_records_created == 500
        assert metrics.existing_records_updated == 300
        assert metrics.records_with_errors == 10
        assert metrics.processing_time_seconds == 30.5
        assert metrics.memory_usage_mb == 1024.0
        assert metrics.partition_count == 4
    
    def test_to_dict(self):
        """Test converting ProcessingMetrics to dictionary."""
        metrics = ProcessingMetrics(
            records_processed=1000,
            new_records_created=500,
            existing_records_updated=300,
            records_with_errors=10,
            processing_time_seconds=30.5,
            memory_usage_mb=1024.0,
            partition_count=4
        )
        
        result = metrics.to_dict()
        
        expected = {
            "records_processed": 1000,
            "new_records_created": 500,
            "existing_records_updated": 300,
            "records_with_errors": 10,
            "processing_time_seconds": 30.5,
            "memory_usage_mb": 1024.0,
            "partition_count": 4
        }
        
        assert result == expected


class TestValidationResult:
    """Test cases for ValidationResult."""
    
    def test_init_valid(self):
        """Test ValidationResult initialization as valid."""
        result = ValidationResult(is_valid=True)
        
        assert result.is_valid is True
        assert result.errors == []
        assert result.warnings == []
    
    def test_init_invalid(self):
        """Test ValidationResult initialization as invalid."""
        result = ValidationResult(is_valid=False)
        
        assert result.is_valid is False
        assert result.errors == []
        assert result.warnings == []
    
    def test_add_error(self):
        """Test adding error to ValidationResult."""
        result = ValidationResult(is_valid=True)
        
        result.add_error("Test error")
        
        assert result.is_valid is False
        assert result.errors == ["Test error"]
        assert result.warnings == []
    
    def test_add_warning(self):
        """Test adding warning to ValidationResult."""
        result = ValidationResult(is_valid=True)
        
        result.add_warning("Test warning")
        
        assert result.is_valid is True
        assert result.errors == []
        assert result.warnings == ["Test warning"]
    
    def test_add_multiple_errors(self):
        """Test adding multiple errors to ValidationResult."""
        result = ValidationResult(is_valid=True)
        
        result.add_error("Error 1")
        result.add_error("Error 2")
        
        assert result.is_valid is False
        assert result.errors == ["Error 1", "Error 2"]
        assert result.warnings == []
    
    def test_add_multiple_warnings(self):
        """Test adding multiple warnings to ValidationResult."""
        result = ValidationResult(is_valid=True)
        
        result.add_warning("Warning 1")
        result.add_warning("Warning 2")
        
        assert result.is_valid is True
        assert result.errors == []
        assert result.warnings == ["Warning 1", "Warning 2"]
    
    def test_to_dict(self):
        """Test converting ValidationResult to dictionary."""
        result = ValidationResult(is_valid=False)
        result.add_error("Error 1")
        result.add_error("Error 2")
        result.add_warning("Warning 1")
        
        result_dict = result.to_dict()
        
        expected = {
            "is_valid": False,
            "errors": ["Error 1", "Error 2"],
            "warnings": ["Warning 1"]
        }
        
        assert result_dict == expected


class TestDeduplicationStrategy:
    """Test cases for DeduplicationStrategy enum."""
    
    def test_enum_values(self):
        """Test DeduplicationStrategy enum values."""
        assert DeduplicationStrategy.LATEST.value == "latest"
        assert DeduplicationStrategy.EARLIEST.value == "earliest"
        assert DeduplicationStrategy.SIGNIFICANT.value == "significant"
        assert DeduplicationStrategy.CUSTOM.value == "custom"
    
    def test_enum_membership(self):
        """Test DeduplicationStrategy enum membership."""
        assert "latest" in [strategy.value for strategy in DeduplicationStrategy]
        assert "earliest" in [strategy.value for strategy in DeduplicationStrategy]
        assert "significant" in [strategy.value for strategy in DeduplicationStrategy]
        assert "custom" in [strategy.value for strategy in DeduplicationStrategy]
