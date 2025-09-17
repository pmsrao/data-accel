"""
Unit tests for SCDValidator.
"""

import pytest
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from libraries.dimensional_processing.scd_type2.validators import SCDValidator
from libraries.dimensional_processing.common.config import SCDConfig, ValidationResult


class TestSCDValidator:
    """Test cases for SCDValidator."""
    
    @pytest.fixture
    def spark(self):
        """Create Spark session for testing."""
        return SparkSession.builder.appName("test").master("local[2]").getOrCreate()
    
    @pytest.fixture
    def scd_config(self):
        """Create SCD configuration for testing."""
        return SCDConfig(
            target_table="test.customer_dim",
            business_key_columns=["customer_id"],
            scd_columns=["name", "email"],
            effective_from_column="last_modified_ts",
            initial_effective_from_column="created_ts"
        )
    
    @pytest.fixture
    def sample_data(self, spark):
        """Create sample data for testing."""
        data = [
            ("1", "John Doe", "john@example.com", "2024-01-01 10:00:00", "2024-01-01 09:00:00"),
            ("2", "Jane Smith", "jane@example.com", "2024-01-01 11:00:00", "2024-01-01 10:00:00"),
            ("3", "Bob Johnson", "bob@example.com", "2024-01-01 12:00:00", "2024-01-01 11:00:00")
        ]
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("last_modified_ts", StringType(), True),
            StructField("created_ts", StringType(), True)
        ])
        
        return spark.createDataFrame(data, schema)
    
    @pytest.fixture
    def validator(self, scd_config):
        """Create SCDValidator instance for testing."""
        return SCDValidator(scd_config)
    
    def test_init(self, scd_config):
        """Test SCDValidator initialization."""
        validator = SCDValidator(scd_config)
        
        assert validator.config == scd_config
    
    def test_validate_source_data_success(self, validator, sample_data):
        """Test successful source data validation."""
        result = validator.validate_source_data(sample_data)
        
        assert result.is_valid is True
        assert len(result.errors) == 0
    
    def test_validate_source_data_missing_columns(self, validator, spark):
        """Test source data validation with missing columns."""
        data = [
            ("1", "John Doe", "john@example.com")
        ]
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True)
        ])
        
        incomplete_data = spark.createDataFrame(data, schema)
        result = validator.validate_source_data(incomplete_data)
        
        assert result.is_valid is False
        assert "Missing required columns" in result.errors[0]
    
    def test_validate_source_data_null_business_keys(self, validator, spark):
        """Test source data validation with null business keys."""
        data = [
            ("1", "John Doe", "john@example.com", "2024-01-01 10:00:00", "2024-01-01 09:00:00"),
            (None, "Jane Smith", "jane@example.com", "2024-01-01 11:00:00", "2024-01-01 10:00:00"),  # Null business key
            ("3", "Bob Johnson", "bob@example.com", "2024-01-01 12:00:00", "2024-01-01 11:00:00")
        ]
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("last_modified_ts", StringType(), True),
            StructField("created_ts", StringType(), True)
        ])
        
        df_with_nulls = spark.createDataFrame(data, schema)
        result = validator.validate_source_data(df_with_nulls)
        
        assert result.is_valid is False
        assert "null values in business key column" in result.errors[0]
    
    def test_validate_source_data_duplicates(self, validator, spark):
        """Test source data validation with duplicate records."""
        data = [
            ("1", "John Doe", "john@example.com", "2024-01-01 10:00:00", "2024-01-01 09:00:00"),
            ("1", "John Doe", "john@example.com", "2024-01-01 10:00:00", "2024-01-01 09:00:00"),  # Duplicate
            ("2", "Jane Smith", "jane@example.com", "2024-01-01 11:00:00", "2024-01-01 10:00:00")
        ]
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("last_modified_ts", StringType(), True),
            StructField("created_ts", StringType(), True)
        ])
        
        df_with_duplicates = spark.createDataFrame(data, schema)
        result = validator.validate_source_data(df_with_duplicates)
        
        assert result.is_valid is False
        assert "duplicate records" in result.errors[0]
    
    def test_validate_scd_metadata_success(self, validator, spark):
        """Test successful SCD metadata validation."""
        data = [
            ("1", "John Doe", "john@example.com", "hash123", "2024-01-01 10:00:00", None, "Y", "2024-01-01 10:00:00", "2024-01-01 10:00:00")
        ]
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("scd_hash", StringType(), True),
            StructField("effective_start_ts_utc", StringType(), True),
            StructField("effective_end_ts_utc", StringType(), True),
            StructField("is_current", StringType(), True),
            StructField("created_ts_utc", StringType(), True),
            StructField("modified_ts_utc", StringType(), True)
        ])
        
        df_with_metadata = spark.createDataFrame(data, schema)
        result = validator.validate_scd_metadata(df_with_metadata)
        
        assert result.is_valid is True
        assert len(result.errors) == 0
    
    def test_validate_scd_metadata_missing_columns(self, validator, spark):
        """Test SCD metadata validation with missing columns."""
        data = [
            ("1", "John Doe", "john@example.com")
        ]
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True)
        ])
        
        incomplete_data = spark.createDataFrame(data, schema)
        result = validator.validate_scd_metadata(incomplete_data)
        
        assert result.is_valid is False
        assert len(result.errors) > 0
        assert "Missing SCD hash column" in result.errors[0]
    
    def test_validate_scd_metadata_invalid_current_values(self, validator, spark):
        """Test SCD metadata validation with invalid is_current values."""
        data = [
            ("1", "John Doe", "john@example.com", "hash123", "2024-01-01 10:00:00", None, "INVALID", "2024-01-01 10:00:00", "2024-01-01 10:00:00")
        ]
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("scd_hash", StringType(), True),
            StructField("effective_start_ts_utc", StringType(), True),
            StructField("effective_end_ts_utc", StringType(), True),
            StructField("is_current", StringType(), True),
            StructField("created_ts_utc", StringType(), True),
            StructField("modified_ts_utc", StringType(), True)
        ])
        
        df_with_invalid_current = spark.createDataFrame(data, schema)
        result = validator.validate_scd_metadata(df_with_invalid_current)
        
        assert result.is_valid is False
        assert "invalid is_current values" in result.errors[0]
    
    def test_validate_processing_result_success(self, validator, sample_data):
        """Test successful processing result validation."""
        # Create processed data with same count
        processed_data = sample_data
        
        result = validator.validate_processing_result(sample_data, processed_data)
        
        assert result.is_valid is True
        assert len(result.errors) == 0
    
    def test_validate_processing_result_data_loss(self, validator, sample_data, spark):
        """Test processing result validation with data loss."""
        # Create processed data with fewer records
        processed_data = sample_data.limit(2)
        
        result = validator.validate_processing_result(sample_data, processed_data)
        
        assert result.is_valid is False
        assert "Data loss detected" in result.errors[0]
    
    def test_validate_processing_result_duplicate_current(self, validator, spark):
        """Test processing result validation with duplicate current records."""
        # Create processed data with duplicate current records
        data = [
            ("1", "John Doe", "john@example.com", "hash1", "2024-01-01 10:00:00", None, "Y", "2024-01-01 10:00:00", "2024-01-01 10:00:00"),
            ("1", "John Doe", "john@example.com", "hash2", "2024-01-01 11:00:00", None, "Y", "2024-01-01 11:00:00", "2024-01-01 11:00:00")  # Duplicate current
        ]
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("scd_hash", StringType(), True),
            StructField("effective_start_ts_utc", StringType(), True),
            StructField("effective_end_ts_utc", StringType(), True),
            StructField("is_current", StringType(), True),
            StructField("created_ts_utc", StringType(), True),
            StructField("modified_ts_utc", StringType(), True)
        ])
        
        processed_data = spark.createDataFrame(data, schema)
        result = validator.validate_processing_result(sample_data, processed_data)
        
        assert result.is_valid is False
        assert "duplicate current records" in result.errors[0]
    
    def test_add_error_flags_valid_data(self, validator, sample_data):
        """Test adding error flags to valid data."""
        validation_result = ValidationResult(is_valid=True)
        
        result_df = validator.add_error_flags(sample_data, validation_result)
        
        # Check that error flag columns are added
        assert validator.config.error_flag_column in result_df.columns
        assert validator.config.error_message_column in result_df.columns
        
        # Check that error flags are set to "N" for valid data
        error_flags = [row[validator.config.error_flag_column] for row in result_df.collect()]
        assert all(flag == "N" for flag in error_flags)
    
    def test_add_error_flags_invalid_data(self, validator, sample_data):
        """Test adding error flags to invalid data."""
        validation_result = ValidationResult(is_valid=False)
        validation_result.add_error("Test error message")
        
        result_df = validator.add_error_flags(sample_data, validation_result)
        
        # Check that error flag columns are added
        assert validator.config.error_flag_column in result_df.columns
        assert validator.config.error_message_column in result_df.columns
        
        # Check that error flags are set to "Y" for invalid data
        error_flags = [row[validator.config.error_flag_column] for row in result_df.collect()]
        assert all(flag == "Y" for flag in error_flags)
        
        # Check that error messages are set
        error_messages = [row[validator.config.error_message_column] for row in result_df.collect()]
        assert all(message == "Test error message" for message in error_messages)
    
    def test_validate_data_types_with_warnings(self, validator, spark):
        """Test data type validation with warnings."""
        data = [
            ("1", "John Doe", "john@example.com", "2024-01-01 10:00:00", "2024-01-01 09:00:00")
        ]
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("last_modified_ts", StringType(), True),  # String instead of timestamp
            StructField("created_ts", StringType(), True)  # String instead of timestamp
        ])
        
        df_with_string_dates = spark.createDataFrame(data, schema)
        result = validator.validate_source_data(df_with_string_dates)
        
        # Should be valid but with warnings
        assert result.is_valid is True
        assert len(result.warnings) > 0
        assert "expected timestamp or date" in result.warnings[0]
