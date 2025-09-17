"""
Unit tests for SCDProcessor.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import lit

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from libraries.dimensional_processing.scd_type2.scd_processor import SCDProcessor
from libraries.dimensional_processing.common.config import SCDConfig
from libraries.dimensional_processing.common.exceptions import SCDValidationError, SCDProcessingError


class TestSCDProcessor:
    """Test cases for SCDProcessor."""
    
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
    def scd_processor(self, scd_config, spark):
        """Create SCDProcessor instance for testing."""
        return SCDProcessor(scd_config, spark)
    
    def test_init(self, scd_config, spark):
        """Test SCDProcessor initialization."""
        processor = SCDProcessor(scd_config, spark)
        
        assert processor.config == scd_config
        assert processor.spark == spark
        assert processor.hash_manager is not None
        assert processor.record_manager is not None
        assert processor.date_manager is not None
        assert processor.validator is not None
    
    def test_prepare_source_data(self, scd_processor, sample_data):
        """Test source data preparation."""
        prepared_df = scd_processor._prepare_source_data(sample_data)
        
        # Check that SCD metadata columns are added
        assert scd_processor.config.scd_hash_column in prepared_df.columns
        assert scd_processor.config.effective_start_column in prepared_df.columns
        assert scd_processor.config.effective_end_column in prepared_df.columns
        assert scd_processor.config.is_current_column in prepared_df.columns
        assert scd_processor.config.created_ts_column in prepared_df.columns
        assert scd_processor.config.modified_ts_column in prepared_df.columns
        
        # Check that all original records are preserved
        assert prepared_df.count() == sample_data.count()
    
    def test_prepare_source_data_without_effective_from(self, scd_processor, sample_data):
        """Test source data preparation without effective_from_column."""
        # Remove effective_from_column from config
        scd_processor.config.effective_from_column = None
        
        prepared_df = scd_processor._prepare_source_data(sample_data)
        
        # Should still work with current timestamp
        assert prepared_df.count() == sample_data.count()
        assert scd_processor.config.effective_start_column in prepared_df.columns
    
    def test_prepare_source_data_without_initial_effective_from(self, scd_processor, sample_data):
        """Test source data preparation without initial_effective_from_column."""
        # Remove initial_effective_from_column from config
        scd_processor.config.initial_effective_from_column = None
        
        prepared_df = scd_processor._prepare_source_data(sample_data)
        
        # Should fall back to effective_from_column
        assert prepared_df.count() == sample_data.count()
        assert scd_processor.config.effective_start_column in prepared_df.columns
    
    @patch('libraries.dimensional_processing.scd_type2.scd_processor.RecordManager')
    def test_process_scd_success(self, mock_record_manager, scd_processor, sample_data):
        """Test successful SCD processing."""
        # Mock the record manager
        mock_manager_instance = Mock()
        mock_record_manager.return_value = mock_manager_instance
        
        # Mock validation result
        mock_validation_result = Mock()
        mock_validation_result.is_valid = True
        scd_processor.validator.validate_source_data.return_value = mock_validation_result
        
        # Mock change plan and execution
        mock_change_plan = {
            "new_records": sample_data,
            "unchanged_records": sample_data.limit(0),
            "changed_records": sample_data.limit(0)
        }
        mock_manager_instance.create_change_plan.return_value = mock_change_plan
        
        mock_execution_result = Mock()
        mock_execution_result.records_processed = 3
        mock_execution_result.new_records_created = 3
        mock_execution_result.existing_records_updated = 0
        mock_execution_result.processing_time_seconds = 1.0
        mock_manager_instance.execute_change_plan.return_value = mock_execution_result
        
        # Execute
        result = scd_processor.process_scd(sample_data)
        
        # Verify
        assert result.records_processed == 3
        assert result.new_records_created == 3
        assert result.existing_records_updated == 0
        assert result.processing_time_seconds == 1.0
    
    def test_process_scd_validation_failure(self, scd_processor, sample_data):
        """Test SCD processing with validation failure."""
        # Mock validation failure
        mock_validation_result = Mock()
        mock_validation_result.is_valid = False
        mock_validation_result.errors = ["Missing required column"]
        scd_processor.validator.validate_source_data.return_value = mock_validation_result
        
        # Should raise SCDValidationError
        with pytest.raises(SCDValidationError):
            scd_processor.process_scd(sample_data)
    
    @patch('libraries.dimensional_processing.scd_type2.scd_processor.RecordManager')
    def test_process_scd_execution_failure(self, mock_record_manager, scd_processor, sample_data):
        """Test SCD processing with execution failure."""
        # Mock the record manager
        mock_manager_instance = Mock()
        mock_record_manager.return_value = mock_manager_instance
        
        # Mock validation success
        mock_validation_result = Mock()
        mock_validation_result.is_valid = True
        scd_processor.validator.validate_source_data.return_value = mock_validation_result
        
        # Mock execution failure
        mock_manager_instance.execute_change_plan.side_effect = Exception("Execution failed")
        
        # Should raise SCDProcessingError
        with pytest.raises(SCDProcessingError):
            scd_processor.process_scd(sample_data)
    
    def test_process_incremental(self, scd_processor, sample_data):
        """Test incremental processing."""
        with patch.object(scd_processor, 'process_scd') as mock_process:
            mock_result = Mock()
            mock_process.return_value = mock_result
            
            result = scd_processor.process_incremental(sample_data, "2024-01-01 09:00:00")
            
            # Should call process_scd with filtered data
            mock_process.assert_called_once()
            assert result == mock_result
    
    def test_validate_table_schema_success(self, scd_processor):
        """Test successful table schema validation."""
        # Mock successful schema validation
        mock_schema_data = [
            ("customer_id", "string", ""),
            ("name", "string", ""),
            ("email", "string", ""),
            ("scd_hash", "string", ""),
            ("effective_start_ts_utc", "timestamp", ""),
            ("effective_end_ts_utc", "timestamp", ""),
            ("is_current", "string", ""),
            ("created_ts_utc", "timestamp", ""),
            ("modified_ts_utc", "timestamp", "")
        ]
        
        with patch.object(scd_processor.spark, 'sql') as mock_sql:
            mock_df = Mock()
            mock_df.collect.return_value = [{"col_name": col[0]} for col in mock_schema_data]
            mock_sql.return_value = mock_df
            
            result = scd_processor.validate_table_schema()
            assert result is True
    
    def test_validate_table_schema_failure(self, scd_processor):
        """Test table schema validation failure."""
        # Mock missing columns
        mock_schema_data = [
            ("customer_id", "string", ""),
            ("name", "string", ""),
            ("email", "string", "")
        ]
        
        with patch.object(scd_processor.spark, 'sql') as mock_sql:
            mock_df = Mock()
            mock_df.collect.return_value = [{"col_name": col[0]} for col in mock_schema_data]
            mock_sql.return_value = mock_df
            
            result = scd_processor.validate_table_schema()
            assert result is False
    
    def test_get_table_info(self, scd_processor):
        """Test getting table information."""
        with patch.object(scd_processor.record_manager, 'get_table_info') as mock_get_info:
            mock_info = {
                "table_name": "test.customer_dim",
                "total_records": 1000,
                "current_records": 500,
                "historical_records": 500
            }
            mock_get_info.return_value = mock_info
            
            result = scd_processor.get_table_info()
            assert result == mock_info
    
    def test_create_target_table_if_not_exists(self, scd_processor):
        """Test creating target table if it doesn't exist."""
        schema = {
            "customer_id": "STRING",
            "name": "STRING",
            "email": "STRING"
        }
        
        with patch.object(scd_processor.spark, 'sql') as mock_sql:
            # Mock table doesn't exist
            mock_df = Mock()
            mock_df.collect.return_value = [{"count": 0}]
            mock_sql.return_value = mock_df
            
            scd_processor.create_target_table_if_not_exists(schema)
            
            # Should call CREATE TABLE
            assert mock_sql.call_count >= 2  # DESCRIBE + CREATE TABLE
    
    def test_build_create_table_sql(self, scd_processor):
        """Test building CREATE TABLE SQL."""
        schema = {
            "customer_id": "STRING",
            "name": "STRING",
            "email": "STRING"
        }
        
        sql = scd_processor._build_create_table_sql(schema)
        
        assert "CREATE TABLE IF NOT EXISTS" in sql
        assert scd_processor.config.target_table in sql
        assert "USING DELTA" in sql
        assert "customer_id STRING" in sql
        assert "scd_hash STRING" in sql
        assert "effective_start_ts_utc TIMESTAMP" in sql
