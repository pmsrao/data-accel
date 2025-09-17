"""
Unit tests for SCDProcessor with mocked Spark session.
This version avoids Java dependency issues for development.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import lit

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from libraries.dimensional_processing.scd_type2.scd_processor import SCDProcessor
from libraries.dimensional_processing.common.config import SCDConfig
from libraries.dimensional_processing.common.exceptions import SCDValidationError, SCDProcessingError


class TestSCDProcessorMocked:
    """Test cases for SCDProcessor with mocked Spark session."""
    
    @pytest.fixture
    def mock_spark(self):
        """Create mocked Spark session."""
        mock_spark = Mock()
        mock_spark.sql.return_value = Mock()
        return mock_spark
    
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
    def mock_sample_data(self, mock_spark):
        """Create mocked sample data."""
        mock_df = Mock()
        mock_df.columns = ["customer_id", "name", "email", "last_modified_ts", "created_ts"]
        mock_df.count.return_value = 3
        mock_df.isEmpty.return_value = False
        mock_df.filter.return_value = mock_df
        mock_df.withColumn.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.selectExpr.return_value = mock_df
        mock_df.drop.return_value = mock_df
        mock_df.unionByName.return_value = mock_df
        return mock_df
    
    @pytest.fixture
    def scd_processor(self, scd_config, mock_spark):
        """Create SCDProcessor instance with mocked Spark."""
        with patch('libraries.dimensional_processing.scd_type2.scd_processor.RecordManager') as mock_rm, \
             patch('libraries.dimensional_processing.scd_type2.scd_processor.HashManager') as mock_hm, \
             patch('libraries.dimensional_processing.scd_type2.scd_processor.DateManager') as mock_dm, \
             patch('libraries.dimensional_processing.scd_type2.scd_processor.SCDValidator') as mock_val:
            
            # Mock the components
            mock_rm.return_value = Mock()
            mock_hm.return_value = Mock()
            mock_dm.return_value = Mock()
            mock_val.return_value = Mock()
            
            processor = SCDProcessor(scd_config, mock_spark)
            return processor
    
    def test_init(self, scd_config, mock_spark):
        """Test SCDProcessor initialization."""
        with patch('libraries.dimensional_processing.scd_type2.scd_processor.RecordManager') as mock_rm, \
             patch('libraries.dimensional_processing.scd_type2.scd_processor.HashManager') as mock_hm, \
             patch('libraries.dimensional_processing.scd_type2.scd_processor.DateManager') as mock_dm, \
             patch('libraries.dimensional_processing.scd_type2.scd_processor.SCDValidator') as mock_val:
            
            # Mock the components
            mock_rm.return_value = Mock()
            mock_hm.return_value = Mock()
            mock_dm.return_value = Mock()
            mock_val.return_value = Mock()
            
            processor = SCDProcessor(scd_config, mock_spark)
            
            assert processor.config == scd_config
            assert processor.spark == mock_spark
            assert processor.hash_manager is not None
            assert processor.record_manager is not None
            assert processor.date_manager is not None
            assert processor.validator is not None
    
    def test_prepare_source_data(self, scd_processor, mock_sample_data):
        """Test source data preparation."""
        # Mock the hash manager
        scd_processor.hash_manager.compute_scd_hash.return_value = mock_sample_data
        scd_processor.date_manager.determine_effective_start.return_value = mock_sample_data
        scd_processor.date_manager.set_effective_end_date.return_value = mock_sample_data
        scd_processor.date_manager.set_current_flag.return_value = mock_sample_data
        scd_processor.date_manager.set_audit_timestamps.return_value = mock_sample_data
        
        prepared_df = scd_processor._prepare_source_data(mock_sample_data)
        
        # Verify that all preparation steps were called
        scd_processor.hash_manager.compute_scd_hash.assert_called_once_with(mock_sample_data)
        scd_processor.date_manager.determine_effective_start.assert_called_once()
        scd_processor.date_manager.set_effective_end_date.assert_called_once()
        scd_processor.date_manager.set_current_flag.assert_called_once()
        scd_processor.date_manager.set_audit_timestamps.assert_called_once()
        
        assert prepared_df == mock_sample_data
    
    def test_prepare_source_data_without_effective_from(self, scd_processor, mock_sample_data):
        """Test source data preparation without effective_from_column."""
        # Remove effective_from_column from config
        scd_processor.config.effective_from_column = None
        
        # Mock the hash manager
        scd_processor.hash_manager.compute_scd_hash.return_value = mock_sample_data
        scd_processor.date_manager.determine_effective_start.return_value = mock_sample_data
        scd_processor.date_manager.set_effective_end_date.return_value = mock_sample_data
        scd_processor.date_manager.set_current_flag.return_value = mock_sample_data
        scd_processor.date_manager.set_audit_timestamps.return_value = mock_sample_data
        
        prepared_df = scd_processor._prepare_source_data(mock_sample_data)
        
        # Should still work with current timestamp
        assert prepared_df == mock_sample_data
    
    @patch('libraries.dimensional_processing.scd_type2.scd_processor.add_error_columns')
    def test_process_scd_success(self, mock_add_error_columns, scd_processor, mock_sample_data):
        """Test successful SCD processing."""
        # Mock validation result
        mock_validation_result = Mock()
        mock_validation_result.is_valid = True
        scd_processor.validator.validate_source_data.return_value = mock_validation_result
        
        # Mock change plan and execution
        mock_change_plan = {
            "new_records": mock_sample_data,
            "unchanged_records": mock_sample_data,
            "changed_records": mock_sample_data
        }
        scd_processor.record_manager.create_change_plan.return_value = mock_change_plan
        
        mock_execution_result = Mock()
        mock_execution_result.records_processed = 3
        mock_execution_result.new_records_created = 3
        mock_execution_result.existing_records_updated = 0
        mock_execution_result.processing_time_seconds = 1.0
        scd_processor.record_manager.execute_change_plan.return_value = mock_execution_result
        
        # Mock add_error_columns
        mock_add_error_columns.return_value = mock_sample_data
        
        # Execute
        result = scd_processor.process_scd(mock_sample_data)
        
        # Verify
        assert result.records_processed == 3
        assert result.new_records_created == 3
        assert result.existing_records_updated == 0
        assert result.processing_time_seconds == 1.0
    
    def test_process_scd_validation_failure(self, scd_processor, mock_sample_data):
        """Test SCD processing with validation failure."""
        # Mock validation failure
        mock_validation_result = Mock()
        mock_validation_result.is_valid = False
        mock_validation_result.errors = ["Missing required column"]
        scd_processor.validator.validate_source_data.return_value = mock_validation_result
        
        # Should raise SCDValidationError
        with pytest.raises(SCDValidationError):
            scd_processor.process_scd(mock_sample_data)
    
    @patch('libraries.dimensional_processing.scd_type2.scd_processor.add_error_columns')
    def test_process_scd_execution_failure(self, mock_add_error_columns, scd_processor, mock_sample_data):
        """Test SCD processing with execution failure."""
        # Mock validation success
        mock_validation_result = Mock()
        mock_validation_result.is_valid = True
        scd_processor.validator.validate_source_data.return_value = mock_validation_result
        
        # Mock execution failure
        scd_processor.record_manager.execute_change_plan.side_effect = Exception("Execution failed")
        
        # Mock add_error_columns
        mock_add_error_columns.return_value = mock_sample_data
        
        # Should raise SCDProcessingError
        with pytest.raises(SCDProcessingError):
            scd_processor.process_scd(mock_sample_data)
    
    def test_process_incremental(self, scd_processor, mock_sample_data):
        """Test incremental processing."""
        with patch.object(scd_processor, 'process_scd') as mock_process:
            mock_result = Mock()
            mock_process.return_value = mock_result
            
            result = scd_processor.process_incremental(mock_sample_data, "2024-01-01 09:00:00")
            
            # Should call process_scd with filtered data
            mock_process.assert_called_once()
            assert result == mock_result
    
    def test_validate_table_schema_success(self, scd_processor):
        """Test successful table schema validation."""
        # Mock successful schema validation
        mock_schema_data = [
            {"col_name": "customer_id"},
            {"col_name": "name"},
            {"col_name": "email"},
            {"col_name": "scd_hash"},
            {"col_name": "effective_start_ts_utc"},
            {"col_name": "effective_end_ts_utc"},
            {"col_name": "is_current"},
            {"col_name": "created_ts_utc"},
            {"col_name": "modified_ts_utc"}
        ]
        
        mock_df = Mock()
        mock_df.collect.return_value = mock_schema_data
        scd_processor.spark.sql.return_value = mock_df
        
        result = scd_processor.validate_table_schema()
        assert result is True
    
    def test_validate_table_schema_failure(self, scd_processor):
        """Test table schema validation failure."""
        # Mock missing columns
        mock_schema_data = [
            {"col_name": "customer_id"},
            {"col_name": "name"},
            {"col_name": "email"}
        ]
        
        mock_df = Mock()
        mock_df.collect.return_value = mock_schema_data
        scd_processor.spark.sql.return_value = mock_df
        
        result = scd_processor.validate_table_schema()
        assert result is False
    
    def test_get_table_info(self, scd_processor):
        """Test getting table information."""
        mock_info = {
            "table_name": "test.customer_dim",
            "total_records": 1000,
            "current_records": 500,
            "historical_records": 500
        }
        scd_processor.record_manager.get_table_info.return_value = mock_info
        
        result = scd_processor.get_table_info()
        assert result == mock_info
    
    def test_create_target_table_if_not_exists(self, scd_processor):
        """Test creating target table if it doesn't exist."""
        schema = {
            "customer_id": "STRING",
            "name": "STRING",
            "email": "STRING"
        }
        
        # Mock table doesn't exist
        mock_df = Mock()
        mock_df.collect.return_value = [{"count": 0}]
        scd_processor.spark.sql.return_value = mock_df
        
        scd_processor.create_target_table_if_not_exists(schema)
        
        # Should call CREATE TABLE
        assert scd_processor.spark.sql.call_count >= 2  # DESCRIBE + CREATE TABLE
    
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
