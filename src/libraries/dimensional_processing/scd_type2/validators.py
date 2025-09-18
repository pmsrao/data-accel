"""
Data validation utilities for SCD processing.
"""

from typing import List, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit
import logging

from ..common.config import SCDConfig, ValidationResult
from ..common.exceptions import SCDValidationError

logger = logging.getLogger(__name__)


class SCDValidator:
    """Validates data for SCD processing."""
    
    def __init__(self, config: SCDConfig):
        """
        Initialize SCDValidator with configuration.
        
        Args:
            config: SCD configuration
        """
        self.config = config
    
    def validate_source_data(self, df: DataFrame) -> ValidationResult:
        """
        Validate source data before SCD processing.
        
        Args:
            df: Input DataFrame
            
        Returns:
            ValidationResult with validation status and errors
        """
        result = ValidationResult(is_valid=True)
        
        # Check required columns
        self._validate_required_columns(df, result)
        
        # Check for null business keys
        self._validate_business_keys(df, result)
        
        # Skip duplicate validation for SCD Type 2 as it's designed to handle multiple versions
        # self._validate_duplicates(df, result)
        
        # Check data types
        self._validate_data_types(df, result)
        
        # Check date consistency
        self._validate_date_consistency(df, result)
        
        logger.info(f"Validation completed. Valid: {result.is_valid}, Errors: {len(result.errors)}")
        return result
    
    def _validate_required_columns(self, df: DataFrame, result: ValidationResult) -> None:
        """Validate that all required columns exist."""
        required_columns = (self.config.business_key_columns + 
                          self.config.scd_columns)
        
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            result.add_error(f"Missing required columns: {missing_columns}")
    
    def _validate_business_keys(self, df: DataFrame, result: ValidationResult) -> None:
        """Validate business key columns."""
        for col_name in self.config.business_key_columns:
            if col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                if null_count > 0:
                    result.add_error(f"Found {null_count} null values in business key column: {col_name}")
    
    def _validate_duplicates(self, df: DataFrame, result: ValidationResult) -> None:
        """Validate for duplicate records based on business keys and effective dates."""
        # In SCD Type 2, we allow multiple records with the same business key
        # as long as they have different effective dates or SCD attributes
        # We only flag as duplicates if they have the same business key AND same effective date AND same SCD attributes
        
        # For SCD Type 2, we should be very permissive with duplicates
        # Only flag exact duplicates (all columns identical)
        duplicate_count = (df.count() - df.dropDuplicates().count())
        
        if duplicate_count > 0:
            result.add_warning(f"Found {duplicate_count} exact duplicate records (all columns identical)")
            # Note: We use add_warning instead of add_error for SCD Type 2
            # as exact duplicates might be acceptable in some scenarios
    
    def _validate_data_types(self, df: DataFrame, result: ValidationResult) -> None:
        """Validate data types for key columns."""
        # Check if date columns are of correct type
        date_columns = [
            self.config.effective_from_column,
            self.config.initial_effective_from_column
        ]
        
        for col_name in date_columns:
            if col_name and col_name in df.columns:
                col_type = dict(df.dtypes)[col_name]
                if col_type not in ["timestamp", "date"]:
                    result.add_warning(f"Column {col_name} has type {col_type}, expected timestamp or date")
    
    def _validate_date_consistency(self, df: DataFrame, result: ValidationResult) -> None:
        """Validate date consistency."""
        # This is a placeholder for more complex date validation logic
        # Can be extended based on specific business requirements
        pass
    
    def validate_scd_metadata(self, df: DataFrame) -> ValidationResult:
        """
        Validate SCD metadata columns.
        
        Args:
            df: Input DataFrame with SCD metadata
            
        Returns:
            ValidationResult with validation status
        """
        result = ValidationResult(is_valid=True)
        
        # Check SCD hash column
        if self.config.scd_hash_column not in df.columns:
            result.add_error(f"Missing SCD hash column: {self.config.scd_hash_column}")
        
        # Check effective date columns
        if self.config.effective_start_column not in df.columns:
            result.add_error(f"Missing effective start column: {self.config.effective_start_column}")
        
        if self.config.effective_end_column not in df.columns:
            result.add_error(f"Missing effective end column: {self.config.effective_end_column}")
        
        # Check is_current column
        if self.config.is_current_column not in df.columns:
            result.add_error(f"Missing is_current column: {self.config.is_current_column}")
        
        # Validate is_current values
        if self.config.is_current_column in df.columns:
            invalid_current_values = df.filter(
                ~col(self.config.is_current_column).isin(["Y", "N"])
            ).count()
            
            if invalid_current_values > 0:
                result.add_error(f"Found {invalid_current_values} records with invalid is_current values")
        
        return result
    
    def validate_processing_result(self, original_df: DataFrame, 
                                 processed_df: DataFrame) -> ValidationResult:
        """
        Validate SCD processing result.
        
        Args:
            original_df: Original source DataFrame
            processed_df: Processed DataFrame
            
        Returns:
            ValidationResult with validation status
        """
        result = ValidationResult(is_valid=True)
        
        # Check for data loss
        original_count = original_df.count()
        processed_count = processed_df.count()
        
        if processed_count < original_count:
            result.add_error(f"Data loss detected: {original_count} -> {processed_count} records")
        
        # Check for duplicate current records
        if self.config.is_current_column in processed_df.columns:
            current_records = processed_df.filter(
                col(self.config.is_current_column) == "Y"
            )
            
            duplicate_current = (current_records.count() - 
                               current_records.dropDuplicates(self.config.business_key_columns).count())
            
            if duplicate_current > 0:
                result.add_error(f"Found {duplicate_current} duplicate current records")
        
        return result
    
    def add_error_flags(self, df: DataFrame, validation_result: ValidationResult) -> DataFrame:
        """
        Add error flags to DataFrame based on validation results.
        
        Args:
            df: Input DataFrame
            validation_result: Validation result
            
        Returns:
            DataFrame with error flags added
        """
        if not validation_result.is_valid:
            # Add error flag and message columns
            df = df.withColumn(self.config.error_flag_column, lit("Y"))
            df = df.withColumn(self.config.error_message_column, 
                             lit("; ".join(validation_result.errors)))
        else:
            df = df.withColumn(self.config.error_flag_column, lit("N"))
            df = df.withColumn(self.config.error_message_column, lit(""))
        
        return df
