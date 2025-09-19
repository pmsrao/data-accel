"""
Date management utilities for SCD processing.
"""

from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, coalesce, lit, monotonically_increasing_id
import logging

from ..common.config import SCDConfig

logger = logging.getLogger(__name__)


class DateManager:
    """Manages date handling for SCD processing."""
    
    def __init__(self, config: SCDConfig):
        """
        Initialize DateManager with configuration.
        
        Args:
            config: SCD configuration
        """
        self.config = config
    
    def determine_effective_start(self, df: DataFrame, 
                                effective_from_col: Optional[str] = None,
                                initial_eff_date_col: Optional[str] = None) -> DataFrame:
        """
        Determine effective start date for records.
        
        Args:
            df: Input DataFrame
            effective_from_col: Column for effective start date
            initial_eff_date_col: Column for initial effective date
            
        Returns:
            DataFrame with effective start date determined
        """
        current_ts = current_timestamp()
        
        # DEBUG: Let's trace what's happening in determine_effective_start
        logger.info("🔍 DEBUG: determine_effective_start - analyzing input:")
        logger.info(f"🔍 DEBUG: df columns: {df.columns}")
        logger.info(f"🔍 DEBUG: effective_from_col: {effective_from_col}")
        logger.info(f"🔍 DEBUG: initial_eff_date_col: {initial_eff_date_col}")
        
        # Show sample data to understand what we're working with
        if df.count() > 0:
            logger.info("🔍 DEBUG: Sample input data:")
            df.select("customer_id", "last_modified_ts", "created_ts").show(10, False)
        
        # Priority: initial_eff_date_col > effective_from_col > current_timestamp
        if initial_eff_date_col and initial_eff_date_col in df.columns:
            effective_start = coalesce(
                col(initial_eff_date_col).cast("timestamp"),
                current_ts
            )
            logger.info(f"🔍 DEBUG: Using initial effective date column: {initial_eff_date_col}")
        elif effective_from_col and effective_from_col in df.columns:
            effective_start = coalesce(
                col(effective_from_col).cast("timestamp"),
                current_ts
            )
            logger.info(f"🔍 DEBUG: Using effective from column: {effective_from_col}")
        else:
            effective_start = current_ts
            logger.info("🔍 DEBUG: Using current timestamp as effective start date")
        
        result_df = df.withColumn(self.config.effective_start_column, effective_start)
        
        # DEBUG: Show the result
        logger.info("🔍 DEBUG: Sample result data after determine_effective_start:")
        result_df.select("customer_id", "effective_start_ts_utc", "last_modified_ts").show(10, False)
        
        return result_df
    
    def set_effective_end_date(self, df: DataFrame, end_date_value) -> DataFrame:
        """
        Set effective end date for records.
        
        Args:
            df: Input DataFrame
            end_date_value: Value for effective end date (can be column or literal)
            
        Returns:
            DataFrame with effective end date set
        """
        return df.withColumn(self.config.effective_end_column, end_date_value)
    
    def set_current_flag(self, df: DataFrame, is_current: str = "Y") -> DataFrame:
        """
        Set current flag for records.
        
        Args:
            df: Input DataFrame
            is_current: Value for current flag ("Y" or "N")
            
        Returns:
            DataFrame with current flag set
        """
        return df.withColumn(self.config.is_current_column, lit(is_current))
    
    def set_audit_timestamps(self, df: DataFrame) -> DataFrame:
        """
        Set audit timestamps for records.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with audit timestamps set
        """
        current_ts = current_timestamp()
        
        return (df
                .withColumn(self.config.created_ts_column, current_ts)
                .withColumn(self.config.modified_ts_column, current_ts))
    
    def convert_to_utc(self, df: DataFrame, date_columns: list, 
                      source_timezone: str) -> DataFrame:
        """
        Convert date columns to UTC.
        
        Args:
            df: Input DataFrame
            date_columns: List of date column names to convert
            source_timezone: Source timezone
            
        Returns:
            DataFrame with UTC converted columns
        """
        from ..common.utils import convert_to_utc
        return convert_to_utc(df, date_columns, source_timezone)
    
    def validate_date_consistency(self, df: DataFrame) -> list:
        """
        Validate date consistency in DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            List of validation errors
        """
        errors = []
        
        # Check if effective_start is before effective_end
        if (self.config.effective_start_column in df.columns and 
            self.config.effective_end_column in df.columns):
            
            invalid_dates = df.filter(
                col(self.config.effective_start_column) > col(self.config.effective_end_column)
            ).count()
            
            if invalid_dates > 0:
                errors.append(f"Found {invalid_dates} records with effective_start > effective_end")
        
        # Check for null effective_start dates
        if self.config.effective_start_column in df.columns:
            null_start_dates = df.filter(
                col(self.config.effective_start_column).isNull()
            ).count()
            
            if null_start_dates > 0:
                errors.append(f"Found {null_start_dates} records with null effective_start dates")
        
        return errors
    
    def get_date_columns(self) -> list:
        """
        Get list of date-related columns.
        
        Returns:
            List of date column names
        """
        return [
            self.config.effective_start_column,
            self.config.effective_end_column,
            self.config.created_ts_column,
            self.config.modified_ts_column
        ]
    
    def generate_surrogate_keys(self, df: DataFrame) -> DataFrame:
        """
        Generate surrogate keys for new records.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with surrogate keys added
        """
        from pyspark.sql.functions import col
        
        logger.info("Generating surrogate keys for new records")
        
        # Generate consistent surrogate keys using timestamp + row_number approach
        from pyspark.sql.functions import row_number, current_timestamp, concat, lit
        from pyspark.sql.window import Window
        
        # Create a window for row numbering (use first business key column for ordering)
        window = Window.orderBy(col(self.config.business_key_columns[0]))
        
        # Generate consistent surrogate keys: timestamp + row_number
        df_with_sk = df.withColumn(
            "row_num", row_number().over(window)
        ).withColumn(
            self.config.surrogate_key_column,
            concat(
                lit("SK_"),
                (current_timestamp().cast("bigint") * 1000 + col("row_num")).cast("string")
            )
        ).drop("row_num")
        
        logger.info(f"Generated surrogate keys for {df_with_sk.count()} records")
        return df_with_sk
