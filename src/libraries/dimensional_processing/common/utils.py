"""
Utility functions for dimensional processing library.
"""

from typing import List, Dict, Any, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_utc_timestamp, to_utc_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import logging

logger = logging.getLogger(__name__)


def convert_to_utc(df: DataFrame, date_columns: List[str], source_timezone: str) -> DataFrame:
    """
    Convert date columns from source timezone to UTC.
    
    Args:
        df: Input DataFrame
        date_columns: List of date column names to convert
        source_timezone: Source timezone (e.g., 'America/New_York')
        
    Returns:
        DataFrame with UTC converted date columns
    """
    result_df = df
    
    for col_name in date_columns:
        if col_name in df.columns:
            result_df = result_df.withColumn(
                f"{col_name}_utc",
                from_utc_timestamp(to_utc_timestamp(col(col_name), source_timezone), "UTC")
            )
            logger.info(f"Converted column {col_name} from {source_timezone} to UTC")
        else:
            logger.warning(f"Column {col_name} not found in DataFrame")
    
    return result_df


def validate_dataframe_schema(df: DataFrame, required_columns: List[str]) -> bool:
    """
    Validate that DataFrame contains all required columns.
    
    Args:
        df: Input DataFrame
        required_columns: List of required column names
        
    Returns:
        True if all required columns exist, False otherwise
    """
    existing_columns = set(df.columns)
    missing_columns = set(required_columns) - existing_columns
    
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        return False
    
    return True


def add_error_columns(df: DataFrame, error_flag_column: str = "_error_flag", 
                     error_message_column: str = "_error_message") -> DataFrame:
    """
    Add error tracking columns to DataFrame.
    
    Args:
        df: Input DataFrame
        error_flag_column: Name of error flag column
        error_message_column: Name of error message column
        
    Returns:
        DataFrame with error tracking columns
    """
    return (df
            .withColumn(error_flag_column, lit("N"))
            .withColumn(error_message_column, lit("")))


def flag_error_records(df: DataFrame, condition, error_message: str,
                      error_flag_column: str = "_error_flag",
                      error_message_column: str = "_error_message") -> DataFrame:
    """
    Flag records that meet error condition.
    
    Args:
        df: Input DataFrame
        condition: Condition to identify error records
        error_message: Error message to set
        error_flag_column: Name of error flag column
        error_message_column: Name of error message column
        
    Returns:
        DataFrame with error flags updated
    """
    return (df
            .withColumn(error_flag_column,
                       when(condition, "Y").otherwise(col(error_flag_column)))
            .withColumn(error_message_column,
                       when(condition, error_message).otherwise(col(error_message_column))))


def get_processing_metrics(df: DataFrame, start_time: float, 
                          end_time: float) -> Dict[str, Any]:
    """
    Calculate processing metrics for a DataFrame.
    
    Args:
        df: Input DataFrame
        start_time: Processing start time
        end_time: Processing end time
        
    Returns:
        Dictionary with processing metrics
    """
    record_count = df.count()
    processing_time = end_time - start_time
    
    return {
        "records_processed": record_count,
        "processing_time_seconds": processing_time,
        "records_per_second": record_count / processing_time if processing_time > 0 else 0
    }


def optimize_dataframe_for_processing(df: DataFrame, 
                                    target_partitions: Optional[int] = None) -> DataFrame:
    """
    Optimize DataFrame for processing by repartitioning and caching.
    
    Args:
        df: Input DataFrame
        target_partitions: Target number of partitions (optional)
        
    Returns:
        Optimized DataFrame
    """
    # Repartition if target partitions specified
    if target_partitions and target_partitions > 0:
        df = df.repartition(target_partitions)
        logger.info(f"Repartitioned DataFrame to {target_partitions} partitions")
    
    # Cache the DataFrame
    df.cache()
    logger.info("Cached DataFrame for processing")
    
    return df


def create_empty_dataframe_with_schema(spark: SparkSession, 
                                     schema: StructType) -> DataFrame:
    """
    Create an empty DataFrame with specified schema.
    
    Args:
        spark: SparkSession
        schema: DataFrame schema
        
    Returns:
        Empty DataFrame with specified schema
    """
    return spark.createDataFrame([], schema)


def log_dataframe_info(df: DataFrame, name: str) -> None:
    """
    Log DataFrame information for debugging.
    
    Args:
        df: Input DataFrame
        name: Name for logging
    """
    logger.info(f"{name} - Rows: {df.count()}, Columns: {len(df.columns)}")
    logger.info(f"{name} - Schema: {df.schema}")


def validate_business_keys(df: DataFrame, business_key_columns: List[str]) -> List[str]:
    """
    Validate business key columns and return any issues.
    
    Args:
        df: Input DataFrame
        business_key_columns: List of business key column names
        
    Returns:
        List of validation error messages
    """
    errors = []
    
    # Check if columns exist
    missing_columns = set(business_key_columns) - set(df.columns)
    if missing_columns:
        errors.append(f"Missing business key columns: {missing_columns}")
    
    # Check for null values in business keys
    for col_name in business_key_columns:
        if col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                errors.append(f"Found {null_count} null values in business key column: {col_name}")
    
    return errors
