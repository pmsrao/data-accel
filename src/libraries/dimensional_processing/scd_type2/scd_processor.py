"""
Main SCD Type 2 processor with clean separation of concerns.
"""

from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
import logging
import time

from ..common.config import SCDConfig, ProcessingMetrics
from ..common.exceptions import SCDValidationError, SCDProcessingError
from ..common.utils import add_error_columns, flag_error_records, get_processing_metrics
from .hash_manager import HashManager
from .record_manager import RecordManager
from .date_manager import DateManager
from .validators import SCDValidator

logger = logging.getLogger(__name__)
# Ensure debug statements are visible
logger.setLevel(logging.DEBUG)

# Add console handler if not already present
if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)


class SCDProcessor:
    """Main SCD Type 2 processor with clean separation of concerns."""
    
    def __init__(self, config: SCDConfig, spark: SparkSession):
        """
        Initialize SCDProcessor with configuration and Spark session.
        
        Args:
            config: SCD configuration
            spark: Spark session
        """
        self.config = config
        self.spark = spark
        
        # Initialize components
        self.hash_manager = HashManager(config)
        self.record_manager = RecordManager(config, spark)
        self.date_manager = DateManager(config)
        self.validator = SCDValidator(config)
        
        logger.info(f"Initialized SCDProcessor for table: {config.target_table}")
    
    def process_scd(self, source_df: DataFrame) -> ProcessingMetrics:
        """
        Main entry point for SCD Type 2 processing.
        
        Args:
            source_df: Source DataFrame with data to process
            
        Returns:
            ProcessingMetrics: Processing metrics and status
        """
        logger.info("🚀 ENTER: process_scd")
        start_time = time.time()
        
        try:
            logger.info(f"Starting SCD processing for {source_df.count()} records")
            
            # Step 1: Validate input data
            validation_result = self.validator.validate_source_data(source_df)
            if not validation_result.is_valid:
                logger.error(f"Validation failed: {validation_result.errors}")
                raise SCDValidationError(f"Validation failed: {validation_result.errors}")
            
            # Step 2: Add error tracking columns
            source_df = add_error_columns(source_df, 
                                        self.config.error_flag_column,
                                        self.config.error_message_column)
            
            # Step 3: Prepare source data with SCD metadata
            prepared_df = self._prepare_source_data(source_df)
            
            # Step 4: Get current records from target table
            current_records = self.record_manager.get_current_records()
            
            # Step 5: Create change plan
            change_plan = self.record_manager.create_change_plan(prepared_df, current_records)
            
            # Step 6: Execute changes
            execution_result = self.record_manager.execute_change_plan(change_plan, prepared_df)
            
            # Step 7: Optimize table if enabled
            if self.config.enable_optimization:
                self.record_manager.optimize_table()
            
            # Step 8: Calculate final metrics
            processing_time = time.time() - start_time
            execution_result.processing_time_seconds = processing_time
            
            logger.info(f"SCD processing completed successfully. Metrics: {execution_result.to_dict()}")
            logger.info("🏁 EXIT: process_scd")
            return execution_result
            
        except Exception as e:
            logger.error(f"SCD processing failed: {str(e)}")
            logger.info("🏁 EXIT: process_scd (with error)")
            raise SCDProcessingError(f"SCD processing failed: {str(e)}")
    
    def _prepare_source_data(self, source_df: DataFrame) -> DataFrame:
        """
        Prepare source data with SCD metadata.
        
        Args:
            source_df: Source DataFrame
            
        Returns:
            DataFrame with SCD metadata added
        """
        logger.info("Preparing source data with SCD metadata")
        
        # Step 0: Deduplicate source data to prevent merge conflicts (only if enabled)
        if self.config.enable_source_deduplication:
            deduplicated_df = self._deduplicate_source_data(source_df)
        else:
            deduplicated_df = source_df
            logger.info("Source deduplication is disabled - using original source data")
        
        # Step 1: Compute SCD hash
        df_with_hash = self.hash_manager.compute_scd_hash(deduplicated_df)
        
        # Step 2: Determine effective start date
        df_with_dates = self.date_manager.determine_effective_start(
            df_with_hash,
            self.config.effective_from_column,
            self.config.initial_effective_from_column
        )
        
        # Step 3: Set effective end date to null for new records
        df_with_end_date = self.date_manager.set_effective_end_date(df_with_dates, lit(None))
        
        # Step 4: Set current flag to 'Y' for new records
        df_with_current = self.date_manager.set_current_flag(df_with_end_date, "Y")
        
        # Step 5: Set audit timestamps
        df_with_audit = self.date_manager.set_audit_timestamps(df_with_current)
        
        # Step 6: Generate surrogate keys for new records
        prepared_df = self.date_manager.generate_surrogate_keys(df_with_audit)
        
        logger.info(f"Prepared {prepared_df.count()} records with SCD metadata")
        return prepared_df
    
    def _deduplicate_source_data(self, source_df: DataFrame) -> DataFrame:
        """
        Deduplicate source data to prevent merge conflicts.
        
        Args:
            source_df: Source DataFrame
            
        Returns:
            Deduplicated DataFrame
        """
        from pyspark.sql.functions import row_number, col
        from pyspark.sql.window import Window
        
        # Check if we have duplicates based on business keys
        original_count = source_df.count()
        
        # Create window specification for deduplication
        # Use business keys + effective date for deduplication
        if self.config.deduplication_strategy == "latest":
            # Keep the record with the latest effective date
            window_spec = Window.partitionBy(*self.config.business_key_columns).orderBy(
                col(self.config.effective_from_column).desc_nulls_last()
            )
        elif self.config.deduplication_strategy == "earliest":
            # Keep the record with the earliest effective date
            window_spec = Window.partitionBy(*self.config.business_key_columns).orderBy(
                col(self.config.effective_from_column).asc_nulls_last()
            )
        else:
            # Default to latest
            window_spec = Window.partitionBy(*self.config.business_key_columns).orderBy(
                col(self.config.effective_from_column).desc_nulls_last()
            )
        
        # Add row number to identify duplicates
        df_with_row_num = source_df.withColumn("row_num", row_number().over(window_spec))
        
        # Keep only the first record for each business key
        deduplicated_df = df_with_row_num.filter(col("row_num") == 1).drop("row_num")
        
        deduplicated_count = deduplicated_df.count()
        duplicates_removed = original_count - deduplicated_count
        
        if duplicates_removed > 0:
            logger.warning(f"Removed {duplicates_removed} duplicate records from source data using '{self.config.deduplication_strategy}' strategy")
            logger.warning(f"Original: {original_count}, After deduplication: {deduplicated_count}")
        else:
            logger.info("No duplicate records found in source data")
        
        return deduplicated_df
    
    def process_incremental(self, source_df: DataFrame, 
                          last_processed_timestamp: str) -> ProcessingMetrics:
        """
        Process only new/changed records since last run.
        
        Args:
            source_df: Source DataFrame
            last_processed_timestamp: Last processed timestamp
            
        Returns:
            ProcessingMetrics: Processing metrics
        """
        logger.info(f"Processing incremental data since {last_processed_timestamp}")
        
        # Filter for incremental data
        if self.config.effective_from_column and self.config.effective_from_column in source_df.columns:
            incremental_df = source_df.filter(
                col(self.config.effective_from_column) > last_processed_timestamp
            )
        else:
            logger.warning("No effective_from_column specified, processing all data")
            incremental_df = source_df
        
        logger.info(f"Found {incremental_df.count()} incremental records")
        return self.process_scd(incremental_df)
    
    def get_table_info(self) -> dict:
        """
        Get information about the target table.
        
        Returns:
            Dictionary with table information
        """
        return self.record_manager.get_table_info()
    
    def validate_table_schema(self) -> bool:
        """
        Validate that target table has required SCD columns.
        
        Returns:
            True if schema is valid, False otherwise
        """
        try:
            # Get table schema
            table_schema = self.spark.sql(f"DESCRIBE {self.config.target_table}").collect()
            table_columns = {row["col_name"] for row in table_schema}
            
            # Check required columns
            required_columns = [
                self.config.scd_hash_column,
                self.config.effective_start_column,
                self.config.effective_end_column,
                self.config.is_current_column,
                self.config.created_ts_column,
                self.config.modified_ts_column
            ]
            
            missing_columns = set(required_columns) - table_columns
            if missing_columns:
                logger.error(f"Missing required columns in target table: {missing_columns}")
                return False
            
            logger.info("Target table schema validation passed")
            return True
            
        except Exception as e:
            logger.error(f"Failed to validate table schema: {str(e)}")
            return False
    
    def create_target_table_if_not_exists(self, schema: dict) -> None:
        """
        Create target table if it doesn't exist.
        
        Args:
            schema: Table schema definition
        """
        try:
            # Check if table exists
            table_exists = self.spark.sql(f"""
                SELECT COUNT(*) as count 
                FROM information_schema.tables 
                WHERE table_name = '{self.config.target_table.split('.')[-1]}'
            """).collect()[0]["count"] > 0
            
            if not table_exists:
                # Create table with specified schema
                create_sql = self._build_create_table_sql(schema)
                self.spark.sql(create_sql)
                logger.info(f"Created target table: {self.config.target_table}")
            else:
                logger.info(f"Target table already exists: {self.config.target_table}")
                
        except Exception as e:
            logger.error(f"Failed to create target table: {str(e)}")
            raise SCDProcessingError(f"Failed to create target table: {str(e)}")
    
    def _build_create_table_sql(self, schema: dict) -> str:
        """
        Build CREATE TABLE SQL statement.
        
        Args:
            schema: Table schema definition
            
        Returns:
            CREATE TABLE SQL statement
        """
        # This is a simplified version - in practice, you'd want more robust schema handling
        columns = []
        
        # Add business key columns
        for col_name in self.config.business_key_columns:
            columns.append(f"{col_name} STRING")
        
        # Add SCD columns
        for col_name in self.config.scd_columns:
            columns.append(f"{col_name} STRING")
        
        # Add SCD metadata columns
        columns.extend([
            f"{self.config.scd_hash_column} STRING",
            f"{self.config.effective_start_column} TIMESTAMP",
            f"{self.config.effective_end_column} TIMESTAMP",
            f"{self.config.is_current_column} STRING",
            f"{self.config.created_ts_column} TIMESTAMP",
            f"{self.config.modified_ts_column} TIMESTAMP"
        ])
        
        columns_sql = ",\n    ".join(columns)
        
        return f"""
        CREATE TABLE IF NOT EXISTS {self.config.target_table} (
            {columns_sql}
        ) USING DELTA
        """
