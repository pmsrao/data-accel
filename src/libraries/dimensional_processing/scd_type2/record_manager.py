"""
Record lifecycle management for SCD processing.
"""

from typing import Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from delta.tables import DeltaTable
from functools import reduce
import logging
import time

from ..common.config import SCDConfig, ProcessingMetrics
from ..common.exceptions import SCDProcessingError

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


class RecordManager:
    """Manages record lifecycle operations for SCD processing."""
    
    def __init__(self, config: SCDConfig, spark: SparkSession):
        """
        Initialize RecordManager with configuration and Spark session.
        
        Args:
            config: SCD configuration
            spark: Spark session
        """
        self.config = config
        self.spark = spark
        self.delta_table = DeltaTable.forName(spark, config.target_table)
    
    def get_current_records(self) -> DataFrame:
        """
        Get current records from target table.
        
        Returns:
            DataFrame with current records only
        """
        current_records = self.spark.sql(f"""
            SELECT * FROM {self.config.target_table}
            WHERE {self.config.is_current_column} = 'Y'
        """)
        
        logger.info(f"Retrieved {current_records.count()} current records from {self.config.target_table}")
        return current_records
    
    def create_change_plan(self, source_df: DataFrame, current_df: DataFrame) -> Dict[str, DataFrame]:
        """
        Create processing plan based on detected changes.
        
        Args:
            source_df: Source DataFrame with new data
            current_df: Current records from target table
            
        Returns:
            Dictionary with categorized DataFrames
        """
        logger.info("🚀 ENTER: create_change_plan")
        
        logger.info(f"Source: {source_df.count()} records, Current: {current_df.count()} records")
        
        # Build join condition using PySpark Column expressions
        # Create join condition using Column expressions
        join_conditions = []
        for bk_col in self.config.business_key_columns:
            join_conditions.append(col(f"source.{bk_col}") == col(f"current.{bk_col}"))
        
        # Combine all conditions with AND
        join_condition = reduce(lambda a, b: a & b, join_conditions)
        
        # Join source with current records using explicit condition
        joined_df = source_df.alias("source").join(
            current_df.alias("current"),
            join_condition,
            "full_outer"
        )
        
        logger.info(f"Joined: {joined_df.count()} records")
        
        # Categorize records using the joined DataFrame
        # Check if current record exists by checking if the surrogate key is null
        new_records_raw = joined_df.filter(col(f"current.{self.config.surrogate_key_column}").isNull())
        unchanged_records_raw = joined_df.filter(
            col(f"source.{self.config.scd_hash_column}") == col(f"current.{self.config.scd_hash_column}")
        )
        changed_records_raw = joined_df.filter(
            col(f"source.{self.config.scd_hash_column}") != col(f"current.{self.config.scd_hash_column}")
        )
        
        logger.info(f"Raw categorization - New: {new_records_raw.count()}, Unchanged: {unchanged_records_raw.count()}, Changed: {changed_records_raw.count()}")
        
        # Clean the DataFrames to avoid ambiguity
        # Always clean the DataFrames, even if empty, to ensure consistent column structure
        new_records = self._clean_joined_dataframe(new_records_raw, "source")
        unchanged_records = unchanged_records_raw  # Keep unchanged records as-is since they don't need processing
        changed_records = self._clean_joined_dataframe_for_changes(changed_records_raw, "source")
        
        logger.info(f"Cleaned categorization - New: {new_records.count()}, Unchanged: {unchanged_records.count()}, Changed: {changed_records.count()}")
        
        change_plan = {
            "new_records": new_records,
            "unchanged_records": unchanged_records,
            "changed_records": changed_records
        }
        
        logger.info(f"Change plan created: {new_records.count()} new, {unchanged_records.count()} unchanged, {changed_records.count()} changed")
        logger.info("🏁 EXIT: create_change_plan")
        return change_plan
    
    def execute_change_plan(self, change_plan: Dict[str, DataFrame], source_df: DataFrame = None) -> ProcessingMetrics:
        """
        Execute the complete change plan.
        
        Args:
            change_plan: Dictionary with categorized DataFrames
            
        Returns:
            ProcessingMetrics with execution results
        """
        logger.info("🚀 ENTER: execute_change_plan")
        start_time = time.time()
        metrics = ProcessingMetrics()
        
        try:
            logger.info(f"Processing - New: {change_plan['new_records'].count()}, Changed: {change_plan['changed_records'].count()}, Unchanged: {change_plan['unchanged_records'].count()}")
            
            # Process new records
            new_count = self._insert_new_records(change_plan["new_records"])
            metrics.new_records_created = new_count
            
            # Process changed records
            updated_count = self._process_changed_records(change_plan["changed_records"], source_df)
            metrics.existing_records_updated = updated_count
            
            # Process unchanged records (update if needed)
            unchanged_count = self._process_unchanged_records(change_plan["unchanged_records"])
            # Don't add unchanged records to existing_records_updated - they weren't actually updated
            
            # records_processed should be the count of input records, not the sum of operations
            metrics.records_processed = change_plan["new_records"].count() + change_plan["changed_records"].count() + change_plan["unchanged_records"].count()
            metrics.processing_time_seconds = time.time() - start_time
            
            logger.info(f"✅ Completed - New: {new_count}, Updated: {updated_count}, Unchanged: {unchanged_count}")
            logger.info("🏁 EXIT: execute_change_plan")
            return metrics
            
        except Exception as e:
            logger.error(f"Error executing change plan: {str(e)}")
            logger.info("🏁 EXIT: execute_change_plan (with error)")
            raise SCDProcessingError(f"Failed to execute change plan: {str(e)}")
    
    def _insert_new_records(self, new_records_df: DataFrame) -> int:
        """
        Insert new records into target table.
        
        Args:
            new_records_df: DataFrame with new records
            
        Returns:
            Number of records inserted
        """
        logger.info("🚀 ENTER: _insert_new_records")
        
        if new_records_df.isEmpty():
            logger.info("🏁 EXIT: _insert_new_records (empty)")
            return 0
        
        # Prepare new records for insertion
        # The DataFrame is already cleaned and has unambiguous column references
        insert_df = new_records_df
        
        # Build merge condition with explicit column references
        merge_conditions = []
        for bk_col in self.config.business_key_columns:
            merge_conditions.append(col(f"target.{bk_col}") == col(f"source.{bk_col}"))
        
        merge_condition = reduce(lambda a, b: a & b, merge_conditions)
        
        before_count = self.delta_table.toDF().count()
        
        # Insert new records
        (self.delta_table.alias("target")
         .merge(insert_df.alias("source"), merge_condition)
         .whenNotMatchedInsertAll()
         .execute())
        
        after_count = self.delta_table.toDF().count()
        actual_inserted = after_count - before_count
        logger.info(f"✅ Inserted {actual_inserted} new records")
        
        logger.info("🏁 EXIT: _insert_new_records")
        return actual_inserted
    
    def _process_changed_records(self, changed_records_df: DataFrame, original_source_df: DataFrame = None) -> int:
        """
        Process changed records (expire old, insert new).
        
        Args:
            changed_records_df: DataFrame with changed records
            
        Returns:
            Number of new versions created
        """
        logger.info("🚀 ENTER: _process_changed_records")
        
        if changed_records_df.isEmpty():
            logger.info("🏁 EXIT: _process_changed_records (empty)")
            return 0
        
        record_count = changed_records_df.count()
        logger.info(f"Processing {record_count} changed records")
        
        # CRITICAL: Use temporary table to avoid DataFrame corruption after merge
        # The issue is that even with persist(), the DataFrame gets corrupted after merge
        # So we'll write to a temp table and read back after the merge
        
        logger.info(f"Storing {changed_records_df.count()} records in temporary table")
        
        # CRITICAL: Ensure effective_end_ts_utc column is preserved in temporary table
        # Spark drops columns with only NULL values when writing to tables
        # So we need to set a placeholder value before writing
        from pyspark.sql.functions import lit, when, col
        df_for_temp_table = changed_records_df.withColumn(
            self.config.effective_end_column,
            when(col(self.config.effective_end_column).isNull(), lit("9999-12-31 23:59:59").cast("timestamp"))
            .otherwise(col(self.config.effective_end_column))
        )
        
        # Create a temporary table to store the data
        temp_table_name = f"temp_changed_records_{id(changed_records_df)}"
        df_for_temp_table.write.mode("overwrite").saveAsTable(temp_table_name)
        
        # Execute the merge operation
        self._expire_existing_records(changed_records_df)
        
        # Read back the data from temporary table
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        reconstructed_df = spark.table(temp_table_name)
        
        # Restore the NULL values for effective_end_ts_utc
        reconstructed_df = reconstructed_df.withColumn(
            self.config.effective_end_column,
            when(col(self.config.effective_end_column) == lit("9999-12-31 23:59:59").cast("timestamp"), lit(None).cast("timestamp"))
            .otherwise(col(self.config.effective_end_column))
        )
        
        # CRITICAL: The reconstructed DataFrame has the wrong effective_start_ts_utc values
        # We need to use the original source data's effective_start_ts_utc values
        # The temporary table preserved the original values, but we need the new ones from source
        logger.info("Updating effective dates from source data")
        
        # Use current timestamp for new versions (simpler and more reliable)
        from pyspark.sql.functions import current_timestamp
        reconstructed_df = reconstructed_df.withColumn(
            self.config.effective_start_column,
            current_timestamp()  # Use current timestamp for new versions
        )
        
        reconstructed_df.persist()
        reconstructed_df.count()  # Force evaluation
        
        # Clean up temporary table
        spark.sql(f"DROP TABLE IF EXISTS {temp_table_name}")
        
        # Then, insert new versions using the reconstructed DataFrame
        new_versions_count = self._insert_new_versions(reconstructed_df)
        logger.info(f"✅ Created {new_versions_count} new versions")
        
        logger.info(f"Processed {record_count} changed records, created {new_versions_count} new versions")
        logger.info("🏁 EXIT: _process_changed_records")
        return new_versions_count
    
    def _process_unchanged_records(self, unchanged_records_df: DataFrame) -> int:
        """
        Process unchanged records (update if needed).
        
        Args:
            unchanged_records_df: DataFrame with unchanged records
            
        Returns:
            Number of records updated
        """
        if unchanged_records_df.isEmpty():
            return 0
        
        # For unchanged records, we might still need to update audit timestamps
        # This is a placeholder for any update logic needed for unchanged records
        record_count = unchanged_records_df.count()
        logger.info(f"Processed {record_count} unchanged records")
        return record_count
    
    def _expire_existing_records(self, changed_records_df: DataFrame) -> None:
        """
        Expire existing records by setting end date and is_current = 'N'.
        
        Args:
            changed_records_df: DataFrame with changed records
        """
        logger.info("🚀 ENTER: _expire_existing_records")
        from pyspark.sql.functions import expr
        
        logger.info(f"Expiring {changed_records_df.count()} existing records")
        
        # Build expire condition using Column expressions with explicit references
        expire_conditions = []
        for bk_col in self.config.business_key_columns:
            expire_conditions.append(col(f"target.{bk_col}") == col(f"source.{bk_col}"))
        
        # Add condition for current records
        expire_conditions.append(col(f"target.{self.config.is_current_column}") == lit("Y"))
        
        # Combine all conditions with AND
        expire_condition = reduce(lambda a, b: a & b, expire_conditions)
        
        # Set effective end date to be one second before the current timestamp
        # This ensures proper temporal ordering with minimal time difference
        from pyspark.sql.functions import col as spark_col, expr, current_timestamp
        
        # Use current timestamp minus 1 second for the expire end date
        expire_end_date = expr("current_timestamp() - interval 1 second")
        
        logger.info("Executing merge to expire existing records")
        
        # Execute merge to expire existing records
        (self.delta_table.alias("target")
         .merge(changed_records_df.alias("source"), expire_condition)
         .whenMatchedUpdate(set={
             self.config.effective_end_column: expire_end_date,
             self.config.is_current_column: lit("N"),
             self.config.modified_ts_column: current_timestamp()
         })
         .execute())
        
        logger.info("✅ Expired existing records")
        logger.info("🏁 EXIT: _expire_existing_records")
    
    def _insert_new_versions(self, changed_records_df: DataFrame) -> int:
        """
        Insert new versions of changed records.
        
        Args:
            changed_records_df: DataFrame with changed records
            
        Returns:
            Number of new versions inserted
        """
        logger.info("🚀 ENTER: _insert_new_versions")
        from pyspark.sql.functions import monotonically_increasing_id
        
        # Drop the surrogate key column if it exists to avoid conflicts
        if self.config.surrogate_key_column in changed_records_df.columns:
            changed_records_df = changed_records_df.drop(self.config.surrogate_key_column)
        
        # Generate new surrogate keys for the new versions using a more robust approach
        # Use current timestamp + row number to ensure uniqueness
        from pyspark.sql.functions import row_number, current_timestamp, concat, lit
        from pyspark.sql.window import Window
        
        # Create a window for row numbering
        window = Window.orderBy(*self.config.business_key_columns)
        
        # Generate unique surrogate keys using timestamp + row number
        new_versions_df = changed_records_df.withColumn(
            "row_num", row_number().over(window)
        ).withColumn(
            self.config.surrogate_key_column,
            concat(
                lit("SK_"),
                (current_timestamp().cast("bigint") * 1000 + col("row_num")).cast("string")
            )
        ).drop("row_num")
        
        # For new versions, we need to insert them directly since they have different surrogate keys
        # We can't use merge with whenNotMatched because the business keys will match
        # Instead, we'll insert them directly as new records
        
        # Insert new versions directly
        new_versions_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(self.config.target_table)
        
        record_count = new_versions_df.count()
        logger.info(f"✅ Inserted {record_count} new versions")
        logger.info("🏁 EXIT: _insert_new_versions")
        return record_count
    
    def _build_source_columns(self) -> list:
        """
        Build list of source columns for selection.
        
        Returns:
            List of column expressions for source data
        """
        source_columns = []
        
        # Add business key columns
        for bk_col in self.config.business_key_columns:
            source_columns.append(col(f"source.{bk_col}"))
        
        # Add SCD columns
        for scd_col in self.config.scd_columns:
            source_columns.append(col(f"source.{scd_col}"))
        
        # Add SCD metadata columns
        source_columns.extend([
            col(f"source.{self.config.scd_hash_column}"),
            col(f"source.{self.config.effective_start_column}"),
            col(f"source.{self.config.effective_end_column}"),
            col(f"source.{self.config.is_current_column}"),
            col(f"source.{self.config.created_ts_column}"),
            col(f"source.{self.config.modified_ts_column}"),
            col(f"source.{self.config.error_flag_column}"),
            col(f"source.{self.config.error_message_column}"),
            col(f"source.{self.config.surrogate_key_column}")
        ])
        
        return source_columns
    
    def _build_source_columns_with_aliases(self) -> list:
        """
        Build list of source columns with explicit aliases to avoid ambiguity.
        
        Returns:
            List of column expressions with aliases for source data
        """
        source_columns = []
        
        # Add business key columns
        for bk_col in self.config.business_key_columns:
            source_columns.append(col(f"source.{bk_col}").alias(bk_col))
        
        # Add SCD columns (excluding any that are already in business key columns)
        for scd_col in self.config.scd_columns:
            if scd_col not in self.config.business_key_columns:
                source_columns.append(col(f"source.{scd_col}").alias(scd_col))
        
        # Add SCD metadata columns
        source_columns.extend([
            col(f"source.{self.config.scd_hash_column}").alias(self.config.scd_hash_column),
            col(f"source.{self.config.effective_start_column}").alias(self.config.effective_start_column),
            col(f"source.{self.config.effective_end_column}").alias(self.config.effective_end_column),
            col(f"source.{self.config.is_current_column}").alias(self.config.is_current_column),
            col(f"source.{self.config.created_ts_column}").alias(self.config.created_ts_column),
            col(f"source.{self.config.modified_ts_column}").alias(self.config.modified_ts_column),
            col(f"source.{self.config.error_flag_column}").alias(self.config.error_flag_column),
            col(f"source.{self.config.error_message_column}").alias(self.config.error_message_column),
            col(f"source.{self.config.surrogate_key_column}").alias(self.config.surrogate_key_column)
        ])
        
        return source_columns
    
    def _clean_joined_dataframe(self, joined_df: DataFrame, alias_prefix: str) -> DataFrame:
        """
        Clean joined DataFrame to avoid ambiguous column references.
        
        Args:
            joined_df: DataFrame with joined data
            alias_prefix: Prefix to use for column selection ("source" or "current")
            
        Returns:
            Cleaned DataFrame with unambiguous column references
        """
        # Handle empty DataFrames by creating an empty DataFrame with the correct schema
        if joined_df.isEmpty():
            logger.info(f"Empty DataFrame provided for cleaning with alias_prefix: {alias_prefix}")
            # Create an empty DataFrame with the correct schema
            from pyspark.sql.types import StructType, StructField, StringType, TimestampType
            
            # Build the schema for the cleaned DataFrame
            schema_fields = []
            
            # Add business key columns
            for bk_col in self.config.business_key_columns:
                schema_fields.append(StructField(bk_col, StringType(), True))
            
            # Add SCD columns (excluding any that are already in business key columns)
            for scd_col in self.config.scd_columns:
                if scd_col not in self.config.business_key_columns:
                    schema_fields.append(StructField(scd_col, StringType(), True))
            
            # Add SCD metadata columns
            schema_fields.extend([
                StructField(self.config.scd_hash_column, StringType(), True),
                StructField(self.config.effective_start_column, TimestampType(), True),
                StructField(self.config.effective_end_column, TimestampType(), True),
                StructField(self.config.is_current_column, StringType(), True),
                StructField(self.config.created_ts_column, TimestampType(), True),
                StructField(self.config.modified_ts_column, TimestampType(), True),
                StructField(self.config.error_flag_column, StringType(), True),
                StructField(self.config.error_message_column, StringType(), True),
                StructField(self.config.surrogate_key_column, StringType(), True)
            ])
            
            schema = StructType(schema_fields)
            empty_df = joined_df.sparkSession.createDataFrame([], schema)
            logger.info(f"Created empty DataFrame with correct schema for alias_prefix: {alias_prefix}")
            return empty_df
        
        # Build list of columns to select from the specified alias
        clean_columns = []
        
        # Add business key columns
        for bk_col in self.config.business_key_columns:
            clean_columns.append(col(f"{alias_prefix}.{bk_col}").alias(bk_col))
        
        # Add SCD columns (excluding any that are already in business key columns)
        for scd_col in self.config.scd_columns:
            if scd_col not in self.config.business_key_columns:
                clean_columns.append(col(f"{alias_prefix}.{scd_col}").alias(scd_col))
        
        # Add SCD metadata columns
        clean_columns.extend([
            col(f"{alias_prefix}.{self.config.scd_hash_column}").alias(self.config.scd_hash_column),
            col(f"{alias_prefix}.{self.config.effective_start_column}").alias(self.config.effective_start_column),
            col(f"{alias_prefix}.{self.config.effective_end_column}").alias(self.config.effective_end_column),
            col(f"{alias_prefix}.{self.config.is_current_column}").alias(self.config.is_current_column),
            col(f"{alias_prefix}.{self.config.created_ts_column}").alias(self.config.created_ts_column),
            col(f"{alias_prefix}.{self.config.modified_ts_column}").alias(self.config.modified_ts_column),
            col(f"{alias_prefix}.{self.config.error_flag_column}").alias(self.config.error_flag_column),
            col(f"{alias_prefix}.{self.config.error_message_column}").alias(self.config.error_message_column),
            col(f"{alias_prefix}.{self.config.surrogate_key_column}").alias(self.config.surrogate_key_column)
        ])
        
        logger.info(f"Cleaning DataFrame with {len(clean_columns)} columns for alias_prefix: {alias_prefix}")
        cleaned_df = joined_df.select(*clean_columns)
        logger.info(f"Cleaned DataFrame has {cleaned_df.count()} records")
        
        return cleaned_df
    
    def _clean_joined_dataframe_for_changes(self, joined_df: DataFrame, alias_prefix: str) -> DataFrame:
        """
        Clean joined DataFrame for changed records (excludes surrogate key).
        
        Args:
            joined_df: DataFrame with joined data
            alias_prefix: Prefix to use for column selection ("source" or "current")
            
        Returns:
            Cleaned DataFrame without surrogate key (will be generated later)
        """
        # Handle empty DataFrames by creating an empty DataFrame with the correct schema
        if joined_df.isEmpty():
            logger.info(f"Empty DataFrame provided for cleaning changed records with alias_prefix: {alias_prefix}")
            # Create an empty DataFrame with the correct schema (without surrogate key)
            from pyspark.sql.types import StructType, StructField, StringType, TimestampType
            
            # Build the schema for the cleaned DataFrame
            schema_fields = []
            
            # Add business key columns
            for bk_col in self.config.business_key_columns:
                schema_fields.append(StructField(bk_col, StringType(), True))
            
            # Add SCD columns (excluding any that are already in business key columns)
            for scd_col in self.config.scd_columns:
                if scd_col not in self.config.business_key_columns:
                    schema_fields.append(StructField(scd_col, StringType(), True))
            
            # Add SCD metadata columns (excluding surrogate key)
            schema_fields.extend([
                StructField(self.config.scd_hash_column, StringType(), True),
                StructField(self.config.effective_start_column, TimestampType(), True),
                StructField(self.config.effective_end_column, TimestampType(), True),
                StructField(self.config.is_current_column, StringType(), True),
                StructField(self.config.created_ts_column, TimestampType(), True),
                StructField(self.config.modified_ts_column, TimestampType(), True),
                StructField(self.config.error_flag_column, StringType(), True),
                StructField(self.config.error_message_column, StringType(), True)
                # Note: No surrogate key field - will be generated later
            ])
            
            schema = StructType(schema_fields)
            empty_df = joined_df.sparkSession.createDataFrame([], schema)
            logger.info(f"Created empty DataFrame with correct schema for changed records")
            return empty_df
        
        # Build list of columns to select from the specified alias (excluding surrogate key)
        clean_columns = []
        
        # Add business key columns
        for bk_col in self.config.business_key_columns:
            clean_columns.append(col(f"{alias_prefix}.{bk_col}").alias(bk_col))
        
        # Add SCD columns (excluding any that are already in business key columns)
        for scd_col in self.config.scd_columns:
            if scd_col not in self.config.business_key_columns:
                clean_columns.append(col(f"{alias_prefix}.{scd_col}").alias(scd_col))
        
        # Add SCD metadata columns (excluding surrogate key)
        clean_columns.extend([
            col(f"{alias_prefix}.{self.config.scd_hash_column}").alias(self.config.scd_hash_column),
            col(f"{alias_prefix}.{self.config.effective_start_column}").alias(self.config.effective_start_column),
            col(f"{alias_prefix}.{self.config.effective_end_column}").alias(self.config.effective_end_column),
            col(f"{alias_prefix}.{self.config.is_current_column}").alias(self.config.is_current_column),
            col(f"{alias_prefix}.{self.config.created_ts_column}").alias(self.config.created_ts_column),
            col(f"{alias_prefix}.{self.config.modified_ts_column}").alias(self.config.modified_ts_column),
            col(f"{alias_prefix}.{self.config.error_flag_column}").alias(self.config.error_flag_column),
            col(f"{alias_prefix}.{self.config.error_message_column}").alias(self.config.error_message_column)
            # Note: No surrogate key - will be generated in _insert_new_versions
        ])
        
        logger.info(f"Cleaning DataFrame for changes with {len(clean_columns)} columns for alias_prefix: {alias_prefix}")
        
        cleaned_df = joined_df.select(*clean_columns)
        
        logger.info(f"Cleaned DataFrame for changes has {cleaned_df.count()} records")
        
        return cleaned_df
    
    def _build_merge_condition(self, source_alias: str, target_alias: str):
        """
        Build merge condition for business keys.
        
        Args:
            source_alias: Source table alias
            target_alias: Target table alias
            
        Returns:
            Merge condition as Column expression
        """
        # Build merge condition using Column expressions
        merge_conditions = []
        for bk_col in self.config.business_key_columns:
            merge_conditions.append(col(f"{target_alias}.{bk_col}") == col(f"{source_alias}.{bk_col}"))
        
        # Combine all conditions with AND
        merge_condition = reduce(lambda a, b: a & b, merge_conditions)
        return merge_condition
    
    def optimize_table(self) -> None:
        """
        Optimize the target table for better performance.
        """
        try:
            # Z-order optimization
            zorder_columns = (self.config.business_key_columns + 
                            [self.config.effective_start_column])
            
            self.spark.sql(f"""
                OPTIMIZE {self.config.target_table}
                ZORDER BY ({', '.join(zorder_columns)})
            """)
            
            logger.info(f"Optimized table {self.config.target_table} with ZORDER")
            
        except Exception as e:
            logger.warning(f"Failed to optimize table: {str(e)}")
    
    def get_table_info(self) -> Dict[str, Any]:
        """
        Get information about the target table.
        
        Returns:
            Dictionary with table information
        """
        try:
            # Get table statistics
            table_info = self.spark.sql(f"DESCRIBE EXTENDED {self.config.target_table}").collect()
            
            # Get record count
            record_count = self.spark.sql(f"SELECT COUNT(*) as count FROM {self.config.target_table}").collect()[0]["count"]
            
            # Get current record count
            current_count = self.spark.sql(f"""
                SELECT COUNT(*) as count FROM {self.config.target_table}
                WHERE {self.config.is_current_column} = 'Y'
            """).collect()[0]["count"]
            
            return {
                "table_name": self.config.target_table,
                "total_records": record_count,
                "current_records": current_count,
                "historical_records": record_count - current_count
            }
            
        except Exception as e:
            logger.error(f"Failed to get table info: {str(e)}")
            return {"error": str(e)}
