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
        # DEBUG: Log input DataFrames
        logger.info(f"🔍 DEBUG: Source DataFrame count: {source_df.count()}")
        logger.info(f"🔍 DEBUG: Current DataFrame count: {current_df.count()}")
        logger.info(f"🔍 DEBUG: Source DataFrame columns: {source_df.columns}")
        logger.info(f"🔍 DEBUG: Current DataFrame columns: {current_df.columns}")
        
        # DEBUG: Show sample source data
        logger.info("🔍 DEBUG: Sample source data:")
        source_df.select(*self.config.business_key_columns, self.config.scd_hash_column).show(5, truncate=False)
        
        # DEBUG: Show sample current data
        logger.info("🔍 DEBUG: Sample current data:")
        current_df.select(*self.config.business_key_columns, self.config.scd_hash_column).show(5, truncate=False)
        
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
        
        logger.info(f"🔍 DEBUG: Joined DataFrame count: {joined_df.count()}")
        logger.info(f"🔍 DEBUG: Joined DataFrame columns: {joined_df.columns}")
        
        # Categorize records using the joined DataFrame
        # Check if current record exists by checking if the surrogate key is null
        new_records_raw = joined_df.filter(col(f"current.{self.config.surrogate_key_column}").isNull())
        unchanged_records_raw = joined_df.filter(
            col(f"source.{self.config.scd_hash_column}") == col(f"current.{self.config.scd_hash_column}")
        )
        changed_records_raw = joined_df.filter(
            col(f"source.{self.config.scd_hash_column}") != col(f"current.{self.config.scd_hash_column}")
        )
        
        logger.info(f"🔍 DEBUG: Raw categorization - New: {new_records_raw.count()}, Unchanged: {unchanged_records_raw.count()}, Changed: {changed_records_raw.count()}")
        
        # DEBUG: Show sample changed records
        if changed_records_raw.count() > 0:
            logger.info("🔍 DEBUG: Sample changed records (raw):")
            changed_records_raw.select(
                col(f"source.{self.config.business_key_columns[0]}").alias("source_bk"),
                col(f"current.{self.config.business_key_columns[0]}").alias("current_bk"),
                col(f"source.{self.config.scd_hash_column}").alias("source_hash"),
                col(f"current.{self.config.scd_hash_column}").alias("current_hash")
            ).show(5, truncate=False)
        
        # DEBUG: Show sample new records
        if new_records_raw.count() > 0:
            logger.info("🔍 DEBUG: Sample new records (raw):")
            new_records_raw.select(
                col(f"source.{self.config.business_key_columns[0]}").alias("source_bk"),
                col(f"current.{self.config.business_key_columns[0]}").alias("current_bk"),
                col(f"source.{self.config.scd_hash_column}").alias("source_hash"),
                col(f"current.{self.config.scd_hash_column}").alias("current_hash")
            ).show(5, truncate=False)
        
        # Clean the DataFrames to avoid ambiguity
        # Always clean the DataFrames, even if empty, to ensure consistent column structure
        new_records = self._clean_joined_dataframe(new_records_raw, "source")
        unchanged_records = unchanged_records_raw  # Keep unchanged records as-is since they don't need processing
        changed_records = self._clean_joined_dataframe_for_changes(changed_records_raw, "source")
        
        logger.info(f"🔍 DEBUG: Cleaned categorization - New: {new_records.count()}, Unchanged: {unchanged_records.count()}, Changed: {changed_records.count()}")
        
        # DEBUG: Show sample cleaned changed records
        if changed_records.count() > 0:
            logger.info("🔍 DEBUG: Sample changed records (cleaned):")
            changed_records.select(*self.config.business_key_columns, self.config.scd_hash_column, self.config.effective_start_column).show(5, truncate=False)
        
        change_plan = {
            "new_records": new_records,
            "unchanged_records": unchanged_records,
            "changed_records": changed_records
        }
        
        logger.info(f"Change plan created: {new_records.count()} new, {unchanged_records.count()} unchanged, {changed_records.count()} changed")
        return change_plan
    
    def execute_change_plan(self, change_plan: Dict[str, DataFrame]) -> ProcessingMetrics:
        """
        Execute the complete change plan.
        
        Args:
            change_plan: Dictionary with categorized DataFrames
            
        Returns:
            ProcessingMetrics with execution results
        """
        start_time = time.time()
        metrics = ProcessingMetrics()
        
        try:
            # Process new records
            new_count = self._insert_new_records(change_plan["new_records"])
            metrics.new_records_created = new_count
            
            # Process changed records
            updated_count = self._process_changed_records(change_plan["changed_records"])
            metrics.existing_records_updated = updated_count
            
            # Process unchanged records (update if needed)
            unchanged_count = self._process_unchanged_records(change_plan["unchanged_records"])
            metrics.existing_records_updated += unchanged_count
            
            metrics.records_processed = new_count + updated_count + unchanged_count
            metrics.processing_time_seconds = time.time() - start_time
            
            logger.info(f"Change plan executed successfully. Metrics: {metrics.to_dict()}")
            return metrics
            
        except Exception as e:
            logger.error(f"Error executing change plan: {str(e)}")
            raise SCDProcessingError(f"Failed to execute change plan: {str(e)}")
    
    def _insert_new_records(self, new_records_df: DataFrame) -> int:
        """
        Insert new records into target table.
        
        Args:
            new_records_df: DataFrame with new records
            
        Returns:
            Number of records inserted
        """
        logger.info(f"🔍 DEBUG: _insert_new_records called with {new_records_df.count()} records")
        
        if new_records_df.isEmpty():
            logger.info("🔍 DEBUG: New records DataFrame is empty, returning 0")
            return 0
        
        # DEBUG: Show sample new records before insertion
        logger.info("🔍 DEBUG: Sample new records before insertion:")
        new_records_df.select(*self.config.business_key_columns, self.config.surrogate_key_column, self.config.effective_start_column).show(5, truncate=False)
        
        # Prepare new records for insertion
        # The DataFrame is already cleaned and has unambiguous column references
        insert_df = new_records_df
        
        # Build merge condition with explicit column references
        merge_conditions = []
        for bk_col in self.config.business_key_columns:
            merge_conditions.append(col(f"target.{bk_col}") == col(f"source.{bk_col}"))
        
        merge_condition = reduce(lambda a, b: a & b, merge_conditions)
        
        logger.info("🔍 DEBUG: About to insert new records")
        # Insert new records
        (self.delta_table.alias("target")
         .merge(insert_df.alias("source"), merge_condition)
         .whenNotMatchedInsertAll()
         .execute())
        
        logger.info("🔍 DEBUG: Finished inserting new records")
        
        record_count = insert_df.count()
        logger.info(f"Inserted {record_count} new records")
        return record_count
    
    def _process_changed_records(self, changed_records_df: DataFrame) -> int:
        """
        Process changed records (expire old, insert new).
        
        Args:
            changed_records_df: DataFrame with changed records
            
        Returns:
            Number of new versions created
        """
        logger.info(f"🔍 DEBUG: _process_changed_records called with {changed_records_df.count()} records")
        
        if changed_records_df.isEmpty():
            logger.info("🔍 DEBUG: Changed records DataFrame is empty, returning 0")
            return 0
        
        record_count = changed_records_df.count()
        logger.info(f"🔍 DEBUG: Processing {record_count} changed records")
        
        # DEBUG: Show sample changed records before processing
        logger.info("🔍 DEBUG: Sample changed records before processing:")
        changed_records_df.select(*self.config.business_key_columns, self.config.scd_hash_column, self.config.effective_start_column).show(5, truncate=False)
        
        # First, expire existing records
        logger.info("🔍 DEBUG: About to expire existing records")
        self._expire_existing_records(changed_records_df)
        logger.info("🔍 DEBUG: Finished expiring existing records")
        
        # Then, insert new versions
        logger.info("🔍 DEBUG: About to insert new versions")
        new_versions_count = self._insert_new_versions(changed_records_df)
        logger.info(f"🔍 DEBUG: Finished inserting {new_versions_count} new versions")
        
        logger.info(f"Processed {record_count} changed records, created {new_versions_count} new versions")
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
        from pyspark.sql.functions import expr
        
        logger.info(f"🔍 DEBUG: _expire_existing_records called with {changed_records_df.count()} records")
        
        # DEBUG: Show sample data before expiring
        logger.info("🔍 DEBUG: Sample changed records before expiring:")
        changed_records_df.select(*self.config.business_key_columns, self.config.effective_start_column).show(5, truncate=False)
        
        # Build expire condition using Column expressions with explicit references
        expire_conditions = []
        for bk_col in self.config.business_key_columns:
            expire_conditions.append(col(f"target.{bk_col}") == col(f"source.{bk_col}"))
        
        # Add condition for current records
        expire_conditions.append(col(f"target.{self.config.is_current_column}") == lit("Y"))
        
        # Combine all conditions with AND
        expire_condition = reduce(lambda a, b: a & b, expire_conditions)
        
        # Set effective end date to be one second before the new record's effective start date
        # This ensures proper temporal ordering with minimal time difference
        from pyspark.sql.functions import col as spark_col, expr
        
        # Use expr to subtract 1 second from the timestamp
        expire_end_date = expr(f"source.{self.config.effective_start_column} - interval 1 second")
        
        logger.info("🔍 DEBUG: About to execute merge for expiring records")
        logger.info(f"🔍 DEBUG: Expire condition: {expire_condition}")
        logger.info(f"🔍 DEBUG: Expire end date expression: source.{self.config.effective_start_column} - interval 1 second")
        
        (self.delta_table.alias("target")
         .merge(changed_records_df.alias("source"), expire_condition)
         .whenMatchedUpdate(set={
             self.config.effective_end_column: expire_end_date,
             self.config.is_current_column: lit("N"),
             self.config.modified_ts_column: current_timestamp()
         })
         .execute())
        
        logger.info("🔍 DEBUG: Finished executing merge for expiring records")
        logger.info("Expired existing records for changed records")
    
    def _insert_new_versions(self, changed_records_df: DataFrame) -> int:
        """
        Insert new versions of changed records.
        
        Args:
            changed_records_df: DataFrame with changed records
            
        Returns:
            Number of new versions inserted
        """
        from pyspark.sql.functions import monotonically_increasing_id
        
        logger.info(f"🔍 DEBUG: _insert_new_versions called with {changed_records_df.count()} records")
        logger.info(f"🔍 DEBUG: Input DataFrame columns: {changed_records_df.columns}")
        
        # First, ensure we don't have any existing surrogate key column
        # Drop the surrogate key column if it exists to avoid conflicts
        if self.config.surrogate_key_column in changed_records_df.columns:
            logger.info(f"🔍 DEBUG: Dropping existing surrogate key column: {self.config.surrogate_key_column}")
            changed_records_df = changed_records_df.drop(self.config.surrogate_key_column)
            logger.info(f"🔍 DEBUG: After dropping surrogate key, columns: {changed_records_df.columns}")
        else:
            logger.info(f"🔍 DEBUG: No existing surrogate key column found: {self.config.surrogate_key_column}")
        
        # Generate new surrogate keys for the new versions (cast to StringType for compatibility)
        logger.info("🔍 DEBUG: About to generate new surrogate keys")
        new_versions_df = changed_records_df.withColumn(
            self.config.surrogate_key_column,
            monotonically_increasing_id().cast("string")
        )
        
        logger.info(f"🔍 DEBUG: After generating surrogate keys, columns: {new_versions_df.columns}")
        logger.info(f"🔍 DEBUG: New versions DataFrame count: {new_versions_df.count()}")
        
        # DEBUG: Show sample new versions data
        logger.info("🔍 DEBUG: Sample new versions data before insertion:")
        new_versions_df.select(*self.config.business_key_columns, self.config.surrogate_key_column, self.config.effective_start_column).show(5, truncate=False)
        
        # For new versions, we need to insert them directly since they have different surrogate keys
        # We can't use merge with whenNotMatched because the business keys will match
        # Instead, we'll insert them directly as new records
        
        # Insert new versions directly
        logger.info(f"🔍 DEBUG: About to insert new versions into table: {self.config.target_table}")
        new_versions_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(self.config.target_table)
        
        logger.info("🔍 DEBUG: Finished inserting new versions")
        
        record_count = new_versions_df.count()
        logger.info(f"Inserted {record_count} new versions for changed records")
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
