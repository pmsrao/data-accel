"""
Historical data deduplication for SCD processing.
"""

from typing import Optional, Callable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, row_number, when
from pyspark.sql.window import Window
import logging
import time

from ..common.config import DeduplicationConfig, ValidationResult
from ..common.exceptions import DeduplicationError
from .hash_manager import HashManager
from .validators import SCDValidator

logger = logging.getLogger(__name__)


class HistoricalDataDeduplicator:
    """Deduplicates historical data before SCD processing."""
    
    def __init__(self, config: DeduplicationConfig, spark: SparkSession):
        """
        Initialize HistoricalDataDeduplicator with configuration and Spark session.
        
        Args:
            config: Deduplication configuration
            spark: Spark session
        """
        self.config = config
        self.spark = spark
        
        # Create a temporary SCD config for hash manager
        from ..common.config import SCDConfig
        temp_scd_config = SCDConfig(
            target_table="temp",
            business_key_columns=config.business_key_columns,
            scd_columns=config.scd_columns,
            surrogate_key_column="temp_sk",  # Temporary surrogate key for deduplication
            scd_hash_column=config.scd_hash_column
        )
        
        self.hash_manager = HashManager(temp_scd_config)
        self.validator = SCDValidator(temp_scd_config)
        
        logger.info(f"Initialized HistoricalDataDeduplicator with strategy: {config.deduplication_strategy}")
    
    def deduplicate_historical_data(self, source_df: DataFrame) -> DataFrame:
        """
        Deduplicate historical data using SCD hash-based approach.
        
        Args:
            source_df: Historical data with potential duplicates
            
        Returns:
            Deduplicated DataFrame ready for SCD processing
        """
        start_time = time.time()
        
        try:
            logger.info(f"Starting deduplication of {source_df.count()} historical records")
            
            # Step 1: Validate input data
            validation_result = self._validate_input_data(source_df)
            if not validation_result.is_valid:
                logger.error(f"Input validation failed: {validation_result.errors}")
                raise DeduplicationError(f"Input validation failed: {validation_result.errors}")
            
            # Step 2: Compute SCD hashes
            df_with_hash = self._compute_scd_hashes(source_df)
            
            # Step 3: Apply deduplication strategy
            deduplicated_df = self._apply_deduplication_strategy(df_with_hash)
            
            # Step 4: Validate results
            self._validate_deduplication_result(source_df, deduplicated_df)
            
            processing_time = time.time() - start_time
            original_count = source_df.count()
            deduplicated_count = deduplicated_df.count()
            duplicates_removed = original_count - deduplicated_count
            
            logger.info(f"Deduplication completed in {processing_time:.2f} seconds")
            logger.info(f"Original records: {original_count}, Deduplicated: {deduplicated_count}, Removed: {duplicates_removed}")
            
            return deduplicated_df
            
        except Exception as e:
            logger.error(f"Deduplication failed: {str(e)}")
            raise DeduplicationError(f"Deduplication failed: {str(e)}")
    
    def _validate_input_data(self, df: DataFrame) -> ValidationResult:
        """
        Validate input data for deduplication.
        
        Args:
            df: Input DataFrame
            
        Returns:
            ValidationResult with validation status
        """
        result = ValidationResult(is_valid=True)
        
        # Check required columns
        required_columns = (self.config.business_key_columns + 
                          self.config.scd_columns + 
                          [self.config.effective_from_column])
        
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            result.add_error(f"Missing required columns: {missing_columns}")
        
        # Check for null business keys
        for col_name in self.config.business_key_columns:
            if col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                if null_count > 0:
                    result.add_error(f"Found {null_count} null values in business key column: {col_name}")
        
        # Check for null effective from dates
        if self.config.effective_from_column in df.columns:
            null_dates = df.filter(col(self.config.effective_from_column).isNull()).count()
            if null_dates > 0:
                result.add_error(f"Found {null_dates} null values in effective_from_column: {self.config.effective_from_column}")
        
        return result
    
    def _compute_scd_hashes(self, df: DataFrame) -> DataFrame:
        """
        Compute SCD hashes for all records.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with SCD hash column added
        """
        logger.info("Computing SCD hashes for deduplication")
        return self.hash_manager.compute_scd_hash(df)
    
    def _apply_deduplication_strategy(self, df: DataFrame) -> DataFrame:
        """
        Apply the configured deduplication strategy.
        
        Args:
            df: DataFrame with SCD hashes
            
        Returns:
            Deduplicated DataFrame
        """
        strategy = self.config.deduplication_strategy.lower()
        
        if strategy == "latest":
            return self._keep_latest_per_scd_hash(df)
        elif strategy == "earliest":
            return self._keep_earliest_per_scd_hash(df)
        elif strategy == "significant":
            return self._keep_significant_records(df)
        elif strategy == "custom":
            return self._apply_custom_logic(df)
        else:
            raise DeduplicationError(f"Unknown deduplication strategy: {strategy}")
    
    def _keep_latest_per_scd_hash(self, df: DataFrame) -> DataFrame:
        """
        Keep the latest record per business key and SCD hash.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with latest records per SCD hash
        """
        logger.info("Applying 'latest' deduplication strategy")
        
        window_spec = Window.partitionBy(
            *self.config.business_key_columns, 
            self.config.scd_hash_column
        ).orderBy(col(self.config.effective_from_column).desc())
        
        return (df
                .withColumn("row_num", row_number().over(window_spec))
                .filter(col("row_num") == 1)
                .drop("row_num"))
    
    def _keep_earliest_per_scd_hash(self, df: DataFrame) -> DataFrame:
        """
        Keep the earliest record per business key and SCD hash.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with earliest records per SCD hash
        """
        logger.info("Applying 'earliest' deduplication strategy")
        
        window_spec = Window.partitionBy(
            *self.config.business_key_columns, 
            self.config.scd_hash_column
        ).orderBy(col(self.config.effective_from_column).asc())
        
        return (df
                .withColumn("row_num", row_number().over(window_spec))
                .filter(col("row_num") == 1)
                .drop("row_num"))
    
    def _keep_significant_records(self, df: DataFrame) -> DataFrame:
        """
        Keep records marked as significant.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with significant records only
        """
        logger.info("Applying 'significant' deduplication strategy")
        
        if not self.config.significance_column:
            raise DeduplicationError("Significance column must be specified for 'significant' strategy")
        
        if self.config.significance_column not in df.columns:
            raise DeduplicationError(f"Significance column '{self.config.significance_column}' not found in DataFrame")
        
        # First, try to keep significant records
        significant_df = df.filter(col(self.config.significance_column) == "Y")
        
        # If no significant records found, fall back to latest strategy
        if significant_df.count() == 0:
            logger.warning("No significant records found, falling back to 'latest' strategy")
            return self._keep_latest_per_scd_hash(df)
        
        # For each business key + SCD hash combination, keep only significant records
        # If multiple significant records exist, keep the latest one
        window_spec = Window.partitionBy(
            *self.config.business_key_columns, 
            self.config.scd_hash_column
        ).orderBy(col(self.config.effective_from_column).desc())
        
        return (significant_df
                .withColumn("row_num", row_number().over(window_spec))
                .filter(col("row_num") == 1)
                .drop("row_num"))
    
    def _apply_custom_logic(self, df: DataFrame) -> DataFrame:
        """
        Apply custom deduplication logic.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with custom deduplication applied
        """
        logger.info("Applying custom deduplication logic")
        
        if not self.config.custom_deduplication_logic:
            raise DeduplicationError("Custom deduplication logic not provided")
        
        try:
            return self.config.custom_deduplication_logic(df)
        except Exception as e:
            raise DeduplicationError(f"Custom deduplication logic failed: {str(e)}")
    
    def _validate_deduplication_result(self, original_df: DataFrame, 
                                     deduplicated_df: DataFrame) -> None:
        """
        Validate deduplication results.
        
        Args:
            original_df: Original DataFrame
            deduplicated_df: Deduplicated DataFrame
        """
        original_count = original_df.count()
        deduplicated_count = deduplicated_df.count()
        
        # Check for data loss (should not happen in deduplication)
        if deduplicated_count > original_count:
            raise DeduplicationError("Deduplication resulted in more records than original - this should not happen")
        
        # Check for complete data loss
        if deduplicated_count == 0 and original_count > 0:
            raise DeduplicationError("Deduplication resulted in complete data loss")
        
        # Validate that all business keys are still present
        original_business_keys = set(original_df.select(*self.config.business_key_columns).distinct().rdd.map(tuple).collect())
        deduplicated_business_keys = set(deduplicated_df.select(*self.config.business_key_columns).distinct().rdd.map(tuple).collect())
        
        missing_business_keys = original_business_keys - deduplicated_business_keys
        if missing_business_keys:
            logger.warning(f"Some business keys were completely removed during deduplication: {len(missing_business_keys)} keys")
        
        logger.info("Deduplication result validation passed")
    
    def get_deduplication_stats(self, original_df: DataFrame, 
                              deduplicated_df: DataFrame) -> dict:
        """
        Get deduplication statistics.
        
        Args:
            original_df: Original DataFrame
            deduplicated_df: Deduplicated DataFrame
            
        Returns:
            Dictionary with deduplication statistics
        """
        original_count = original_df.count()
        deduplicated_count = deduplicated_df.count()
        duplicates_removed = original_count - deduplicated_count
        
        # Calculate deduplication ratio
        deduplication_ratio = duplicates_removed / original_count if original_count > 0 else 0
        
        return {
            "original_records": original_count,
            "deduplicated_records": deduplicated_count,
            "duplicates_removed": duplicates_removed,
            "deduplication_ratio": deduplication_ratio,
            "strategy_used": self.config.deduplication_strategy
        }
