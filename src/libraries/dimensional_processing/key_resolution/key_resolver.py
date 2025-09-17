"""
Main dimensional key resolver for fact tables.
"""

from typing import Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_date
import logging
import time

from ..common.config import KeyResolutionConfig
from ..common.exceptions import KeyResolutionError
from .lookup_manager import LookupManager
from .cache_manager import CacheManager

logger = logging.getLogger(__name__)


class DimensionalKeyResolver:
    """Resolves dimensional keys for fact tables."""
    
    def __init__(self, config: KeyResolutionConfig, spark: SparkSession):
        """
        Initialize DimensionalKeyResolver with configuration and Spark session.
        
        Args:
            config: Key resolution configuration
            spark: Spark session
        """
        self.config = config
        self.spark = spark
        
        # Initialize components
        self.lookup_manager = LookupManager(config, spark)
        self.cache_manager = CacheManager(config, spark)
        
        logger.info(f"Initialized DimensionalKeyResolver for dimension: {config.dimension_table}")
    
    def resolve_keys(self, fact_df: DataFrame, business_date_column: str) -> DataFrame:
        """
        Resolve dimensional keys for fact table.
        
        Args:
            fact_df: Fact table DataFrame
            business_date_column: Column containing business date
            
        Returns:
            DataFrame with resolved surrogate keys
        """
        start_time = time.time()
        
        try:
            logger.info(f"Starting key resolution for {fact_df.count()} fact records")
            
            # Validate input
            self._validate_input(fact_df, business_date_column)
            
            # Determine resolution strategy based on data
            resolution_strategy = self._determine_resolution_strategy(fact_df, business_date_column)
            
            # Resolve keys based on strategy
            if resolution_strategy == "current_only":
                resolved_df = self._resolve_current_only(fact_df)
            elif resolution_strategy == "historical_only":
                resolved_df = self._resolve_historical_only(fact_df, business_date_column)
            else:  # mixed
                resolved_df = self._resolve_mixed(fact_df, business_date_column)
            
            # Validate results
            validation_results = self.lookup_manager.validate_resolution_results(fact_df, resolved_df)
            
            processing_time = time.time() - start_time
            logger.info(f"Key resolution completed in {processing_time:.2f} seconds")
            logger.info(f"Resolution rate: {validation_results['resolution_rate']:.2%}")
            
            return resolved_df
            
        except Exception as e:
            logger.error(f"Key resolution failed: {str(e)}")
            raise KeyResolutionError(f"Key resolution failed: {str(e)}")
    
    def _validate_input(self, fact_df: DataFrame, business_date_column: str) -> None:
        """
        Validate input parameters.
        
        Args:
            fact_df: Fact table DataFrame
            business_date_column: Business date column name
        """
        # Check if business date column exists
        if business_date_column not in fact_df.columns:
            raise KeyResolutionError(f"Business date column '{business_date_column}' not found in fact table")
        
        # Check if business key columns exist in fact table
        missing_columns = set(self.config.business_key_columns) - set(fact_df.columns)
        if missing_columns:
            raise KeyResolutionError(f"Business key columns not found in fact table: {missing_columns}")
        
        # Check for null business keys
        for col_name in self.config.business_key_columns:
            null_count = fact_df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                logger.warning(f"Found {null_count} null values in business key column: {col_name}")
    
    def _determine_resolution_strategy(self, fact_df: DataFrame, 
                                     business_date_column: str) -> str:
        """
        Determine the best resolution strategy based on data characteristics.
        
        Args:
            fact_df: Fact table DataFrame
            business_date_column: Business date column name
            
        Returns:
            Resolution strategy: "current_only", "historical_only", or "mixed"
        """
        # Check date distribution
        current_date_count = fact_df.filter(col(business_date_column) >= current_date()).count()
        historical_date_count = fact_df.filter(col(business_date_column) < current_date()).count()
        total_count = fact_df.count()
        
        if current_date_count == total_count:
            return "current_only"
        elif historical_date_count == total_count:
            return "historical_only"
        else:
            return "mixed"
    
    def _resolve_current_only(self, fact_df: DataFrame) -> DataFrame:
        """
        Resolve keys for current records only.
        
        Args:
            fact_df: Fact table DataFrame
            
        Returns:
            DataFrame with resolved surrogate keys
        """
        logger.info("Resolving keys for current records only")
        
        # Get current dimension records (with caching)
        current_dimension = self.cache_manager.get_cached_records("current_dimension")
        if current_dimension is None:
            current_dimension = self.lookup_manager.get_current_dimension_records()
            self.cache_manager.cache_records(current_dimension, "current_dimension")
        
        return self.lookup_manager.resolve_current_keys(fact_df, current_dimension)
    
    def _resolve_historical_only(self, fact_df: DataFrame, 
                               business_date_column: str) -> DataFrame:
        """
        Resolve keys for historical records only.
        
        Args:
            fact_df: Fact table DataFrame
            business_date_column: Business date column name
            
        Returns:
            DataFrame with resolved surrogate keys
        """
        logger.info("Resolving keys for historical records only")
        return self.lookup_manager.resolve_historical_keys(fact_df, business_date_column)
    
    def _resolve_mixed(self, fact_df: DataFrame, business_date_column: str) -> DataFrame:
        """
        Resolve keys for mixed current and historical records.
        
        Args:
            fact_df: Fact table DataFrame
            business_date_column: Business date column name
            
        Returns:
            DataFrame with resolved surrogate keys
        """
        logger.info("Resolving keys for mixed current and historical records")
        return self.lookup_manager.resolve_mixed_keys(fact_df, business_date_column)
    
    def get_resolution_stats(self, fact_df: DataFrame, 
                           resolved_df: DataFrame) -> Dict[str, Any]:
        """
        Get key resolution statistics.
        
        Args:
            fact_df: Original fact DataFrame
            resolved_df: Resolved DataFrame
            
        Returns:
            Dictionary with resolution statistics
        """
        validation_results = self.lookup_manager.validate_resolution_results(fact_df, resolved_df)
        cache_stats = self.cache_manager.get_cache_stats()
        dimension_info = self.lookup_manager.get_dimension_info()
        
        return {
            "resolution_stats": validation_results,
            "cache_stats": cache_stats,
            "dimension_info": dimension_info
        }
    
    def clear_cache(self) -> None:
        """Clear all cached data."""
        self.cache_manager.clear_cache()
        logger.info("Cleared key resolution cache")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache statistics
        """
        return self.cache_manager.get_cache_stats()
    
    def batch_resolve_keys(self, fact_df: DataFrame, business_date_column: str,
                          batch_size: int = None) -> DataFrame:
        """
        Resolve keys in batches for large datasets.
        
        Args:
            fact_df: Fact table DataFrame
            business_date_column: Business date column name
            batch_size: Batch size (uses config default if not specified)
            
        Returns:
            DataFrame with resolved surrogate keys
        """
        if batch_size is None:
            batch_size = self.config.batch_size
        
        total_records = fact_df.count()
        if total_records <= batch_size:
            return self.resolve_keys(fact_df, business_date_column)
        
        logger.info(f"Resolving keys in batches of {batch_size} records")
        
        # Get unique business keys for batching
        unique_keys = fact_df.select(*self.config.business_key_columns).distinct()
        unique_key_count = unique_keys.count()
        
        # Process in batches
        resolved_batches = []
        for i in range(0, unique_key_count, batch_size):
            batch_keys = unique_keys.limit(batch_size).offset(i)
            batch_fact = fact_df.join(batch_keys, self.config.business_key_columns, "inner")
            
            batch_resolved = self.resolve_keys(batch_fact, business_date_column)
            resolved_batches.append(batch_resolved)
            
            logger.info(f"Processed batch {i//batch_size + 1}/{(unique_key_count + batch_size - 1)//batch_size}")
        
        # Union all batches
        if resolved_batches:
            result_df = resolved_batches[0]
            for batch_df in resolved_batches[1:]:
                result_df = result_df.unionByName(batch_df)
            
            logger.info(f"Batch resolution completed for {result_df.count()} records")
            return result_df
        else:
            return self.spark.createDataFrame([], fact_df.schema)
