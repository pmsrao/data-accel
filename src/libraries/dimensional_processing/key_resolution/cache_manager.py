"""
Cache management for dimensional key resolution.
"""

from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import broadcast
import logging
import time

from ..common.config import KeyResolutionConfig

logger = logging.getLogger(__name__)


class CacheManager:
    """Manages caching for dimensional key resolution performance optimization."""
    
    def __init__(self, config: KeyResolutionConfig, spark: SparkSession):
        """
        Initialize CacheManager with configuration and Spark session.
        
        Args:
            config: Key resolution configuration
            spark: Spark session
        """
        self.config = config
        self.spark = spark
        self.cache = {}
        self.cache_timestamps = {}
        
        logger.info(f"Initialized CacheManager with TTL: {config.cache_ttl_minutes} minutes")
    
    def get_cached_records(self, cache_key: str = "current_dimension") -> Optional[DataFrame]:
        """
        Get cached dimension records.
        
        Args:
            cache_key: Cache key for the records
            
        Returns:
            Cached DataFrame or None if not found/expired
        """
        if not self.config.enable_caching:
            return None
        
        if cache_key not in self.cache:
            return None
        
        # Check if cache has expired
        if self._is_cache_expired(cache_key):
            logger.info(f"Cache expired for key: {cache_key}")
            self._remove_from_cache(cache_key)
            return None
        
        logger.info(f"Retrieved cached records for key: {cache_key}")
        return self.cache[cache_key]
    
    def cache_records(self, df: DataFrame, cache_key: str = "current_dimension") -> None:
        """
        Cache dimension records.
        
        Args:
            df: DataFrame to cache
            cache_key: Cache key for the records
        """
        if not self.config.enable_caching:
            return
        
        # Cache the DataFrame (skip in Spark Connect/Serverless)
        try:
            df.cache()
            self.cache[cache_key] = df
            self.cache_timestamps[cache_key] = time.time()
            logger.info(f"Cached {df.count()} records for key: {cache_key}")
        except Exception as e:
            # Skip caching in Spark Connect/Serverless where cache() is not supported
            logger.warning(f"Caching not supported in this environment: {str(e)}")
            logger.info(f"Storing DataFrame reference for key: {cache_key} (without caching)")
            self.cache[cache_key] = df
            self.cache_timestamps[cache_key] = time.time()
    
    def create_broadcast_lookup(self, dimension_df: DataFrame) -> DataFrame:
        """
        Create broadcast lookup for small dimension tables.
        
        Args:
            dimension_df: Dimension DataFrame
            
        Returns:
            Broadcast DataFrame for efficient joins
        """
        record_count = dimension_df.count()
        
        # Only broadcast if table is small enough
        if record_count <= 100000:  # 100K records threshold
            broadcast_df = broadcast(dimension_df)
            logger.info(f"Created broadcast lookup for {record_count} records")
            return broadcast_df
        else:
            logger.info(f"Table too large for broadcast ({record_count} records), using regular join")
            return dimension_df
    
    def _is_cache_expired(self, cache_key: str) -> bool:
        """
        Check if cache has expired.
        
        Args:
            cache_key: Cache key
            
        Returns:
            True if cache has expired, False otherwise
        """
        if cache_key not in self.cache_timestamps:
            return True
        
        cache_time = self.cache_timestamps[cache_key]
        current_time = time.time()
        ttl_seconds = self.config.cache_ttl_minutes * 60
        
        return (current_time - cache_time) > ttl_seconds
    
    def _remove_from_cache(self, cache_key: str) -> None:
        """
        Remove entry from cache.
        
        Args:
            cache_key: Cache key to remove
        """
        if cache_key in self.cache:
            # Unpersist the DataFrame
            self.cache[cache_key].unpersist()
            del self.cache[cache_key]
            del self.cache_timestamps[cache_key]
            logger.info(f"Removed cache entry for key: {cache_key}")
    
    def clear_cache(self) -> None:
        """Clear all cached entries."""
        for cache_key in list(self.cache.keys()):
            self._remove_from_cache(cache_key)
        
        logger.info("Cleared all cache entries")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache statistics
        """
        current_time = time.time()
        active_entries = 0
        expired_entries = 0
        
        for cache_key in self.cache_timestamps:
            if self._is_cache_expired(cache_key):
                expired_entries += 1
            else:
                active_entries += 1
        
        return {
            "total_entries": len(self.cache),
            "active_entries": active_entries,
            "expired_entries": expired_entries,
            "cache_enabled": self.config.enable_caching,
            "ttl_minutes": self.config.cache_ttl_minutes
        }
