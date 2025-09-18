"""
Hash management utilities for SCD processing.
"""

from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, concat_ws, md5
import logging

from ..common.config import SCDConfig

logger = logging.getLogger(__name__)


class HashManager:
    """Manages hash computation and comparison for SCD processing."""
    
    def __init__(self, config: SCDConfig):
        """
        Initialize HashManager with configuration.
        
        Args:
            config: SCD configuration
        """
        self.config = config
        self.hash_algorithm = config.hash_algorithm.lower()
        
        # Validate hash algorithm
        if self.hash_algorithm not in ["sha256", "md5"]:
            raise ValueError(f"Unsupported hash algorithm: {self.hash_algorithm}")
    
    def compute_scd_hash(self, df: DataFrame) -> DataFrame:
        """
        Compute SCD hash for the specified columns.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with SCD hash column added
        """
        # Get unique SCD columns (remove duplicates if business keys are also in SCD columns)
        unique_scd_columns = list(dict.fromkeys(self.config.scd_columns))  # Preserves order, removes duplicates
        hash_columns = [col(c) for c in unique_scd_columns]
        
        if self.hash_algorithm == "sha256":
            hash_expr = sha2(concat_ws("|", *hash_columns), 256)
        elif self.hash_algorithm == "md5":
            hash_expr = md5(concat_ws("|", *hash_columns))
        else:
            raise ValueError(f"Unsupported hash algorithm: {self.hash_algorithm}")
        
        result_df = df.withColumn(self.config.scd_hash_column, hash_expr)
        
        logger.info(f"Computed {self.hash_algorithm} hash for {len(unique_scd_columns)} unique SCD columns")
        return result_df
    
    def compute_business_key_hash(self, df: DataFrame) -> DataFrame:
        """
        Compute hash for business key columns.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with business key hash column added
        """
        hash_columns = [col(c) for c in self.config.business_key_columns]
        
        if self.hash_algorithm == "sha256":
            hash_expr = sha2(concat_ws("|", *hash_columns), 256)
        elif self.hash_algorithm == "md5":
            hash_expr = md5(concat_ws("|", *hash_columns))
        else:
            raise ValueError(f"Unsupported hash algorithm: {self.hash_algorithm}")
        
        result_df = df.withColumn("business_key_hash", hash_expr)
        
        logger.info(f"Computed {self.hash_algorithm} hash for {len(self.config.business_key_columns)} business key columns")
        return result_df
    
    def compare_hashes(self, hash1: str, hash2: str) -> bool:
        """
        Compare two hash values.
        
        Args:
            hash1: First hash value
            hash2: Second hash value
            
        Returns:
            True if hashes are equal, False otherwise
        """
        return hash1 == hash2
    
    def get_hash_columns(self) -> List[str]:
        """
        Get list of columns used for hash computation.
        
        Returns:
            List of unique column names
        """
        return list(dict.fromkeys(self.config.scd_columns))  # Preserves order, removes duplicates
    
    def get_business_key_columns(self) -> List[str]:
        """
        Get list of business key columns.
        
        Returns:
            List of business key column names
        """
        return self.config.business_key_columns.copy()
