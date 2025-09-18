"""
Configuration classes for dimensional processing library.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Callable
from enum import Enum


class DeduplicationStrategy(Enum):
    """Enumeration of available deduplication strategies."""
    LATEST = "latest"
    EARLIEST = "earliest"
    SIGNIFICANT = "significant"
    CUSTOM = "custom"


@dataclass
class SCDConfig:
    """Configuration for SCD Type 2 processing."""
    
    # Required parameters
    target_table: str
    business_key_columns: List[str]
    scd_columns: List[str]
    surrogate_key_column: str  # Surrogate key column name (e.g., customer_sk, product_sk, etc.)
    
    # Optional parameters
    effective_from_column: Optional[str] = None
    initial_effective_from_column: Optional[str] = None
    
    # Standard column names
    scd_hash_column: str = "scd_hash"
    effective_start_column: str = "effective_start_ts_utc"
    effective_end_column: str = "effective_end_ts_utc"
    is_current_column: str = "is_current"
    created_ts_column: str = "created_ts_utc"
    modified_ts_column: str = "modified_ts_utc"
    
    # Performance settings
    batch_size: int = 100000
    enable_optimization: bool = True
    hash_algorithm: str = "sha256"
    
    # Error handling
    error_flag_column: str = "_error_flag"
    error_message_column: str = "_error_message"
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.target_table:
            raise ValueError("target_table is required")
        if not self.business_key_columns:
            raise ValueError("business_key_columns cannot be empty")
        if not self.scd_columns:
            raise ValueError("scd_columns cannot be empty")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")


@dataclass
class DeduplicationConfig:
    """Configuration for historical data deduplication."""
    
    # Required parameters
    business_key_columns: List[str]
    scd_columns: List[str]
    effective_from_column: str
    
    # Deduplication strategy
    deduplication_strategy: str = "latest"
    
    # Standard column names
    scd_hash_column: str = "scd_hash"
    significance_column: Optional[str] = None
    
    # Custom deduplication logic
    custom_deduplication_logic: Optional[Callable] = None
    
    # Performance settings
    batch_size: int = 100000
    enable_optimization: bool = True
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.business_key_columns:
            raise ValueError("business_key_columns cannot be empty")
        if not self.scd_columns:
            raise ValueError("scd_columns cannot be empty")
        if not self.effective_from_column:
            raise ValueError("effective_from_column is required")
        
        # Validate deduplication strategy
        valid_strategies = [strategy.value for strategy in DeduplicationStrategy]
        if self.deduplication_strategy not in valid_strategies:
            raise ValueError(f"deduplication_strategy must be one of {valid_strategies}")
        
        # Validate significance column for significant strategy
        if (self.deduplication_strategy == "significant" and 
            not self.significance_column):
            raise ValueError("significance_column is required for 'significant' strategy")
        
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")


@dataclass
class KeyResolutionConfig:
    """Configuration for dimensional key resolution."""
    
    # Required parameters
    dimension_table: str
    business_key_columns: List[str]
    
    # Standard column names
    surrogate_key_column: str = "surrogate_key"
    effective_start_column: str = "effective_start_ts_utc"
    effective_end_column: str = "effective_end_ts_utc"
    is_current_column: str = "is_current"
    
    # Performance settings
    enable_caching: bool = True
    cache_ttl_minutes: int = 60
    batch_size: int = 100000
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.dimension_table:
            raise ValueError("dimension_table is required")
        if not self.business_key_columns:
            raise ValueError("business_key_columns cannot be empty")
        if self.cache_ttl_minutes <= 0:
            raise ValueError("cache_ttl_minutes must be positive")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")


@dataclass
class ProcessingMetrics:
    """Metrics for processing operations."""
    
    records_processed: int = 0
    new_records_created: int = 0
    existing_records_updated: int = 0
    records_with_errors: int = 0
    processing_time_seconds: float = 0.0
    memory_usage_mb: float = 0.0
    partition_count: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            "records_processed": self.records_processed,
            "new_records_created": self.new_records_created,
            "existing_records_updated": self.existing_records_updated,
            "records_with_errors": self.records_with_errors,
            "processing_time_seconds": self.processing_time_seconds,
            "memory_usage_mb": self.memory_usage_mb,
            "partition_count": self.partition_count
        }


@dataclass
class ValidationResult:
    """Result of data validation."""
    
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    def add_error(self, error: str) -> None:
        """Add an error message."""
        self.errors.append(error)
        self.is_valid = False
    
    def add_warning(self, warning: str) -> None:
        """Add a warning message."""
        self.warnings.append(warning)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert validation result to dictionary."""
        return {
            "is_valid": self.is_valid,
            "errors": self.errors,
            "warnings": self.warnings
        }
