"""
Dimensional Processing Library

A production-ready library for implementing SCD Type 2 dimensional tables
and dimensional key resolution in Databricks environments.

Main Components:
- SCDProcessor: Handles SCD Type 2 processing with hash-based change detection
- HistoricalDataDeduplicator: Deduplicates historical data before SCD processing
- DimensionalKeyResolver: Resolves dimensional keys for fact tables

Author: Data Engineering Team
Version: 1.0.0
"""

from .scd_type2.scd_processor import SCDProcessor
from .scd_type2.historical_data_deduplicator import HistoricalDataDeduplicator
from .key_resolution.key_resolver import DimensionalKeyResolver
from .common.config import SCDConfig, DeduplicationConfig, KeyResolutionConfig
from .common.exceptions import (
    SCDValidationError,
    SCDProcessingError,
    DeduplicationError,
    KeyResolutionError
)

__version__ = "1.0.0"
__author__ = "Data Engineering Team"

__all__ = [
    "SCDProcessor",
    "HistoricalDataDeduplicator", 
    "DimensionalKeyResolver",
    "SCDConfig",
    "DeduplicationConfig",
    "KeyResolutionConfig",
    "SCDValidationError",
    "SCDProcessingError",
    "DeduplicationError",
    "KeyResolutionError"
]
