"""
Common utilities and configurations for dimensional processing library.
"""

from .config import SCDConfig, DeduplicationConfig, KeyResolutionConfig
from .exceptions import (
    SCDValidationError,
    SCDProcessingError,
    DeduplicationError,
    KeyResolutionError
)
from .utils import convert_to_utc, validate_dataframe_schema

__all__ = [
    "SCDConfig",
    "DeduplicationConfig", 
    "KeyResolutionConfig",
    "SCDValidationError",
    "SCDProcessingError",
    "DeduplicationError",
    "KeyResolutionError",
    "convert_to_utc",
    "validate_dataframe_schema"
]
