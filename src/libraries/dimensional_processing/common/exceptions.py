"""
Custom exceptions for dimensional processing library.
"""


class DimensionalProcessingError(Exception):
    """Base exception for dimensional processing library."""
    
    def __init__(self, message: str, error_code: str = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code


class SCDValidationError(DimensionalProcessingError):
    """Exception raised when SCD data validation fails."""
    
    def __init__(self, message: str, validation_errors: list = None):
        super().__init__(message, "SCD_VALIDATION_ERROR")
        self.validation_errors = validation_errors or []


class SCDProcessingError(DimensionalProcessingError):
    """Exception raised when SCD processing fails."""
    
    def __init__(self, message: str, processing_step: str = None):
        super().__init__(message, "SCD_PROCESSING_ERROR")
        self.processing_step = processing_step


class DeduplicationError(DimensionalProcessingError):
    """Exception raised when deduplication fails."""
    
    def __init__(self, message: str, deduplication_strategy: str = None):
        super().__init__(message, "DEDUPLICATION_ERROR")
        self.deduplication_strategy = deduplication_strategy


class KeyResolutionError(DimensionalProcessingError):
    """Exception raised when key resolution fails."""
    
    def __init__(self, message: str, resolution_step: str = None):
        super().__init__(message, "KEY_RESOLUTION_ERROR")
        self.resolution_step = resolution_step


class ConfigurationError(DimensionalProcessingError):
    """Exception raised when configuration is invalid."""
    
    def __init__(self, message: str, config_field: str = None):
        super().__init__(message, "CONFIGURATION_ERROR")
        self.config_field = config_field
