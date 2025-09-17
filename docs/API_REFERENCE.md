# API Reference

This document provides comprehensive API reference for the Dimensional Processing Library.

## Table of Contents

- [SCDProcessor](#scdprocessor)
- [HistoricalDataDeduplicator](#historicaldatadeduplicator)
- [DimensionalKeyResolver](#dimensionalkeyresolver)
- [Configuration Classes](#configuration-classes)
- [Exception Classes](#exception-classes)
- [Utility Functions](#utility-functions)

## SCDProcessor

Main orchestrator for SCD Type 2 processing.

### Constructor

```python
SCDProcessor(config: SCDConfig, spark: SparkSession)
```

**Parameters:**
- `config` (SCDConfig): SCD configuration
- `spark` (SparkSession): Spark session

**Example:**
```python
from libraries.dimensional_processing import SCDProcessor, SCDConfig

config = SCDConfig(
    target_table="gold.customer_dim",
    business_key_columns=["customer_id"],
    scd_columns=["name", "email"]
)

processor = SCDProcessor(config, spark)
```

### Methods

#### `process_scd(source_df: DataFrame) -> ProcessingMetrics`

Main entry point for SCD Type 2 processing.

**Parameters:**
- `source_df` (DataFrame): Source DataFrame with data to process

**Returns:**
- `ProcessingMetrics`: Processing metrics and status

**Raises:**
- `SCDValidationError`: If input data validation fails
- `SCDProcessingError`: If processing fails

**Example:**
```python
result = processor.process_scd(source_df)
print(f"Processed {result.records_processed} records")
print(f"Created {result.new_records_created} new records")
print(f"Updated {result.existing_records_updated} existing records")
```

#### `process_incremental(source_df: DataFrame, last_processed_timestamp: str) -> ProcessingMetrics`

Process only new/changed records since last run.

**Parameters:**
- `source_df` (DataFrame): Source DataFrame
- `last_processed_timestamp` (str): Last processed timestamp

**Returns:**
- `ProcessingMetrics`: Processing metrics

**Example:**
```python
result = processor.process_incremental(source_df, "2024-01-01 10:00:00")
```

#### `get_table_info() -> Dict[str, Any]`

Get information about the target table.

**Returns:**
- `Dict[str, Any]`: Table information including record counts

**Example:**
```python
info = processor.get_table_info()
print(f"Total records: {info['total_records']}")
print(f"Current records: {info['current_records']}")
```

#### `validate_table_schema() -> bool`

Validate that target table has required SCD columns.

**Returns:**
- `bool`: True if schema is valid, False otherwise

**Example:**
```python
is_valid = processor.validate_table_schema()
if not is_valid:
    print("Table schema validation failed")
```

#### `create_target_table_if_not_exists(schema: Dict[str, str]) -> None`

Create target table if it doesn't exist.

**Parameters:**
- `schema` (Dict[str, str]): Table schema definition

**Raises:**
- `SCDProcessingError`: If table creation fails

**Example:**
```python
schema = {
    "customer_id": "STRING",
    "name": "STRING",
    "email": "STRING"
}
processor.create_target_table_if_not_exists(schema)
```

## HistoricalDataDeduplicator

Deduplicates historical data before SCD processing.

### Constructor

```python
HistoricalDataDeduplicator(config: DeduplicationConfig, spark: SparkSession)
```

**Parameters:**
- `config` (DeduplicationConfig): Deduplication configuration
- `spark` (SparkSession): Spark session

**Example:**
```python
from libraries.dimensional_processing import HistoricalDataDeduplicator, DeduplicationConfig

config = DeduplicationConfig(
    business_key_columns=["customer_id"],
    scd_columns=["name", "email"],
    effective_from_column="created_ts",
    deduplication_strategy="latest"
)

deduplicator = HistoricalDataDeduplicator(config, spark)
```

### Methods

#### `deduplicate_historical_data(source_df: DataFrame) -> DataFrame`

Deduplicate historical data using SCD hash-based approach.

**Parameters:**
- `source_df` (DataFrame): Historical data with potential duplicates

**Returns:**
- `DataFrame`: Deduplicated DataFrame ready for SCD processing

**Raises:**
- `DeduplicationError`: If deduplication fails

**Example:**
```python
deduplicated_df = deduplicator.deduplicate_historical_data(historical_df)
print(f"Original records: {historical_df.count()}")
print(f"Deduplicated records: {deduplicated_df.count()}")
```

#### `get_deduplication_stats(original_df: DataFrame, deduplicated_df: DataFrame) -> Dict[str, Any]`

Get deduplication statistics.

**Parameters:**
- `original_df` (DataFrame): Original DataFrame
- `deduplicated_df` (DataFrame): Deduplicated DataFrame

**Returns:**
- `Dict[str, Any]`: Deduplication statistics

**Example:**
```python
stats = deduplicator.get_deduplication_stats(original_df, deduplicated_df)
print(f"Deduplication ratio: {stats['deduplication_ratio']:.2%}")
```

## DimensionalKeyResolver

Resolves dimensional keys for fact tables.

### Constructor

```python
DimensionalKeyResolver(config: KeyResolutionConfig, spark: SparkSession)
```

**Parameters:**
- `config` (KeyResolutionConfig): Key resolution configuration
- `spark` (SparkSession): Spark session

**Example:**
```python
from libraries.dimensional_processing import DimensionalKeyResolver, KeyResolutionConfig

config = KeyResolutionConfig(
    dimension_table="gold.customer_dim",
    business_key_columns=["customer_id"],
    surrogate_key_column="customer_sk"
)

resolver = DimensionalKeyResolver(config, spark)
```

### Methods

#### `resolve_keys(fact_df: DataFrame, business_date_column: str) -> DataFrame`

Resolve dimensional keys for fact table.

**Parameters:**
- `fact_df` (DataFrame): Fact table DataFrame
- `business_date_column` (str): Column containing business date

**Returns:**
- `DataFrame`: DataFrame with resolved surrogate keys

**Raises:**
- `KeyResolutionError`: If key resolution fails

**Example:**
```python
resolved_df = resolver.resolve_keys(fact_df, "transaction_date")
print(f"Resolved keys for {resolved_df.count()} fact records")
```

#### `batch_resolve_keys(fact_df: DataFrame, business_date_column: str, batch_size: int = None) -> DataFrame`

Resolve keys in batches for large datasets.

**Parameters:**
- `fact_df` (DataFrame): Fact table DataFrame
- `business_date_column` (str): Column containing business date
- `batch_size` (int, optional): Batch size (uses config default if not specified)

**Returns:**
- `DataFrame`: DataFrame with resolved surrogate keys

**Example:**
```python
resolved_df = resolver.batch_resolve_keys(fact_df, "transaction_date", batch_size=100000)
```

#### `get_resolution_stats(fact_df: DataFrame, resolved_df: DataFrame) -> Dict[str, Any]`

Get key resolution statistics.

**Parameters:**
- `fact_df` (DataFrame): Original fact DataFrame
- `resolved_df` (DataFrame): Resolved DataFrame

**Returns:**
- `Dict[str, Any]`: Resolution statistics

**Example:**
```python
stats = resolver.get_resolution_stats(fact_df, resolved_df)
print(f"Resolution rate: {stats['resolution_stats']['resolution_rate']:.2%}")
```

#### `clear_cache() -> None`

Clear all cached data.

**Example:**
```python
resolver.clear_cache()
```

#### `get_cache_stats() -> Dict[str, Any]`

Get cache statistics.

**Returns:**
- `Dict[str, Any]`: Cache statistics

**Example:**
```python
cache_stats = resolver.get_cache_stats()
print(f"Cache enabled: {cache_stats['cache_enabled']}")
```

## Configuration Classes

### SCDConfig

Configuration for SCD Type 2 processing.

```python
@dataclass
class SCDConfig:
    # Required parameters
    target_table: str
    business_key_columns: List[str]
    scd_columns: List[str]
    
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
```

**Example:**
```python
config = SCDConfig(
    target_table="gold.customer_dim",
    business_key_columns=["customer_id"],
    scd_columns=["name", "email", "address"],
    effective_from_column="last_modified_ts",
    initial_effective_from_column="created_ts",
    batch_size=500000,
    enable_optimization=True,
    hash_algorithm="sha256"
)
```

### DeduplicationConfig

Configuration for historical data deduplication.

```python
@dataclass
class DeduplicationConfig:
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
    custom_deduplication_logic: Optional[callable] = None
    
    # Performance settings
    batch_size: int = 100000
    enable_optimization: bool = True
```

**Example:**
```python
config = DeduplicationConfig(
    business_key_columns=["customer_id"],
    scd_columns=["name", "email", "address"],
    effective_from_column="created_ts",
    deduplication_strategy="latest",
    batch_size=500000
)
```

### KeyResolutionConfig

Configuration for dimensional key resolution.

```python
@dataclass
class KeyResolutionConfig:
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
```

**Example:**
```python
config = KeyResolutionConfig(
    dimension_table="gold.customer_dim",
    business_key_columns=["customer_id"],
    surrogate_key_column="customer_sk",
    enable_caching=True,
    cache_ttl_minutes=120
)
```

## Exception Classes

### DimensionalProcessingError

Base exception for dimensional processing library.

```python
class DimensionalProcessingError(Exception):
    def __init__(self, message: str, error_code: str = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
```

### SCDValidationError

Exception raised when SCD data validation fails.

```python
class SCDValidationError(DimensionalProcessingError):
    def __init__(self, message: str, validation_errors: list = None):
        super().__init__(message, "SCD_VALIDATION_ERROR")
        self.validation_errors = validation_errors or []
```

### SCDProcessingError

Exception raised when SCD processing fails.

```python
class SCDProcessingError(DimensionalProcessingError):
    def __init__(self, message: str, processing_step: str = None):
        super().__init__(message, "SCD_PROCESSING_ERROR")
        self.processing_step = processing_step
```

### DeduplicationError

Exception raised when deduplication fails.

```python
class DeduplicationError(DimensionalProcessingError):
    def __init__(self, message: str, deduplication_strategy: str = None):
        super().__init__(message, "DEDUPLICATION_ERROR")
        self.deduplication_strategy = deduplication_strategy
```

### KeyResolutionError

Exception raised when key resolution fails.

```python
class KeyResolutionError(DimensionalProcessingError):
    def __init__(self, message: str, resolution_step: str = None):
        super().__init__(message, "KEY_RESOLUTION_ERROR")
        self.resolution_step = resolution_step
```

## Utility Functions

### `convert_to_utc(df: DataFrame, date_columns: List[str], source_timezone: str) -> DataFrame`

Convert date columns from source timezone to UTC.

**Parameters:**
- `df` (DataFrame): Input DataFrame
- `date_columns` (List[str]): List of date column names to convert
- `source_timezone` (str): Source timezone (e.g., 'America/New_York')

**Returns:**
- `DataFrame`: DataFrame with UTC converted date columns

**Example:**
```python
from libraries.dimensional_processing.common.utils import convert_to_utc

df_utc = convert_to_utc(df, ["created_ts", "modified_ts"], "America/New_York")
```

### `validate_dataframe_schema(df: DataFrame, required_columns: List[str]) -> bool`

Validate that DataFrame contains all required columns.

**Parameters:**
- `df` (DataFrame): Input DataFrame
- `required_columns` (List[str]): List of required column names

**Returns:**
- `bool`: True if all required columns exist, False otherwise

**Example:**
```python
from libraries.dimensional_processing.common.utils import validate_dataframe_schema

required_columns = ["customer_id", "name", "email"]
is_valid = validate_dataframe_schema(df, required_columns)
```

### `add_error_columns(df: DataFrame, error_flag_column: str = "_error_flag", error_message_column: str = "_error_message") -> DataFrame`

Add error tracking columns to DataFrame.

**Parameters:**
- `df` (DataFrame): Input DataFrame
- `error_flag_column` (str): Name of error flag column
- `error_message_column` (str): Name of error message column

**Returns:**
- `DataFrame`: DataFrame with error tracking columns

**Example:**
```python
from libraries.dimensional_processing.common.utils import add_error_columns

df_with_errors = add_error_columns(df)
```

### `flag_error_records(df: DataFrame, condition, error_message: str, error_flag_column: str = "_error_flag", error_message_column: str = "_error_message") -> DataFrame`

Flag records that meet error condition.

**Parameters:**
- `df` (DataFrame): Input DataFrame
- `condition`: Condition to identify error records
- `error_message` (str): Error message to set
- `error_flag_column` (str): Name of error flag column
- `error_message_column` (str): Name of error message column

**Returns:**
- `DataFrame`: DataFrame with error flags updated

**Example:**
```python
from libraries.dimensional_processing.common.utils import flag_error_records
from pyspark.sql.functions import col

df_flagged = flag_error_records(df, col("customer_id").isNull(), "Null customer ID")
```

### `get_processing_metrics(df: DataFrame, start_time: float, end_time: float) -> Dict[str, Any]`

Calculate processing metrics for a DataFrame.

**Parameters:**
- `df` (DataFrame): Input DataFrame
- `start_time` (float): Processing start time
- `end_time` (float): Processing end time

**Returns:**
- `Dict[str, Any]`: Dictionary with processing metrics

**Example:**
```python
from libraries.dimensional_processing.common.utils import get_processing_metrics
import time

start_time = time.time()
# ... processing ...
end_time = time.time()

metrics = get_processing_metrics(df, start_time, end_time)
print(f"Processing time: {metrics['processing_time_seconds']:.2f} seconds")
```

### `optimize_dataframe_for_processing(df: DataFrame, target_partitions: Optional[int] = None) -> DataFrame`

Optimize DataFrame for processing by repartitioning and caching.

**Parameters:**
- `df` (DataFrame): Input DataFrame
- `target_partitions` (int, optional): Target number of partitions

**Returns:**
- `DataFrame`: Optimized DataFrame

**Example:**
```python
from libraries.dimensional_processing.common.utils import optimize_dataframe_for_processing

optimized_df = optimize_dataframe_for_processing(df, target_partitions=200)
```

### `create_empty_dataframe_with_schema(spark: SparkSession, schema: StructType) -> DataFrame`

Create an empty DataFrame with specified schema.

**Parameters:**
- `spark` (SparkSession): Spark session
- `schema` (StructType): DataFrame schema

**Returns:**
- `DataFrame`: Empty DataFrame with specified schema

**Example:**
```python
from libraries.dimensional_processing.common.utils import create_empty_dataframe_with_schema
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True)
])

empty_df = create_empty_dataframe_with_schema(spark, schema)
```

### `log_dataframe_info(df: DataFrame, name: str) -> None`

Log DataFrame information for debugging.

**Parameters:**
- `df` (DataFrame): Input DataFrame
- `name` (str): Name for logging

**Example:**
```python
from libraries.dimensional_processing.common.utils import log_dataframe_info

log_dataframe_info(df, "Source Data")
```

### `validate_business_keys(df: DataFrame, business_key_columns: List[str]) -> List[str]`

Validate business key columns and return any issues.

**Parameters:**
- `df` (DataFrame): Input DataFrame
- `business_key_columns` (List[str]): List of business key column names

**Returns:**
- `List[str]`: List of validation error messages

**Example:**
```python
from libraries.dimensional_processing.common.utils import validate_business_keys

errors = validate_business_keys(df, ["customer_id"])
if errors:
    print(f"Validation errors: {errors}")
```

---

**Version**: 1.0.0  
**Last Updated**: 2024-01-15
