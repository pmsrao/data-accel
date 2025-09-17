# Usage Examples

This document provides comprehensive usage examples for the Dimensional Processing Library.

## Table of Contents

- [Basic SCD Type 2 Processing](#basic-scd-type-2-processing)
- [Historical Data Deduplication](#historical-data-deduplication)
- [Dimensional Key Resolution](#dimensional-key-resolution)
- [Complete Workflows](#complete-workflows)
- [Advanced Scenarios](#advanced-scenarios)
- [Error Handling](#error-handling)
- [Performance Optimization](#performance-optimization)

## Basic SCD Type 2 Processing

### Simple Customer Dimension

```python
from pyspark.sql import SparkSession
from libraries.dimensional_processing import SCDProcessor, SCDConfig

# Initialize Spark session
spark = SparkSession.builder.appName("SCD Processing").getOrCreate()

# Sample source data
source_data = [
    ("1", "John Doe", "john@example.com", "123 Main St", "2024-01-01 10:00:00", "2024-01-01 09:00:00"),
    ("2", "Jane Smith", "jane@example.com", "456 Oak Ave", "2024-01-01 11:00:00", "2024-01-01 10:00:00"),
    ("3", "Bob Johnson", "bob@example.com", "789 Pine St", "2024-01-01 12:00:00", "2024-01-01 11:00:00")
]

schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("address", StringType(), True),
    StructField("last_modified_ts", StringType(), True),
    StructField("created_ts", StringType(), True)
])

source_df = spark.createDataFrame(source_data, schema)

# Configure SCD processing
config = SCDConfig(
    target_table="gold.customer_dim",
    business_key_columns=["customer_id"],
    scd_columns=["name", "email", "address"],
    effective_from_column="last_modified_ts",
    initial_effective_from_column="created_ts"
)

# Process SCD
processor = SCDProcessor(config, spark)
result = processor.process_scd(source_df)

print(f"Processed {result.records_processed} records")
print(f"Created {result.new_records_created} new records")
print(f"Updated {result.existing_records_updated} existing records")
print(f"Processing time: {result.processing_time_seconds:.2f} seconds")
```

### Product Dimension with Multiple Business Keys

```python
# Sample product data with multiple business keys
product_data = [
    ("P001", "SKU001", "Laptop", "Electronics", "Dell", "2024-01-01 10:00:00"),
    ("P002", "SKU002", "Mouse", "Electronics", "Logitech", "2024-01-01 11:00:00"),
    ("P003", "SKU003", "Keyboard", "Electronics", "Microsoft", "2024-01-01 12:00:00")
]

schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("sku", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("last_modified_ts", StringType(), True)
])

product_df = spark.createDataFrame(product_data, schema)

# Configure with multiple business keys
config = SCDConfig(
    target_table="gold.product_dim",
    business_key_columns=["product_id", "sku"],  # Composite business key
    scd_columns=["product_name", "category", "brand"],
    effective_from_column="last_modified_ts"
)

processor = SCDProcessor(config, spark)
result = processor.process_scd(product_df)
```

### Incremental Processing

```python
# Process only new/changed records since last run
last_processed_timestamp = "2024-01-01 10:00:00"

# New data since last run
new_data = [
    ("1", "John Doe Updated", "john.updated@example.com", "123 Main St Updated", "2024-01-02 10:00:00", "2024-01-01 09:00:00"),
    ("4", "Alice Brown", "alice@example.com", "999 Cedar St", "2024-01-02 10:00:00", "2024-01-02 09:00:00")
]

new_df = spark.createDataFrame(new_data, schema)

# Process incrementally
result = processor.process_incremental(new_df, last_processed_timestamp)
```

## Historical Data Deduplication

### Basic Deduplication

```python
from libraries.dimensional_processing import HistoricalDataDeduplicator, DeduplicationConfig

# Sample historical data with duplicates
historical_data = [
    ("1", "John Doe", "john@example.com", "123 Main St", "2024-01-01 10:00:00"),
    ("1", "John Doe", "john@example.com", "123 Main St", "2024-01-01 11:00:00"),  # Duplicate
    ("1", "John Smith", "john.smith@example.com", "456 Oak Ave", "2024-01-01 12:00:00"),  # Different SCD
    ("2", "Jane Smith", "jane@example.com", "789 Pine St", "2024-01-01 10:00:00"),
    ("2", "Jane Smith", "jane@example.com", "789 Pine St", "2024-01-01 11:00:00"),  # Duplicate
    ("3", "Bob Johnson", "bob@example.com", "321 Elm St", "2024-01-01 10:00:00")
]

schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("address", StringType(), True),
    StructField("created_ts", StringType(), True)
])

historical_df = spark.createDataFrame(historical_data, schema)

# Configure deduplication
dedup_config = DeduplicationConfig(
    business_key_columns=["customer_id"],
    scd_columns=["name", "email", "address"],
    effective_from_column="created_ts",
    deduplication_strategy="latest"
)

# Deduplicate historical data
deduplicator = HistoricalDataDeduplicator(dedup_config, spark)
deduplicated_df = deduplicator.deduplicate_historical_data(historical_df)

print(f"Original records: {historical_df.count()}")
print(f"Deduplicated records: {deduplicated_df.count()}")
print(f"Duplicates removed: {historical_df.count() - deduplicated_df.count()}")
```

### Significant Records Deduplication

```python
# Sample data with significance column
significant_data = [
    ("1", "John Doe", "john@example.com", "123 Main St", "2024-01-01 10:00:00", "N"),
    ("1", "John Doe", "john@example.com", "123 Main St", "2024-01-01 11:00:00", "Y"),  # Significant
    ("2", "Jane Smith", "jane@example.com", "789 Pine St", "2024-01-01 10:00:00", "N"),
    ("2", "Jane Smith", "jane@example.com", "789 Pine St", "2024-01-01 11:00:00", "N")
]

schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("address", StringType(), True),
    StructField("created_ts", StringType(), True),
    StructField("is_significant", StringType(), True)
])

significant_df = spark.createDataFrame(significant_data, schema)

# Configure for significant records
dedup_config = DeduplicationConfig(
    business_key_columns=["customer_id"],
    scd_columns=["name", "email", "address"],
    effective_from_column="created_ts",
    deduplication_strategy="significant",
    significance_column="is_significant"
)

deduplicator = HistoricalDataDeduplicator(dedup_config, spark)
deduplicated_df = deduplicator.deduplicate_historical_data(significant_df)

# Should keep only significant records
print(f"Significant records kept: {deduplicated_df.count()}")
```

### Custom Deduplication Logic

```python
def custom_deduplication_logic(df):
    """Custom logic to keep records where status = 'ACTIVE'."""
    return df.filter(df.status == 'ACTIVE')

# Sample data with status column
status_data = [
    ("1", "John Doe", "john@example.com", "123 Main St", "2024-01-01 10:00:00", "ACTIVE"),
    ("1", "John Doe", "john@example.com", "123 Main St", "2024-01-01 11:00:00", "INACTIVE"),
    ("2", "Jane Smith", "jane@example.com", "789 Pine St", "2024-01-01 10:00:00", "ACTIVE")
]

schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("address", StringType(), True),
    StructField("created_ts", StringType(), True),
    StructField("status", StringType(), True)
])

status_df = spark.createDataFrame(status_data, schema)

# Configure with custom logic
dedup_config = DeduplicationConfig(
    business_key_columns=["customer_id"],
    scd_columns=["name", "email", "address"],
    effective_from_column="created_ts",
    deduplication_strategy="custom",
    custom_deduplication_logic=custom_deduplication_logic
)

deduplicator = HistoricalDataDeduplicator(dedup_config, spark)
deduplicated_df = deduplicator.deduplicate_historical_data(status_df)
```

## Dimensional Key Resolution

### Basic Key Resolution

```python
from libraries.dimensional_processing import DimensionalKeyResolver, KeyResolutionConfig

# Sample fact data
fact_data = [
    ("1", "2024-01-01 10:00:00", 100.0),
    ("2", "2024-01-01 11:00:00", 200.0),
    ("3", "2024-01-01 12:00:00", 300.0)
]

fact_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("amount", StringType(), True)
])

fact_df = spark.createDataFrame(fact_data, fact_schema)

# Configure key resolution
key_config = KeyResolutionConfig(
    dimension_table="gold.customer_dim",
    business_key_columns=["customer_id"],
    surrogate_key_column="customer_sk"
)

# Resolve keys
resolver = DimensionalKeyResolver(key_config, spark)
resolved_df = resolver.resolve_keys(fact_df, "transaction_date")

print(f"Resolved keys for {resolved_df.count()} fact records")
print(f"Unresolved keys: {resolved_df.filter(resolved_df.customer_sk.isNull()).count()}")
```

### Batch Key Resolution

```python
# For large datasets, use batch processing
resolved_df = resolver.batch_resolve_keys(fact_df, "transaction_date", batch_size=100000)
```

### Historical Key Resolution

```python
# Sample historical fact data
historical_fact_data = [
    ("1", "2020-01-01 10:00:00", 100.0),  # Historical date
    ("2", "2020-01-01 11:00:00", 200.0),
    ("3", "2020-01-01 12:00:00", 300.0)
]

historical_fact_df = spark.createDataFrame(historical_fact_data, fact_schema)

# Resolve historical keys
resolved_df = resolver.resolve_keys(historical_fact_df, "transaction_date")
```

## Complete Workflows

### Historical Dimension Build

```python
# Complete workflow for building dimension from historical data

# Step 1: Deduplicate historical data
dedup_config = DeduplicationConfig(
    business_key_columns=["customer_id"],
    scd_columns=["name", "email", "address"],
    effective_from_column="created_ts",
    deduplication_strategy="latest"
)

deduplicator = HistoricalDataDeduplicator(dedup_config, spark)
deduplicated_df = deduplicator.deduplicate_historical_data(historical_df)

# Step 2: Process SCD with deduplicated data
scd_config = SCDConfig(
    target_table="gold.customer_dim",
    business_key_columns=["customer_id"],
    scd_columns=["name", "email", "address"],
    effective_from_column="last_modified_ts",
    initial_effective_from_column="created_ts"
)

processor = SCDProcessor(scd_config, spark)
result = processor.process_scd(deduplicated_df)

# Step 3: Verify results
print(f"Historical dimension build completed:")
print(f"- Original records: {historical_df.count()}")
print(f"- After deduplication: {deduplicated_df.count()}")
print(f"- Final dimension records: {result.records_processed}")

# Step 4: Get table information
table_info = processor.get_table_info()
print(f"Table info: {table_info}")
```

### Fact Table Key Resolution

```python
# Complete workflow for resolving keys in fact table

# Step 1: Ensure dimension table exists and is populated
dimension_config = SCDConfig(
    target_table="gold.customer_dim",
    business_key_columns=["customer_id"],
    scd_columns=["name", "email", "address"],
    effective_from_column="last_modified_ts"
)

dimension_processor = SCDProcessor(dimension_config, spark)
dimension_processor.process_scd(customer_df)

# Step 2: Resolve keys for fact table
key_config = KeyResolutionConfig(
    dimension_table="gold.customer_dim",
    business_key_columns=["customer_id"],
    surrogate_key_column="customer_sk"
)

resolver = DimensionalKeyResolver(key_config, spark)
resolved_fact_df = resolver.resolve_keys(fact_df, "transaction_date")

# Step 3: Get resolution statistics
stats = resolver.get_resolution_stats(fact_df, resolved_fact_df)
print(f"Resolution rate: {stats['resolution_stats']['resolution_rate']:.2%}")
print(f"Unresolved records: {stats['resolution_stats']['unresolved_records']}")
```

## Advanced Scenarios

### Multiple Dimension Processing

```python
# Process multiple dimensions in sequence

dimensions = [
    {
        "name": "customer_dim",
        "config": SCDConfig(
            target_table="gold.customer_dim",
            business_key_columns=["customer_id"],
            scd_columns=["name", "email", "address"]
        ),
        "data": customer_df
    },
    {
        "name": "product_dim",
        "config": SCDConfig(
            target_table="gold.product_dim",
            business_key_columns=["product_id"],
            scd_columns=["product_name", "category", "brand"]
        ),
        "data": product_df
    }
]

results = {}
for dim in dimensions:
    processor = SCDProcessor(dim["config"], spark)
    result = processor.process_scd(dim["data"])
    results[dim["name"]] = result
    print(f"Processed {dim['name']}: {result.records_processed} records")
```

### Time Zone Handling

```python
from libraries.dimensional_processing.common.utils import convert_to_utc

# Convert timezone-aware data to UTC
df_utc = convert_to_utc(df, ["created_ts", "modified_ts"], "America/New_York")

# Process with UTC data
config = SCDConfig(
    target_table="gold.customer_dim",
    business_key_columns=["customer_id"],
    scd_columns=["name", "email"],
    effective_from_column="created_ts_utc"  # Use UTC column
)

processor = SCDProcessor(config, spark)
result = processor.process_scd(df_utc)
```

### Custom Hash Algorithm

```python
# Use MD5 instead of SHA-256 for faster processing
config = SCDConfig(
    target_table="gold.customer_dim",
    business_key_columns=["customer_id"],
    scd_columns=["name", "email"],
    hash_algorithm="md5"  # Use MD5 for faster hashing
)

processor = SCDProcessor(config, spark)
result = processor.process_scd(source_df)
```

## Error Handling

### Validation Error Handling

```python
from libraries.dimensional_processing.common.exceptions import SCDValidationError

try:
    result = processor.process_scd(source_df)
except SCDValidationError as e:
    print(f"Validation failed: {e.message}")
    print(f"Errors: {e.validation_errors}")
    
    # Handle validation errors
    for error in e.validation_errors:
        print(f"- {error}")
```

### Processing Error Handling

```python
from libraries.dimensional_processing.common.exceptions import SCDProcessingError

try:
    result = processor.process_scd(source_df)
except SCDProcessingError as e:
    print(f"Processing failed: {e.message}")
    print(f"Processing step: {e.processing_step}")
    
    # Handle processing errors
    if "merge" in e.processing_step:
        print("Merge operation failed - check data integrity")
    elif "validation" in e.processing_step:
        print("Validation failed - check input data")
```

### Deduplication Error Handling

```python
from libraries.dimensional_processing.common.exceptions import DeduplicationError

try:
    deduplicated_df = deduplicator.deduplicate_historical_data(historical_df)
except DeduplicationError as e:
    print(f"Deduplication failed: {e.message}")
    print(f"Strategy: {e.deduplication_strategy}")
    
    # Handle deduplication errors
    if e.deduplication_strategy == "significant":
        print("Check significance column configuration")
    elif e.deduplication_strategy == "custom":
        print("Check custom deduplication logic")
```

### Key Resolution Error Handling

```python
from libraries.dimensional_processing.common.exceptions import KeyResolutionError

try:
    resolved_df = resolver.resolve_keys(fact_df, "transaction_date")
except KeyResolutionError as e:
    print(f"Key resolution failed: {e.message}")
    print(f"Resolution step: {e.resolution_step}")
    
    # Handle key resolution errors
    if "validation" in e.resolution_step:
        print("Input validation failed - check fact table schema")
    elif "lookup" in e.resolution_step:
        print("Dimension lookup failed - check dimension table")
```

## Performance Optimization

### Large Dataset Processing

```python
# Optimize for large datasets
config = SCDConfig(
    target_table="gold.customer_dim",
    business_key_columns=["customer_id"],
    scd_columns=["name", "email"],
    batch_size=500000,  # Increase batch size
    enable_optimization=True
)

processor = SCDProcessor(config, spark)
result = processor.process_scd(large_df)
```

### Caching Optimization

```python
# Enable caching for key resolution
key_config = KeyResolutionConfig(
    dimension_table="gold.customer_dim",
    business_key_columns=["customer_id"],
    enable_caching=True,
    cache_ttl_minutes=120  # Increase cache TTL
)

resolver = DimensionalKeyResolver(key_config, spark)
resolved_df = resolver.resolve_keys(fact_df, "transaction_date")
```

### Memory Optimization

```python
from libraries.dimensional_processing.common.utils import optimize_dataframe_for_processing

# Optimize DataFrame for processing
optimized_df = optimize_dataframe_for_processing(source_df, target_partitions=200)

# Process optimized DataFrame
result = processor.process_scd(optimized_df)
```

### Monitoring and Metrics

```python
# Get processing metrics
result = processor.process_scd(source_df)
print(f"Processing metrics: {result.to_dict()}")

# Get cache statistics
cache_stats = resolver.get_cache_stats()
print(f"Cache statistics: {cache_stats}")

# Get table information
table_info = processor.get_table_info()
print(f"Table information: {table_info}")
```

---

**Version**: 1.0.0  
**Last Updated**: 2024-01-15
