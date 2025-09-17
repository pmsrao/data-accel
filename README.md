# Dimensional Processing Library

A production-ready library for implementing SCD Type 2 dimensional tables and dimensional key resolution in Databricks environments.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Components](#core-components)
- [Usage Examples](#usage-examples)
- [Configuration](#configuration)
- [API Reference](#api-reference)
- [Testing](#testing)
- [Performance](#performance)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

## Overview

The Dimensional Processing Library provides a comprehensive solution for building and maintaining dimensional tables in Databricks environments. It implements SCD Type 2 (Slowly Changing Dimension Type 2) processing with hash-based change detection, historical data deduplication, and dimensional key resolution for fact tables.

### Key Benefits

- **Production-Ready**: Built with enterprise-grade error handling and validation
- **Performance Optimized**: Designed for large-scale data processing with caching and optimization
- **Flexible**: Supports multiple deduplication strategies and configuration options
- **Clean Architecture**: Modular design with clear separation of concerns
- **Comprehensive Testing**: Extensive unit and integration test coverage
- **Databricks Optimized**: Leverages Delta Lake features for ACID compliance

## Features

### SCD Type 2 Processing
- Hash-based change detection using SHA-256 or MD5
- Automatic record versioning with effective dates
- Support for both incremental and full processing
- Comprehensive data validation and error handling

### Historical Data Deduplication
- Multiple deduplication strategies (latest, earliest, significant, custom)
- Hash-based duplicate detection
- Performance optimized for large datasets
- Validation of deduplication results

### Dimensional Key Resolution
- Efficient key resolution for fact tables
- Support for both current and historical lookups
- Caching mechanisms for performance optimization
- Batch processing for large datasets

### Data Quality & Validation
- Comprehensive input validation
- Error flagging and reporting
- Data integrity checks
- Processing metrics and monitoring

## Installation

### Prerequisites

- Python 3.8+
- PySpark 3.0+
- Databricks Runtime 10.0+
- Delta Lake 1.0+

### Install from Source

```bash
# Clone the repository
git clone <repository-url>
cd data-accel

# Install the library
pip install -e .
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

## Quick Start

### 1. Basic SCD Type 2 Processing

```python
from pyspark.sql import SparkSession
from libraries.dimensional_processing import SCDProcessor, SCDConfig

# Initialize Spark session
spark = SparkSession.builder.appName("SCD Processing").getOrCreate()

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
```

### 2. Historical Data Deduplication

```python
from libraries.dimensional_processing import HistoricalDataDeduplicator, DeduplicationConfig

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
```

### 3. Dimensional Key Resolution

```python
from libraries.dimensional_processing import DimensionalKeyResolver, KeyResolutionConfig

# Configure key resolution
key_config = KeyResolutionConfig(
    dimension_table="gold.customer_dim",
    business_key_columns=["customer_id"],
    surrogate_key_column="customer_sk"
)

# Resolve keys for fact table
resolver = DimensionalKeyResolver(key_config, spark)
resolved_df = resolver.resolve_keys(fact_df, "transaction_date")

print(f"Resolved keys for {resolved_df.count()} fact records")
```

## Core Components

### SCDProcessor

The main orchestrator for SCD Type 2 processing. Handles the complete lifecycle of dimensional record management.

**Key Features:**
- Hash-based change detection
- Automatic record versioning
- Comprehensive validation
- Performance optimization

### HistoricalDataDeduplicator

Deduplicates historical data before SCD processing. Essential for building dimensions from historical data sources.

**Deduplication Strategies:**
- `latest`: Keep the latest record per SCD hash
- `earliest`: Keep the earliest record per SCD hash
- `significant`: Keep records marked as significant
- `custom`: Apply custom business logic

### DimensionalKeyResolver

Resolves dimensional keys for fact tables. Optimized for performance with caching and batch processing.

**Key Features:**
- Current and historical key resolution
- Performance caching
- Batch processing support
- Comprehensive validation

## Usage Examples

### Complete Historical Dimension Build

```python
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

print(f"Historical dimension build completed:")
print(f"- Original records: {historical_df.count()}")
print(f"- After deduplication: {deduplicated_df.count()}")
print(f"- Final dimension records: {result.records_processed}")
```

### Incremental Processing

```python
# Process only new/changed records since last run
last_processed_timestamp = "2024-01-01 10:00:00"
result = processor.process_incremental(source_df, last_processed_timestamp)
```

### Batch Key Resolution

```python
# Resolve keys in batches for large datasets
resolved_df = resolver.batch_resolve_keys(fact_df, "transaction_date", batch_size=100000)
```

### Custom Deduplication Logic

```python
def custom_deduplication_logic(df):
    # Keep records where status = 'ACTIVE'
    return df.filter(df.status == 'ACTIVE')

dedup_config = DeduplicationConfig(
    business_key_columns=["customer_id"],
    scd_columns=["name", "email"],
    effective_from_column="created_ts",
    deduplication_strategy="custom",
    custom_deduplication_logic=custom_deduplication_logic
)
```

## Configuration

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

## API Reference

### SCDProcessor

#### `process_scd(source_df: DataFrame) -> ProcessingMetrics`

Main entry point for SCD Type 2 processing.

**Parameters:**
- `source_df`: Source DataFrame with data to process

**Returns:**
- `ProcessingMetrics`: Processing metrics and status

#### `process_incremental(source_df: DataFrame, last_processed_timestamp: str) -> ProcessingMetrics`

Process only new/changed records since last run.

**Parameters:**
- `source_df`: Source DataFrame
- `last_processed_timestamp`: Last processed timestamp

**Returns:**
- `ProcessingMetrics`: Processing metrics

### HistoricalDataDeduplicator

#### `deduplicate_historical_data(source_df: DataFrame) -> DataFrame`

Deduplicate historical data using SCD hash-based approach.

**Parameters:**
- `source_df`: Historical data with potential duplicates

**Returns:**
- `DataFrame`: Deduplicated DataFrame ready for SCD processing

### DimensionalKeyResolver

#### `resolve_keys(fact_df: DataFrame, business_date_column: str) -> DataFrame`

Resolve dimensional keys for fact table.

**Parameters:**
- `fact_df`: Fact table DataFrame
- `business_date_column`: Column containing business date

**Returns:**
- `DataFrame`: DataFrame with resolved surrogate keys

#### `batch_resolve_keys(fact_df: DataFrame, business_date_column: str, batch_size: int = None) -> DataFrame`

Resolve keys in batches for large datasets.

**Parameters:**
- `fact_df`: Fact table DataFrame
- `business_date_column`: Column containing business date
- `batch_size`: Batch size (uses config default if not specified)

**Returns:**
- `DataFrame`: DataFrame with resolved surrogate keys

## Testing

### Running Tests

```bash
# Run all tests
pytest tests/

# Run unit tests only
pytest tests/unit/

# Run integration tests only
pytest tests/integration/

# Run with coverage
pytest --cov=src/libraries/dimensional_processing tests/
```

### Test Structure

```
tests/
├── unit/                    # Unit tests
│   ├── test_scd_processor.py
│   ├── test_historical_data_deduplicator.py
│   ├── test_key_resolver.py
│   ├── test_hash_manager.py
│   ├── test_validators.py
│   └── test_config.py
├── integration/             # Integration tests
│   └── test_end_to_end_scd.py
└── fixtures/               # Test data fixtures
    ├── sample_dimensional_data.py
    └── sample_fact_data.py
```

### Test Coverage

The library maintains >90% test coverage across all modules:

- **Unit Tests**: Test individual components in isolation
- **Integration Tests**: Test end-to-end workflows
- **Performance Tests**: Validate performance requirements
- **Error Handling Tests**: Test error scenarios and edge cases

## Performance

### Performance Characteristics

- **Processing Speed**: <30 minutes for typical dimensional tables (up to 10M records)
- **Memory Usage**: Optimized for large datasets with efficient caching
- **Scalability**: Supports daily and hourly processing frequencies
- **Throughput**: Handles up to 10M records efficiently

### Optimization Features

- **Hash-based Change Detection**: Efficient change detection using SHA-256/MD5
- **Delta Lake Integration**: Leverages Delta Lake for ACID compliance and optimization
- **Caching Mechanisms**: Multi-level caching for frequently accessed data
- **Batch Processing**: Optimized batch processing for large datasets
- **Partitioning**: Support for time-based and hash-based partitioning

### Performance Tuning

```python
# Optimize for large datasets
config = SCDConfig(
    target_table="gold.customer_dim",
    business_key_columns=["customer_id"],
    scd_columns=["name", "email"],
    batch_size=500000,  # Increase batch size
    enable_optimization=True
)

# Enable caching for key resolution
key_config = KeyResolutionConfig(
    dimension_table="gold.customer_dim",
    business_key_columns=["customer_id"],
    enable_caching=True,
    cache_ttl_minutes=120  # Increase cache TTL
)
```

## Best Practices

### Data Modeling

1. **Business Key Design**: Choose stable, unique business keys
2. **SCD Column Selection**: Select columns that represent meaningful business changes
3. **Date Handling**: Use UTC timestamps for all SCD metadata columns
4. **Naming Conventions**: Follow consistent naming conventions for SCD columns

### Performance Optimization

1. **Partitioning**: Partition tables by effective date for better query performance
2. **Caching**: Enable caching for frequently accessed dimension tables
3. **Batch Processing**: Use batch processing for large datasets
4. **Optimization**: Enable Delta Lake optimization for better performance

### Error Handling

1. **Validation**: Always validate input data before processing
2. **Error Flagging**: Use error flagging to handle problematic records
3. **Monitoring**: Monitor processing metrics and error rates
4. **Retry Logic**: Implement retry logic for transient failures

### Data Quality

1. **Null Handling**: Handle null values appropriately in business keys
2. **Duplicate Detection**: Use deduplication for historical data
3. **Data Integrity**: Validate data integrity after processing
4. **Audit Trail**: Maintain audit trails for data lineage

## Troubleshooting

### Common Issues

#### Validation Errors

**Problem**: SCDValidationError with missing columns
**Solution**: Ensure all required columns are present in source data

```python
# Check required columns
required_columns = config.business_key_columns + config.scd_columns
missing_columns = set(required_columns) - set(source_df.columns)
if missing_columns:
    print(f"Missing columns: {missing_columns}")
```

#### Performance Issues

**Problem**: Slow processing for large datasets
**Solution**: Optimize configuration and enable caching

```python
# Optimize configuration
config = SCDConfig(
    target_table="gold.customer_dim",
    business_key_columns=["customer_id"],
    scd_columns=["name", "email"],
    batch_size=500000,  # Increase batch size
    enable_optimization=True
)
```

#### Memory Issues

**Problem**: Out of memory errors
**Solution**: Reduce batch size and enable optimization

```python
# Reduce batch size
config.batch_size = 50000
config.enable_optimization = True
```

### Debugging

#### Enable Logging

```python
import logging
logging.basicConfig(level=logging.INFO)
```

#### Check Processing Metrics

```python
result = processor.process_scd(source_df)
print(f"Processing metrics: {result.to_dict()}")
```

#### Validate Table Schema

```python
is_valid = processor.validate_table_schema()
if not is_valid:
    print("Table schema validation failed")
```

## Contributing

### Development Setup

```bash
# Clone repository
git clone <repository-url>
cd data-accel

# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/

# Run linting
flake8 src/
black src/
```

### Code Style

- Follow PEP 8 style guidelines
- Use type hints for all functions
- Write comprehensive docstrings
- Maintain >90% test coverage

### Pull Request Process

1. Fork the repository
2. Create a feature branch
3. Make changes with tests
4. Ensure all tests pass
5. Submit a pull request

## Documentation

Complete documentation is available in the [`docs/`](docs/) directory:

### 📚 Core Documentation
- **[API Reference](docs/API_REFERENCE.md)** - Complete API documentation
- **[Usage Examples](docs/USAGE_EXAMPLES.md)** - Comprehensive usage examples
- **[Library Specification](docs/SCD_LIBRARY_SPECIFICATION.md)** - Technical specification

### 🚀 Deployment & Setup
- **[Databricks Setup](docs/DATABRICKS_SETUP.md)** - Step-by-step Databricks setup
- **[Deployment Summary](docs/DEPLOYMENT_SUMMARY.md)** - Deployment status and next steps

### 📁 Documentation Index
- **[Documentation Index](docs/README.md)** - Complete documentation guide

### 📝 Ad-hoc Documentation
- **[adhoc/](docs/adhoc/)** - Ad-hoc documentation and notes

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For support and questions:

- Create an issue in the repository
- Check the [troubleshooting](#troubleshooting) section
- Review the [API reference](docs/API_REFERENCE.md)
- See [Documentation Index](docs/README.md) for complete guide

---

**Version**: 1.0.0  
**Author**: Data Engineering Team  
**Last Updated**: 2024-01-15
