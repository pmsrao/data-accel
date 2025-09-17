# SCD Type 2 Library - Complete Specification Document

## Table of Contents
1. [Project Overview](#project-overview)
2. [Core Requirements](#core-requirements)
3. [Technical Architecture](#technical-architecture)
4. [Data Quality and Validation](#data-quality-and-validation)
5. [Performance and Scalability](#performance-and-scalability)
6. [Business Logic Requirements](#business-logic-requirements)
7. [Integration Requirements](#integration-requirements)
8. [Advanced Features](#advanced-features)
9. [Implementation Guidelines](#implementation-guidelines)
10. [Testing Strategy](#testing-strategy)

---

## Project Overview

### Purpose
Build a production-ready library for implementing SCD Type 2 dimensional tables in Databricks environment with dimensional key resolution capabilities for fact tables.

### Context
- **Environment**: Databricks with Delta Lake
- **Language**: Python/PySpark
- **Architecture**: Clean code practices with modular design
- **Target Users**: Data Engineering teams building dimensional models

### Existing Foundation
- Initial SCD library implementation exists in `resources/scd_handler.py`
- Hybrid SCD Type 1/Type 2 approach with hash-based change detection
- Delta Lake merge operations for ACID compliance

---

## Core Requirements

### 1. SCD Type 2 Processing

#### Primary Function: `process_scd()`
**Purpose**: Create/merge/update SCD records based on business key and hash comparison

**Input Parameters**:
```python
def process_scd(
    dataframe: DataFrame,
    target_table: str,
    bk_columns: List[str],                    # Business key columns
    scd_columns: List[str],                   # Columns tracked for SCD Type 2
    effective_from_col: Optional[str],        # Column for effective start date
    initial_effective_from_col: Optional[str] # Column for initial effective date
) -> SCDProcessingResult
```

#### Standard SCD Attribute Names
```sql
-- SCD-specific columns (mandatory)
scd_hash STRING                              -- Hash of SCD columns
effective_start_ts_utc TIMESTAMP            -- When record became effective
effective_end_ts_utc TIMESTAMP              -- When record became inactive
is_current STRING                            -- 'Y' for current, 'N' for historical

-- Standard audit columns
created_ts_utc TIMESTAMP                     -- Record creation timestamp
modified_ts_utc TIMESTAMP                    -- Last modification timestamp
```

#### Core Logic
1. **Look for existing record** with business key
2. **If found**:
   - Compute `scd_hash` from `scd_columns`
   - **If hash is same**: Update existing record (no new version)
   - **If hash is different**: 
     - Create new version
     - Update previous record with `effective_end_ts_utc` and `is_current = 'N'`
3. **If not found**: Create new record using `initial_effective_from_col` or `effective_from_col`

### 2. Historical Data Deduplication

#### Primary Function: `deduplicate_historical_data()`
**Purpose**: Deduplicate historical records before SCD processing when building dimensions from historical data

**Input Parameters**:
```python
def deduplicate_historical_data(
    dataframe: DataFrame,                     # Historical data with potential duplicates
    bk_columns: List[str],                   # Business key columns
    scd_columns: List[str],                  # Columns tracked for SCD Type 2
    effective_from_col: str,                 # Column for effective start date
    deduplication_strategy: str = "latest"   # Strategy: "latest", "earliest", "significant"
) -> DataFrame
```

#### Deduplication Logic
- **Scenario**: Building dimension from historical data with multiple records per business key
- **Problem**: Some records are insignificant, some are significant changes
- **Solution**: Deduplicate using `scd_hash` to identify unique versions
- **Process**: 
  1. Compute `scd_hash` for all records
  2. Group by business key and `scd_hash`
  3. Apply deduplication strategy (latest, earliest, or significant)
  4. Return deduplicated records ready for SCD processing

#### Deduplication Strategies
```python
class DeduplicationStrategy(Enum):
    LATEST = "latest"           # Keep the latest record per scd_hash
    EARLIEST = "earliest"       # Keep the earliest record per scd_hash
    SIGNIFICANT = "significant" # Keep records marked as significant
    CUSTOM = "custom"           # Custom logic based on business rules
```

### 3. Dimensional Key Resolution

#### Primary Function: `resolve_dimensional_keys()`
**Purpose**: Resolve surrogate keys for fact tables based on business keys and business dates

**Input Parameters**:
```python
def resolve_dimensional_keys(
    dataframe: DataFrame,                     # Fact table data
    bk_column: str,                          # Business key column in fact table
    effective_from_column: str,              # Business date column
    dim_table: str,                          # Dimension table name
    bk_column_dim: str                       # Business key column in dimension
) -> DataFrame
```

#### Key Resolution Logic
- Takes unique business keys and business dates as arguments
- Returns resolved dimensional keys (surrogate keys)
- Handles both current and historical record lookups
- Optimized for performance with caching mechanisms

---

## Technical Architecture

### Project Structure
```
src/libraries/dimensional_processing/
├── __init__.py
├── scd_type2/
│   ├── __init__.py
│   ├── scd_processor.py          # Main SCD orchestrator
│   ├── hash_manager.py           # Hash computation and comparison
│   ├── record_manager.py         # Record lifecycle management
│   ├── date_manager.py           # Date handling utilities
│   └── validators.py             # Data validation
├── key_resolution/
│   ├── __init__.py
│   ├── key_resolver.py           # Main key resolution orchestrator
│   ├── lookup_manager.py         # Dimension lookup operations
│   └── cache_manager.py          # Performance optimization
└── common/
    ├── __init__.py
    ├── config.py                 # Configuration management
    ├── exceptions.py             # Custom exceptions
    └── utils.py                  # Common utilities
```

### Core Classes

#### 1. SCDProcessor
```python
class SCDProcessor:
    def __init__(self, config: SCDConfig, spark: SparkSession)
    def process_scd(self, source_df: DataFrame) -> SCDProcessingResult
    def _prepare_source_data(self, source_df: DataFrame) -> DataFrame
    def _get_current_records(self) -> DataFrame
    def _create_change_plan(self, source_df: DataFrame, current_df: DataFrame) -> ChangePlan
    def _execute_changes(self, change_plan: ChangePlan) -> ExecutionResult
```

#### 2. HistoricalDataDeduplicator
```python
class HistoricalDataDeduplicator:
    def __init__(self, config: DeduplicationConfig, spark: SparkSession)
    def deduplicate_historical_data(self, source_df: DataFrame) -> DataFrame
    def _compute_scd_hashes(self, df: DataFrame) -> DataFrame
    def _apply_deduplication_strategy(self, df: DataFrame) -> DataFrame
    def _validate_deduplication_result(self, original_df: DataFrame, deduplicated_df: DataFrame) -> ValidationResult
```

#### 3. DimensionalKeyResolver
```python
class DimensionalKeyResolver:
    def __init__(self, config: KeyResolutionConfig, spark: SparkSession)
    def resolve_keys(self, fact_df: DataFrame, business_date_column: str) -> DataFrame
    def _get_current_dimension_records(self) -> DataFrame
    def _perform_key_resolution(self, fact_df: DataFrame, dimension_df: DataFrame, business_date_column: str) -> DataFrame
    def _resolve_historical_keys(self, fact_df: DataFrame, business_date_column: str) -> DataFrame
```

#### 4. Configuration Classes
```python
@dataclass
class SCDConfig:
    target_table: str
    business_key_columns: List[str]
    scd_columns: List[str]
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

@dataclass
class DeduplicationConfig:
    business_key_columns: List[str]
    scd_columns: List[str]
    effective_from_column: str
    deduplication_strategy: str = "latest"  # latest, earliest, significant, custom
    
    # Standard column names
    scd_hash_column: str = "scd_hash"
    significance_column: Optional[str] = None  # Column to mark significant records
    
    # Custom deduplication logic
    custom_deduplication_logic: Optional[callable] = None
    
    # Performance settings
    batch_size: int = 100000
    enable_optimization: bool = True

@dataclass
class KeyResolutionConfig:
    dimension_table: str
    business_key_columns: List[str]
    surrogate_key_column: str = "surrogate_key"
    effective_start_column: str = "effective_start_ts_utc"
    effective_end_column: str = "effective_end_ts_utc"
    is_current_column: str = "is_current"
    
    # Performance settings
    enable_caching: bool = True
    cache_ttl_minutes: int = 60
```

---

## Data Quality and Validation

### Data Quality Rules (Proposed)

#### 1. Input Validation
- **Required Columns Check**: Verify all required columns exist
- **Null Business Keys**: Flag records with null business keys
- **Duplicate Detection**: Identify duplicate records based on business keys
- **Data Type Validation**: Ensure correct data types for all columns

#### 2. Business Logic Validation
- **Date Consistency**: Validate that effective dates are logical
- **Hash Integrity**: Verify hash computation accuracy
- **Referential Integrity**: Check business key consistency

#### 3. Output Validation
- **Record Count Validation**: Ensure no data loss during processing
- **Hash Uniqueness**: Verify hash uniqueness for current records
- **Date Range Validation**: Ensure proper date ranges for historical records

### Error Handling Strategy

#### Error Flagging Approach
```python
# Add error flags to source DataFrame
source_df = source_df.withColumn("_error_flag", lit("N"))
source_df = source_df.withColumn("_error_message", lit(""))

# Example error handling
def validate_and_flag_errors(df: DataFrame) -> DataFrame:
    # Check for null business keys
    null_bk_df = df.filter(col("business_key").isNull())
    if null_bk_df.count() > 0:
        df = df.withColumn("_error_flag", 
                          when(col("business_key").isNull(), "Y")
                          .otherwise(col("_error_flag")))
        df = df.withColumn("_error_message",
                          when(col("business_key").isNull(), "Null business key")
                          .otherwise(col("_error_message")))
    return df
```

#### Error Processing
- **Flag Records**: Mark problematic records with `_error_flag = 'Y'`
- **Error Messages**: Provide descriptive error messages in `_error_message`
- **Continue Processing**: Process valid records, skip flagged records
- **Error Reporting**: Return error summary in processing result

### Retry Logic
- **No Automatic Retries**: As specified, no automatic retry mechanisms
- **Manual Retry**: Users can manually retry after fixing data issues
- **Idempotent Operations**: Ensure operations can be safely retried

---

## Performance and Scalability

### Expected Data Volumes
- **Dimensional Tables**: Generally smaller (except 1-2 like customer)
- **Typical Sizes**: Most dimensions < 1M records, large dimensions < 10M records
- **Processing Target**: Handle dimensions up to 10M records efficiently

### Processing Frequency
- **Primary**: Daily processing
- **Secondary**: Hourly processing capability
- **Design**: Solution should work for both frequencies

### Performance SLAs
- **Target Processing Time**: 30 minutes for typical dimensional tables
- **Scalability**: Handle up to 10M records within SLA
- **Optimization**: Implement caching, partitioning, and batch processing

### Performance Optimizations

#### 1. Partitioning Strategy
```python
# Time-based partitioning for historical queries
partition_columns = ["effective_start_year", "effective_start_month"]

# Hash-based partitioning for current state queries
partition_columns = ["business_key_hash_bucket"]

# Composite partitioning
partition_columns = ["effective_start_year", "business_key_hash_bucket"]
```

#### 2. Caching Mechanisms
```python
# Multi-level caching
class CacheManager:
    def cache_dimension_table(self, table_name: str, business_date: str) -> DataFrame
    def create_broadcast_lookup(self, dimension_df: DataFrame) -> Broadcast
    def cache_key_mapping(self, business_keys: List[str], surrogate_keys: List[int])
```

#### 3. Delta Lake Optimizations
```python
# Z-ordering for query performance
spark.sql(f"""
    OPTIMIZE {table_name}
    ZORDER BY (business_key, effective_start_ts_utc, business_date)
""")

# Auto-optimization settings
spark.sql(f"""
    ALTER TABLE {table_name} 
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true',
        'delta.targetFileSize' = '128MB'
    )
""")
```

---

## Business Logic Requirements

### Date Handling
- **UTC Standardization**: All SCD system columns in UTC
- **Business Column Conversion**: Convert business columns from other timezones to UTC
- **Version Decision**: Use UTC for versioning and key resolution decisions
- **Timezone Support**: Handle multiple source timezones

```python
def convert_to_utc(df: DataFrame, date_columns: List[str], source_timezone: str) -> DataFrame:
    """Convert date columns from source timezone to UTC"""
    for col_name in date_columns:
        df = df.withColumn(
            f"{col_name}_utc",
            from_utc_timestamp(to_utc_timestamp(col(col_name), source_timezone), "UTC")
        )
    return df
```

### Soft Deletes
- **No Hard Deletes**: Dimensions don't expect deletes
- **Inactivation**: At most, inactivate records with `is_current = 'N'`
- **End Date**: Set `effective_end_ts_utc` when inactivating
- **Historical Preservation**: Maintain all historical versions

### Data Lineage (Version 2)
- **Audit Trail**: Track data lineage and processing history
- **Source Tracking**: Record source system and processing timestamp
- **Change Tracking**: Track what changed between versions
- **Metadata Management**: Store processing metadata

---

## Integration Requirements

### Configuration Management
- **Code-Based Configuration**: All configuration resolved by team code/invocation
- **Environment Agnostic**: No environment-specific configuration in library
- **Parameter Validation**: Validate all configuration parameters
- **Default Values**: Provide sensible defaults for optional parameters

### Monitoring and Metrics
```python
@dataclass
class ProcessingMetrics:
    records_processed: int
    new_records_created: int
    existing_records_updated: int
    records_with_errors: int
    processing_time_seconds: float
    memory_usage_mb: float
    partition_count: int
```

### Notification System
- **No Built-in Notifications**: Library doesn't send notifications
- **Event Hooks**: Provide hooks for external notification systems
- **Status Reporting**: Return processing status and metrics
- **Error Reporting**: Include error details in processing results

---

## Advanced Features

### Historical Data Deduplication Implementation

#### Deduplication Logic Details
```python
class HistoricalDataDeduplicator:
    def deduplicate_historical_data(self, source_df: DataFrame) -> DataFrame:
        """
        Deduplicate historical data using SCD hash-based approach
        
        Process:
        1. Compute SCD hash for all records
        2. Group by business key and SCD hash
        3. Apply deduplication strategy
        4. Return deduplicated records
        """
        # Step 1: Compute SCD hashes
        df_with_hash = self._compute_scd_hashes(source_df)
        
        # Step 2: Apply deduplication strategy
        deduplicated_df = self._apply_deduplication_strategy(df_with_hash)
        
        # Step 3: Validate results
        self._validate_deduplication_result(source_df, deduplicated_df)
        
        return deduplicated_df
    
    def _apply_deduplication_strategy(self, df: DataFrame) -> DataFrame:
        """Apply the configured deduplication strategy"""
        if self.config.deduplication_strategy == "latest":
            return self._keep_latest_per_scd_hash(df)
        elif self.config.deduplication_strategy == "earliest":
            return self._keep_earliest_per_scd_hash(df)
        elif self.config.deduplication_strategy == "significant":
            return self._keep_significant_records(df)
        elif self.config.deduplication_strategy == "custom":
            return self._apply_custom_logic(df)
        else:
            raise ValueError(f"Unknown deduplication strategy: {self.config.deduplication_strategy}")
    
    def _keep_latest_per_scd_hash(self, df: DataFrame) -> DataFrame:
        """Keep the latest record per business key and SCD hash"""
        window_spec = Window.partitionBy(
            *self.config.business_key_columns, 
            self.config.scd_hash_column
        ).orderBy(col(self.config.effective_from_column).desc())
        
        return (df
                .withColumn("row_num", row_number().over(window_spec))
                .filter(col("row_num") == 1)
                .drop("row_num"))
    
    def _keep_earliest_per_scd_hash(self, df: DataFrame) -> DataFrame:
        """Keep the earliest record per business key and SCD hash"""
        window_spec = Window.partitionBy(
            *self.config.business_key_columns, 
            self.config.scd_hash_column
        ).orderBy(col(self.config.effective_from_column).asc())
        
        return (df
                .withColumn("row_num", row_number().over(window_spec))
                .filter(col("row_num") == 1)
                .drop("row_num"))
    
    def _keep_significant_records(self, df: DataFrame) -> DataFrame:
        """Keep records marked as significant"""
        if not self.config.significance_column:
            raise ValueError("Significance column must be specified for 'significant' strategy")
        
        return df.filter(col(self.config.significance_column) == "Y")
```

#### Deduplication Scenarios
1. **Multiple Insignificant Records**: Remove duplicate records with same SCD hash
2. **Mixed Significant/Insignificant**: Keep only significant records when available
3. **Temporal Duplicates**: Handle records with same content but different timestamps
4. **Business Rule Duplicates**: Apply custom business logic for deduplication

### Incremental Processing
- **Delta Support**: Support incremental processing based on change data capture
- **Idempotent Operations**: Ensure operations can be safely repeated
- **Change Detection**: Efficiently detect and process only changed records
- **Watermarking**: Track processing progress for incremental runs

```python
def process_incremental(self, source_df: DataFrame, last_processed_timestamp: str) -> ProcessingResult:
    """Process only new/changed records since last run"""
    incremental_df = source_df.filter(
        col("last_modified_timestamp") > last_processed_timestamp
    )
    return self.process_scd(incremental_df)
```

### Schema Evolution
- **Dynamic Schema**: Handle schema changes over time
- **Column Addition**: Support adding new columns to existing tables
- **Column Removal**: Handle removal of columns gracefully
- **Type Changes**: Support data type changes with validation

```python
def handle_schema_evolution(self, source_df: DataFrame, target_schema: StructType) -> DataFrame:
    """Handle schema evolution between source and target"""
    # Add missing columns with default values
    # Remove extra columns
    # Validate type compatibility
    return evolved_df
```

### Data Versioning (Future)
- **Not Required**: Data versioning not needed at this moment
- **Design Consideration**: Architecture should support future versioning needs
- **Rollback Capability**: Foundation for future rollback capabilities

---

## Implementation Guidelines

### Code Quality Standards
- **Clean Code**: Follow clean code practices and SOLID principles
- **Type Hints**: Use comprehensive type hints
- **Documentation**: Include docstrings for all public methods
- **Error Handling**: Comprehensive error handling with custom exceptions
- **Logging**: Structured logging for debugging and monitoring

### Testing Strategy
```python
# Unit Tests
class TestSCDProcessor:
    def test_scd_type2_change_detection(self)
    def test_hash_computation(self)
    def test_date_handling(self)
    def test_error_handling(self)

# Integration Tests
class TestSCDIntegration:
    def test_end_to_end_scd_processing(self)
    def test_key_resolution_integration(self)
    def test_performance_benchmarks(self)

# Data Quality Tests
class TestDataQuality:
    def test_validation_rules(self)
    def test_error_flagging(self)
    def test_data_integrity(self)
```

### Deployment Considerations
- **Databricks Integration**: Optimized for Databricks environment
- **Delta Lake**: Leverage Delta Lake features for ACID compliance
- **Spark Optimization**: Use Spark best practices for performance
- **Resource Management**: Efficient memory and compute usage

---

## Usage Examples

### Historical Data Deduplication
```python
# Configuration for deduplication
dedup_config = DeduplicationConfig(
    business_key_columns=["customer_id"],
    scd_columns=["name", "email", "address", "phone"],
    effective_from_column="created_ts",
    deduplication_strategy="latest"  # or "earliest", "significant", "custom"
)

# Deduplicate historical data
deduplicator = HistoricalDataDeduplicator(dedup_config, spark)
deduplicated_df = deduplicator.deduplicate_historical_data(historical_df)

print(f"Original records: {historical_df.count()}")
print(f"Deduplicated records: {deduplicated_df.count()}")
print(f"Duplicates removed: {historical_df.count() - deduplicated_df.count()}")
```

### SCD Type 2 Processing (After Deduplication)
```python
# Configuration
scd_config = SCDConfig(
    target_table="gold.customer_dim",
    business_key_columns=["customer_id"],
    scd_columns=["name", "email", "address", "phone"],
    effective_from_column="last_modified_ts",
    initial_effective_from_column="created_ts"
)

# Process SCD with deduplicated data
processor = SCDProcessor(scd_config, spark)
result = processor.process_scd(deduplicated_df)

print(f"Processed {result.records_processed} records")
print(f"Created {result.new_records_created} new records")
print(f"Updated {result.existing_records_updated} existing records")
print(f"Errors: {result.records_with_errors}")
```

### Complete Historical Dimension Build Workflow
```python
# Step 1: Deduplicate historical data
dedup_config = DeduplicationConfig(
    business_key_columns=["customer_id"],
    scd_columns=["name", "email", "address", "phone"],
    effective_from_column="created_ts",
    deduplication_strategy="latest"
)

deduplicator = HistoricalDataDeduplicator(dedup_config, spark)
deduplicated_df = deduplicator.deduplicate_historical_data(historical_df)

# Step 2: Process SCD with deduplicated data
scd_config = SCDConfig(
    target_table="gold.customer_dim",
    business_key_columns=["customer_id"],
    scd_columns=["name", "email", "address", "phone"],
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

### Key Resolution
```python
# Configuration
key_config = KeyResolutionConfig(
    dimension_table="gold.customer_dim",
    business_key_columns=["customer_id"],
    surrogate_key_column="customer_sk"
)

# Resolve keys
resolver = DimensionalKeyResolver(key_config, spark)
resolved_df = resolver.resolve_keys(fact_df, "transaction_date")
```

---

## Success Criteria

### Functional Requirements
- ✅ Implement SCD Type 2 with hash-based change detection
- ✅ Support historical data deduplication before SCD processing
- ✅ Support dimensional key resolution for fact tables
- ✅ Handle timezone conversion to UTC
- ✅ Provide comprehensive data validation
- ✅ Support incremental processing
- ✅ Ensure idempotent operations

### Non-Functional Requirements
- ✅ Process dimensions within 30-minute SLA
- ✅ Handle up to 10M records efficiently
- ✅ Support daily and hourly processing frequencies
- ✅ Provide comprehensive error handling
- ✅ Follow clean code practices
- ✅ Include comprehensive testing

### Quality Metrics
- **Code Coverage**: >90% test coverage
- **Performance**: <30 minutes for typical dimensions
- **Reliability**: 99.9% success rate
- **Maintainability**: Clear documentation and modular design

---

## Next Steps

1. **Review and Approve**: Review this specification document
2. **Implementation Planning**: Create detailed implementation plan
3. **Development**: Implement core SCD functionality
4. **Testing**: Develop comprehensive test suite
5. **Documentation**: Create user documentation and examples
6. **Deployment**: Deploy to development environment
7. **Validation**: Validate with real data scenarios
8. **Production**: Deploy to production environment

---

*This document serves as the complete specification for building the SCD Type 2 library. All requirements, constraints, and design decisions are captured for future implementation.*
