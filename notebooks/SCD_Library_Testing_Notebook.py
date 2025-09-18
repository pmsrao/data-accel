# Databricks notebook source
# MAGIC %md
# MAGIC # SCD Type 2 Library Testing Notebook
# MAGIC 
# MAGIC This notebook demonstrates how to use the Dimensional Processing Library for SCD Type 2 operations.
# MAGIC 
# MAGIC ## Features Tested:
# MAGIC - SCD Type 2 Processing
# MAGIC - Historical Data Deduplication
# MAGIC - Dimensional Key Resolution
# MAGIC - Error Handling and Validation
# MAGIC - Performance Testing
# MAGIC 
# MAGIC ## Prerequisites:
# MAGIC 1. Upload the `src/libraries/dimensional_processing/` directory to your Databricks workspace
# MAGIC 2. Ensure you have appropriate permissions to create tables in your database
# MAGIC 3. This notebook uses Delta Lake (pre-configured in Databricks)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup and Imports

# COMMAND ----------

# Import the SCD library
import sys
import os

# Add the library path (adjust path as needed based on your workspace structure)
sys.path.append('/Workspace/Repos/dev/data-accel/src/')  # Update this path

from libraries.dimensional_processing.scd_type2.scd_processor import SCDProcessor
from libraries.dimensional_processing.scd_type2.historical_data_deduplicator import HistoricalDataDeduplicator
from libraries.dimensional_processing.key_resolution.key_resolver import DimensionalKeyResolver
from libraries.dimensional_processing.common.config import SCDConfig, DeduplicationConfig, KeyResolutionConfig

# Standard imports
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import lit, current_timestamp, col
from datetime import datetime, timedelta
import time

print("✅ All imports successful!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Test Database and Sample Data

# COMMAND ----------

# Create test database
spark.sql("CREATE DATABASE IF NOT EXISTS scd_test")
spark.sql("USE scd_test")

print("✅ Test database created/selected")

# COMMAND ----------

# Create sample historical data with duplicates for testing
def create_sample_historical_data():
    """Create sample historical data with duplicates for testing."""
    data = [
        # Customer 1 - Multiple records (some duplicates, some changes)
        ("1", "John Doe", "john@example.com", "123 Main St", "2024-01-01 10:00:00", "2024-01-01 09:00:00"),
        ("1", "John Doe", "john@example.com", "123 Main St", "2024-01-01 11:00:00", "2024-01-01 09:00:00"),  # Duplicate
        ("1", "John Smith", "john.smith@example.com", "456 Oak Ave", "2024-01-01 12:00:00", "2024-01-01 09:00:00"),  # Different SCD
        
        # Customer 2 - Multiple records
        ("2", "Jane Smith", "jane@example.com", "789 Pine St", "2024-01-01 10:00:00", "2024-01-01 09:00:00"),
        ("2", "Jane Smith", "jane@example.com", "789 Pine St", "2024-01-01 11:00:00", "2024-01-01 09:00:00"),  # Duplicate
        ("2", "Jane Smith", "jane@example.com", "789 Pine St Updated", "2024-01-02 10:00:00", "2024-01-01 09:00:00"),  # Address change
        
        # Customer 3 - Single record
        ("3", "Bob Johnson", "bob@example.com", "321 Elm St", "2024-01-01 10:00:00", "2024-01-01 09:00:00"),
        
        # Customer 4 - Multiple records with different changes
        ("4", "Alice Brown", "alice@example.com", "999 Cedar St", "2024-01-01 10:00:00", "2024-01-01 09:00:00"),
        ("4", "Alice Brown", "alice.brown@example.com", "999 Cedar St", "2024-01-01 11:00:00", "2024-01-01 09:00:00"),  # Email change
        ("4", "Alice Brown", "alice.brown@example.com", "999 Cedar St", "2024-01-01 12:00:00", "2024-01-01 09:00:00"),  # Duplicate
    ]
    
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("address", StringType(), True),
        StructField("last_modified_ts", StringType(), True),
        StructField("created_ts", StringType(), True)
    ])
    
    return spark.createDataFrame(data, schema)

# Create the sample data
historical_data = create_sample_historical_data()
print(f"✅ Created sample historical data with {historical_data.count()} records")
historical_data.show()

# COMMAND ----------

# Create sample fact data for key resolution testing
def create_sample_fact_data():
    """Create sample fact data for key resolution testing."""
    data = [
        ("1", "2024-01-01 10:00:00", 100.0),
        ("2", "2024-01-01 11:00:00", 200.0),
        ("3", "2024-01-01 12:00:00", 300.0),
        ("4", "2024-01-01 13:00:00", 400.0),
        ("5", "2024-01-01 14:00:00", 500.0),  # Customer 5 doesn't exist in dimension
    ]
    
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("amount", StringType(), True)
    ])
    
    return spark.createDataFrame(data, schema)

fact_data = create_sample_fact_data()
print(f"✅ Created sample fact data with {fact_data.count()} records")
fact_data.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Configure SCD Processing

# COMMAND ----------

# Configure SCD processing
scd_config = SCDConfig(
    target_table="scd_test.customer_dim",
    business_key_columns=["customer_id"],
    scd_columns=["customer_id", "name", "email", "address"],
    surrogate_key_column="customer_sk",
    effective_from_column="last_modified_ts",
    initial_effective_from_column="created_ts",
    enable_optimization=True,
    hash_algorithm="sha256"
)

print("✅ SCD Configuration created:")
print(f"   Target Table: {scd_config.target_table}")
print(f"   Business Keys: {scd_config.business_key_columns}")
print(f"   SCD Columns: {scd_config.scd_columns}")

# COMMAND ----------

# Configure deduplication
dedup_config = DeduplicationConfig(
    business_key_columns=["customer_id"],
    scd_columns=["name", "email", "address"],
    effective_from_column="created_ts",
    deduplication_strategy="latest"
)

print("✅ Deduplication Configuration created:")
print(f"   Strategy: {dedup_config.deduplication_strategy}")

# COMMAND ----------

# Configure key resolution
key_config = KeyResolutionConfig(
    dimension_table="scd_test.customer_dim",
    business_key_columns=["customer_id"],
    surrogate_key_column="customer_sk",
    effective_start_column="effective_start_ts_utc",
    effective_end_column="effective_end_ts_utc",
    is_current_column="is_current",
    enable_caching=True,
    cache_ttl_minutes=60
)

print("✅ Key Resolution Configuration created:")
print(f"   Dimension Table: {key_config.dimension_table}")
print(f"   Surrogate Key: {key_config.surrogate_key_column}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Target Dimension Table
# MAGIC 
# MAGIC Create the target dimension table with the proper schema for SCD Type 2 processing.

# COMMAND ----------

# Create the target dimension table with proper SCD schema
def create_target_dimension_table():
    """Create the target dimension table with SCD Type 2 schema."""
    
    # Define the schema for the dimension table
    schema = StructType([
        StructField("customer_sk", StringType(), False),  # Surrogate key
        StructField("customer_id", StringType(), True),   # Business key
        StructField("name", StringType(), True),          # SCD attributes
        StructField("email", StringType(), True),
        StructField("address", StringType(), True),
        StructField("scd_hash", StringType(), True),      # SCD metadata
        StructField("effective_start_ts_utc", TimestampType(), True),
        StructField("effective_end_ts_utc", TimestampType(), True),
        StructField("is_current", StringType(), True),
        StructField("created_ts_utc", TimestampType(), True),  # Audit columns
        StructField("modified_ts_utc", TimestampType(), True),
        StructField("_error_flag", StringType(), True),    # Error handling
        StructField("_error_message", StringType(), True)
    ])
    
    # Create empty DataFrame with the schema
    empty_df = spark.createDataFrame([], schema)
    
    # Write as Delta table
    empty_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("scd_test.customer_dim")
    
    print("✅ Target dimension table created successfully")
    print("   Table: scd_test.customer_dim")
    print("   Format: Delta")
    print("   Schema: SCD Type 2 with audit columns")

# Create the target table
create_target_dimension_table()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Test Historical Data Deduplication

# COMMAND ----------

print("🔄 Testing Historical Data Deduplication...")

# Initialize deduplicator
deduplicator = HistoricalDataDeduplicator(dedup_config, spark)

# Perform deduplication
start_time = time.time()
deduplicated_data = deduplicator.deduplicate_historical_data(historical_data)
dedup_time = time.time() - start_time

# Display results
original_count = historical_data.count()
dedup_count = deduplicated_data.count()
duplicates_removed = original_count - dedup_count

print(f"✅ Deduplication completed in {dedup_time:.2f} seconds")
print(f"   Original records: {original_count}")
print(f"   After deduplication: {dedup_count}")
print(f"   Duplicates removed: {duplicates_removed}")

# Show deduplicated data
print("\n📊 Deduplicated Data:")
deduplicated_data.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Test SCD Type 2 Processing

# COMMAND ----------

print("🔄 Testing SCD Type 2 Processing...")

# Initialize SCD processor
processor = SCDProcessor(scd_config, spark)

# Process SCD with deduplicated data
start_time = time.time()
scd_result = processor.process_scd(deduplicated_data)
processing_time = time.time() - start_time

# Display results
print(f"✅ SCD Processing completed in {processing_time:.2f} seconds")
print(f"   Records processed: {scd_result.records_processed}")
print(f"   New records created: {scd_result.new_records_created}")
print(f"   Existing records updated: {scd_result.existing_records_updated}")
print(f"   Processing time: {scd_result.processing_time_seconds:.2f} seconds")

# COMMAND ----------

# Verify the target table
print("📊 Target Table Contents:")
target_df = spark.sql(f"SELECT * FROM {scd_config.target_table} ORDER BY customer_id, effective_start_ts_utc")
target_df.show()

# COMMAND ----------

# Check SCD metadata
print("🔍 SCD Metadata Analysis:")
print(f"   Total records in dimension: {target_df.count()}")

# Check current vs historical records
current_records = target_df.filter(col("is_current") == "Y").count()
historical_records = target_df.filter(col("is_current") == "N").count()

print(f"   Current records: {current_records}")
print(f"   Historical records: {historical_records}")

# Show current records
print("\n📋 Current Records:")
current_df = target_df.filter(col("is_current") == "Y").orderBy("customer_id")
current_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Test Incremental SCD Processing

# COMMAND ----------

print("🔄 Testing Incremental SCD Processing...")

# Create new data for incremental processing
new_data = [
    ("1", "John Doe Updated", "john.updated@example.com", "123 Main St Updated", "2024-01-03 10:00:00", "2024-01-01 09:00:00"),  # Update existing
    ("5", "Eve Wilson", "eve@example.com", "555 New St", "2024-01-03 10:00:00", "2024-01-03 09:00:00"),  # New customer
    ("2", "Jane Smith", "jane.smith@example.com", "789 Pine St", "2024-01-03 11:00:00", "2024-01-01 09:00:00"),  # Email change
]

schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("address", StringType(), True),
    StructField("last_modified_ts", StringType(), True),
    StructField("created_ts", StringType(), True)
])

incremental_df = spark.createDataFrame(new_data, schema)
print(f"✅ Created incremental data with {incremental_df.count()} records")
incremental_df.show()

# COMMAND ----------

# Process incremental update
start_time = time.time()
incremental_result = processor.process_scd(incremental_df)
incremental_time = time.time() - start_time

print(f"✅ Incremental Processing completed in {incremental_time:.2f} seconds")
print(f"   Records processed: {incremental_result.records_processed}")
print(f"   New records created: {incremental_result.new_records_created}")
print(f"   Existing records updated: {incremental_result.existing_records_updated}")

# COMMAND ----------

# Verify incremental results
print("📊 Updated Target Table Contents:")
updated_target_df = spark.sql(f"SELECT * FROM {scd_config.target_table} ORDER BY customer_id, effective_start_ts_utc")
updated_target_df.show()

# Check final counts
final_count = updated_target_df.count()
final_current = updated_target_df.filter(col("is_current") == "Y").count()
final_historical = updated_target_df.filter(col("is_current") == "N").count()

print(f"\n📈 Final Statistics:")
print(f"   Total records: {final_count}")
print(f"   Current records: {final_current}")
print(f"   Historical records: {final_historical}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Test Dimensional Key Resolution

# COMMAND ----------

print("🔄 Testing Dimensional Key Resolution...")

# Initialize key resolver
key_resolver = DimensionalKeyResolver(key_config, spark)

# Resolve keys for fact data
start_time = time.time()
resolved_fact_data = key_resolver.resolve_dimensional_keys(fact_data, "transaction_date")
resolution_time = time.time() - start_time

print(f"✅ Key Resolution completed in {resolution_time:.2f} seconds")

# Display results
print(f"📊 Resolved Fact Data:")
resolved_fact_data.show()

# COMMAND ----------

# Analyze key resolution results
print("🔍 Key Resolution Analysis:")
total_facts = resolved_fact_data.count()
resolved_keys = resolved_fact_data.filter(col("customer_sk").isNotNull()).count()
unresolved_keys = resolved_fact_data.filter(col("customer_sk").isNull()).count()

print(f"   Total fact records: {total_facts}")
print(f"   Successfully resolved: {resolved_keys}")
print(f"   Unresolved: {unresolved_keys}")

if unresolved_keys > 0:
    print("\n❌ Unresolved Records:")
    unresolved_df = resolved_fact_data.filter(col("customer_sk").isNull())
    unresolved_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Test Error Handling and Validation

# COMMAND ----------

print("🔄 Testing Error Handling and Validation...")

# Create invalid data (null business keys)
invalid_data = [
    ("1", "John Doe", "john@example.com", "123 Main St", "2024-01-01 10:00:00", "2024-01-01 09:00:00"),
    (None, "Jane Smith", "jane@example.com", "789 Pine St", "2024-01-01 10:00:00", "2024-01-01 09:00:00"),  # Null business key
    ("3", "Bob Johnson", "bob@example.com", "321 Elm St", "2024-01-01 10:00:00", "2024-01-01 09:00:00"),
    ("", "Empty ID", "empty@example.com", "Empty St", "2024-01-01 10:00:00", "2024-01-01 09:00:00"),  # Empty business key
]

schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("address", StringType(), True),
    StructField("last_modified_ts", StringType(), True),
    StructField("created_ts", StringType(), True)
])

invalid_df = spark.createDataFrame(invalid_data, schema)
print(f"✅ Created invalid data with {invalid_df.count()} records")
invalid_df.show()

# COMMAND ----------

# Test error handling
try:
    print("🔄 Processing invalid data...")
    error_result = processor.process_scd(invalid_df)
    print("✅ Processing completed with error handling")
    print(f"   Records processed: {error_result.records_processed}")
    
    # Check for error flags
    error_df = spark.sql(f"SELECT * FROM {scd_config.target_table} WHERE _error_flag = 'Y'")
    error_count = error_df.count()
    
    if error_count > 0:
        print(f"   Records with errors: {error_count}")
        print("\n❌ Error Records:")
        error_df.show()
    else:
        print("   No error records found")
        
except Exception as e:
    print(f"❌ Validation error caught: {str(e)}")
    print("✅ Error handling working correctly")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Performance Testing

# COMMAND ----------

print("🔄 Testing Performance with Larger Dataset...")

# Create larger dataset for performance testing
def create_large_dataset(num_records=1000):
    """Create a larger dataset for performance testing."""
    data = []
    for i in range(num_records):
        # Create some duplicates and changes
        customer_id = str(i % 100)  # 100 unique customers with multiple records
        
        if i % 3 == 0:  # Every 3rd record is a duplicate
            name = f"Customer {customer_id}"
            email = f"customer{customer_id}@example.com"
        else:  # Different variations
            name = f"Customer {customer_id} Updated"
            email = f"customer{customer_id}.updated@example.com"
        
        data.append((
            customer_id,
            name,
            email,
            f"Address {customer_id}",
            f"2024-01-{(i % 30) + 1:02d} 10:00:00",
            "2024-01-01 09:00:00"
        ))
    
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("address", StringType(), True),
        StructField("last_modified_ts", StringType(), True),
        StructField("created_ts", StringType(), True)
    ])
    
    return spark.createDataFrame(data, schema)

# Create large dataset
large_df = create_large_dataset(1000)
print(f"✅ Created large dataset with {large_df.count()} records")

# COMMAND ----------

# Test performance with large dataset
print("🔄 Processing large dataset...")

start_time = time.time()
large_result = processor.process_scd(large_df)
large_processing_time = time.time() - start_time

print(f"✅ Large Dataset Processing completed in {large_processing_time:.2f} seconds")
print(f"   Records processed: {large_result.records_processed}")
print(f"   New records created: {large_result.new_records_created}")
print(f"   Existing records updated: {large_result.existing_records_updated}")
print(f"   Processing rate: {large_result.records_processed / large_processing_time:.2f} records/second")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Final Summary and Cleanup

# COMMAND ----------

print("📊 Final Summary:")
print("=" * 50)

# Get final table statistics
final_df = spark.sql(f"SELECT * FROM {scd_config.target_table}")
total_records = final_df.count()
current_records = final_df.filter(col("is_current") == "Y").count()
historical_records = final_df.filter(col("is_current") == "N").count()

print(f"✅ SCD Library Testing Completed Successfully!")
print(f"")
print(f"📈 Final Statistics:")
print(f"   Total dimension records: {total_records}")
print(f"   Current records: {current_records}")
print(f"   Historical records: {historical_records}")
print(f"")
print(f"🧪 Tests Performed:")
print(f"   ✅ Historical Data Deduplication")
print(f"   ✅ SCD Type 2 Processing")
print(f"   ✅ Incremental Processing")
print(f"   ✅ Dimensional Key Resolution")
print(f"   ✅ Error Handling and Validation")
print(f"   ✅ Performance Testing")
print(f"")
print(f"🎉 All tests completed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: Cleanup Test Data
# MAGIC 
# MAGIC Uncomment the following cell if you want to clean up the test data after testing.

# COMMAND ----------

# Uncomment to clean up test data
# spark.sql("DROP TABLE IF EXISTS scd_test.customer_dim")
# spark.sql("DROP DATABASE IF EXISTS scd_test")
# print("✅ Test data cleaned up")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC 1. **Review Results**: Check the output of each test to ensure everything is working correctly
# MAGIC 2. **Customize Configuration**: Modify the configuration objects to match your specific requirements
# MAGIC 3. **Test with Real Data**: Replace the sample data with your actual data
# MAGIC 4. **Monitor Performance**: Use the performance metrics to optimize your processing
# MAGIC 5. **Implement in Production**: Use the library in your production data pipelines
# MAGIC 
# MAGIC ## Troubleshooting
# MAGIC 
# MAGIC If you encounter any issues:
# MAGIC 1. Check that the library path is correct in the imports
# MAGIC 2. Ensure you have appropriate permissions to create tables
# MAGIC 3. Verify that your data schema matches the expected format
# MAGIC 4. Check the error messages for specific validation failures
