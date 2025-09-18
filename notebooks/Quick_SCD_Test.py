# Databricks notebook source
# MAGIC %md
# MAGIC # Quick SCD Library Test
# MAGIC 
# MAGIC A simple notebook to quickly test the SCD Type 2 library functionality.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# Import the library
import sys
sys.path.append('/Workspace/Repos/dev/data-accel/src/') # Update this path

from libraries.dimensional_processing.scd_type2.scd_processor import SCDProcessor
from libraries.dimensional_processing.common.config import SCDConfig
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col

print("✅ Library imported successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Test Data

# COMMAND ----------

# Create test database
spark.sql("CREATE DATABASE IF NOT EXISTS quick_test")
spark.sql("USE quick_test")

# Create sample data
data = [
    ("1", "John Doe", "john@example.com", "123 Main St", "2024-01-01 10:00:00", "2024-01-01 09:00:00"),
    ("2", "Jane Smith", "jane@example.com", "789 Pine St", "2024-01-01 10:00:00", "2024-01-01 09:00:00"),
    ("3", "Bob Johnson", "bob@example.com", "321 Elm St", "2024-01-01 10:00:00", "2024-01-01 09:00:00"),
]

schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("address", StringType(), True),
    StructField("last_modified_ts", StringType(), True),
    StructField("created_ts", StringType(), True)
])

df = spark.createDataFrame(data, schema)
print(f"✅ Created test data with {df.count()} records")
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Target Table

# COMMAND ----------

# Create the target dimension table with proper SCD schema
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

def create_target_table():
    """Create the target dimension table with SCD Type 2 schema."""
    
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
        .saveAsTable("quick_test.customer_dim")
    
    print("✅ Target dimension table created successfully")

# Create the target table
create_target_table()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure and Run SCD

# COMMAND ----------

# Configure SCD
config = SCDConfig(
    target_table="quick_test.customer_dim",
    business_key_columns=["customer_id"],
    scd_columns=["name", "email", "address"],
    effective_from_column="last_modified_ts",
    initial_effective_from_column="created_ts"
)

print("✅ SCD Configuration created")

# COMMAND ----------

# Process SCD
processor = SCDProcessor(config, spark)
result = processor.process_scd(df)

print(f"✅ SCD Processing completed!")
print(f"   Records processed: {result.records_processed}")
print(f"   New records created: {result.new_records_created}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Results

# COMMAND ----------

# Show the results
result_df = spark.sql("SELECT * FROM quick_test.customer_dim ORDER BY customer_id")
result_df.show()

print(f"✅ Total records in dimension: {result_df.count()}")
print(f"✅ Current records: {result_df.filter(col('is_current') == 'Y').count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Incremental Update

# COMMAND ----------

# Create new data for incremental update
new_data = [
    ("1", "John Doe Updated", "john.updated@example.com", "123 Main St Updated", "2024-01-02 10:00:00", "2024-01-01 09:00:00"),
    ("4", "Alice Brown", "alice@example.com", "999 Cedar St", "2024-01-02 10:00:00", "2024-01-02 09:00:00"),
]

new_df = spark.createDataFrame(new_data, schema)
print("✅ Created incremental data")
new_df.show()

# COMMAND ----------

# Process incremental update
incremental_result = processor.process_scd(new_df)

print(f"✅ Incremental Processing completed!")
print(f"   Records processed: {incremental_result.records_processed}")
print(f"   New records created: {incremental_result.new_records_created}")

# COMMAND ----------

# Show final results
final_df = spark.sql("SELECT * FROM quick_test.customer_dim ORDER BY customer_id, effective_start_ts_utc")
final_df.show()

print(f"✅ Final total records: {final_df.count()}")
print(f"✅ Final current records: {final_df.filter(col('is_current') == 'Y').count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)

# COMMAND ----------

# Uncomment to clean up
# spark.sql("DROP TABLE IF EXISTS quick_test.customer_dim")
# spark.sql("DROP DATABASE IF EXISTS quick_test")
# print("✅ Cleanup completed")
