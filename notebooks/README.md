# Databricks Testing Notebooks

This directory contains Databricks notebooks for testing the SCD Type 2 Dimensional Processing Library.

## Notebooks

### 1. `SCD_Library_Testing_Notebook.py` - Comprehensive Testing
**Purpose**: Complete end-to-end testing of all library features

**Features Tested**:
- ✅ Historical Data Deduplication
- ✅ SCD Type 2 Processing
- ✅ Incremental Processing
- ✅ Dimensional Key Resolution
- ✅ Error Handling and Validation
- ✅ Performance Testing with Large Datasets

**Duration**: ~10-15 minutes
**Data Volume**: 1000+ records for performance testing

### 2. `Quick_SCD_Test.py` - Quick Validation
**Purpose**: Fast validation of basic SCD functionality

**Features Tested**:
- ✅ Basic SCD Type 2 Processing
- ✅ Incremental Updates
- ✅ Result Verification

**Duration**: ~2-3 minutes
**Data Volume**: 3-5 records for quick testing

## Setup Instructions

### Step 1: Upload Library to Databricks
1. Navigate to your Databricks workspace
2. Go to **Workspace** → **Users** → **your_username**
3. Create a folder called `dimensional_processing`
4. Upload the entire `src/libraries/dimensional_processing/` directory to this folder

### Step 2: Update Import Paths
In both notebooks, update the import path:
```python
sys.path.append('/Workspace/Users/your_username/dimensional_processing')
```
Replace `your_username` with your actual Databricks username.

### Step 3: Run the Notebooks
1. **For Quick Testing**: Start with `Quick_SCD_Test.py`
2. **For Comprehensive Testing**: Run `SCD_Library_Testing_Notebook.py`

## Expected Results

### Quick Test Results
- ✅ 3 initial records processed
- ✅ 2 incremental records processed
- ✅ 5 total records in dimension table
- ✅ 4 current records (1 historical due to update)

### Comprehensive Test Results
- ✅ Deduplication removes duplicate records
- ✅ SCD processing creates proper versioning
- ✅ Key resolution works for fact tables
- ✅ Error handling catches invalid data
- ✅ Performance metrics are reasonable

## Troubleshooting

### Common Issues

1. **Import Error**: 
   - Check that the library path is correct
   - Ensure the `dimensional_processing` folder is uploaded correctly

2. **Permission Error**:
   - Ensure you have CREATE TABLE permissions
   - Check that you can create databases in your workspace

3. **Delta Lake Error**:
   - This should not occur in Databricks (Delta Lake is pre-configured)
   - If it does, check your Databricks runtime version

4. **Schema Mismatch**:
   - Ensure your data has the required columns
   - Check that column names match the configuration

### Performance Expectations

- **Small datasets** (< 100 records): < 5 seconds
- **Medium datasets** (100-1000 records): < 30 seconds
- **Large datasets** (1000+ records): < 2 minutes

## Customization

### Modifying Test Data
Update the data creation functions to match your schema:
```python
# Example: Add more columns
schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),  # New column
    StructField("address", StringType(), True),
    StructField("last_modified_ts", StringType(), True),
    StructField("created_ts", StringType(), True)
])
```

### Modifying Configuration
Update the configuration objects to match your requirements:
```python
config = SCDConfig(
    target_table="your_schema.your_table",
    business_key_columns=["your_business_key"],
    scd_columns=["column1", "column2", "column3"],
    effective_from_column="your_effective_date_column",
    initial_effective_from_column="your_created_date_column"
)
```

## Next Steps

After successful testing:

1. **Integrate with Real Data**: Replace sample data with your actual data
2. **Production Deployment**: Use the library in your production pipelines
3. **Monitoring**: Set up monitoring for your SCD processes
4. **Optimization**: Use performance metrics to optimize processing

## Support

If you encounter issues:
1. Check the error messages in the notebook output
2. Verify your data schema matches the expected format
3. Ensure you have appropriate permissions
4. Review the library documentation in the main README
