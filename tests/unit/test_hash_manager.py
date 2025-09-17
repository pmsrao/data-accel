"""
Unit tests for HashManager.
"""

import pytest
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from libraries.dimensional_processing.scd_type2.hash_manager import HashManager
from libraries.dimensional_processing.common.config import SCDConfig


class TestHashManager:
    """Test cases for HashManager."""
    
    @pytest.fixture
    def spark(self):
        """Create Spark session for testing."""
        return SparkSession.builder.appName("test").master("local[2]").getOrCreate()
    
    @pytest.fixture
    def scd_config(self):
        """Create SCD configuration for testing."""
        return SCDConfig(
            target_table="test.customer_dim",
            business_key_columns=["customer_id"],
            scd_columns=["name", "email"],
            hash_algorithm="sha256"
        )
    
    @pytest.fixture
    def sample_data(self, spark):
        """Create sample data for testing."""
        data = [
            ("1", "John Doe", "john@example.com"),
            ("2", "Jane Smith", "jane@example.com"),
            ("3", "Bob Johnson", "bob@example.com")
        ]
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True)
        ])
        
        return spark.createDataFrame(data, schema)
    
    @pytest.fixture
    def hash_manager(self, scd_config):
        """Create HashManager instance for testing."""
        return HashManager(scd_config)
    
    def test_init(self, scd_config):
        """Test HashManager initialization."""
        hash_manager = HashManager(scd_config)
        
        assert hash_manager.config == scd_config
        assert hash_manager.hash_algorithm == "sha256"
    
    def test_init_invalid_algorithm(self):
        """Test HashManager initialization with invalid algorithm."""
        config = SCDConfig(
            target_table="test.customer_dim",
            business_key_columns=["customer_id"],
            scd_columns=["name", "email"],
            hash_algorithm="invalid"
        )
        
        with pytest.raises(ValueError):
            HashManager(config)
    
    def test_compute_scd_hash_sha256(self, hash_manager, sample_data):
        """Test SCD hash computation with SHA256."""
        result_df = hash_manager.compute_scd_hash(sample_data)
        
        # Check that hash column is added
        assert hash_manager.config.scd_hash_column in result_df.columns
        
        # Check that all original records are preserved
        assert result_df.count() == sample_data.count()
        
        # Check that records with same SCD columns have same hash
        john_record = result_df.filter(result_df.customer_id == "1").collect()[0]
        jane_record = result_df.filter(result_df.customer_id == "2").collect()[0]
        
        # Different records should have different hashes
        assert john_record[hash_manager.config.scd_hash_column] != jane_record[hash_manager.config.scd_hash_column]
    
    def test_compute_scd_hash_md5(self, sample_data):
        """Test SCD hash computation with MD5."""
        config = SCDConfig(
            target_table="test.customer_dim",
            business_key_columns=["customer_id"],
            scd_columns=["name", "email"],
            hash_algorithm="md5"
        )
        
        hash_manager = HashManager(config)
        result_df = hash_manager.compute_scd_hash(sample_data)
        
        # Check that hash column is added
        assert hash_manager.config.scd_hash_column in result_df.columns
        
        # Check that all original records are preserved
        assert result_df.count() == sample_data.count()
    
    def test_compute_business_key_hash(self, hash_manager, sample_data):
        """Test business key hash computation."""
        result_df = hash_manager.compute_business_key_hash(sample_data)
        
        # Check that business key hash column is added
        assert "business_key_hash" in result_df.columns
        
        # Check that all original records are preserved
        assert result_df.count() == sample_data.count()
        
        # Check that records with same business keys have same hash
        john_record = result_df.filter(result_df.customer_id == "1").collect()[0]
        jane_record = result_df.filter(result_df.customer_id == "2").collect()[0]
        
        # Different business keys should have different hashes
        assert john_record["business_key_hash"] != jane_record["business_key_hash"]
    
    def test_compare_hashes_equal(self, hash_manager):
        """Test hash comparison with equal hashes."""
        hash1 = "abc123"
        hash2 = "abc123"
        
        result = hash_manager.compare_hashes(hash1, hash2)
        
        assert result is True
    
    def test_compare_hashes_different(self, hash_manager):
        """Test hash comparison with different hashes."""
        hash1 = "abc123"
        hash2 = "def456"
        
        result = hash_manager.compare_hashes(hash1, hash2)
        
        assert result is False
    
    def test_get_hash_columns(self, hash_manager):
        """Test getting hash columns."""
        hash_columns = hash_manager.get_hash_columns()
        
        assert hash_columns == hash_manager.config.scd_columns
    
    def test_get_business_key_columns(self, hash_manager):
        """Test getting business key columns."""
        business_key_columns = hash_manager.get_business_key_columns()
        
        assert business_key_columns == hash_manager.config.business_key_columns
    
    def test_hash_consistency(self, hash_manager, sample_data):
        """Test that hash computation is consistent."""
        # Compute hash twice
        result_df1 = hash_manager.compute_scd_hash(sample_data)
        result_df2 = hash_manager.compute_scd_hash(sample_data)
        
        # Hashes should be identical
        hashes1 = [row[hash_manager.config.scd_hash_column] for row in result_df1.collect()]
        hashes2 = [row[hash_manager.config.scd_hash_column] for row in result_df2.collect()]
        
        assert hashes1 == hashes2
    
    def test_hash_uniqueness(self, hash_manager, sample_data):
        """Test that different records produce different hashes."""
        result_df = hash_manager.compute_scd_hash(sample_data)
        
        # Get all hashes
        hashes = [row[hash_manager.config.scd_hash_column] for row in result_df.collect()]
        
        # All hashes should be unique (since all records are different)
        assert len(hashes) == len(set(hashes))
    
    def test_hash_with_null_values(self, hash_manager, spark):
        """Test hash computation with null values."""
        data = [
            ("1", "John Doe", "john@example.com"),
            ("2", None, "jane@example.com"),  # Null name
            ("3", "Bob Johnson", None)  # Null email
        ]
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True)
        ])
        
        df_with_nulls = spark.createDataFrame(data, schema)
        result_df = hash_manager.compute_scd_hash(df_with_nulls)
        
        # Should handle null values gracefully
        assert result_df.count() == df_with_nulls.count()
        assert hash_manager.config.scd_hash_column in result_df.columns
        
        # Records with null values should still have hashes
        null_hashes = result_df.filter(result_df.name.isNull() | result_df.email.isNull()).collect()
        for row in null_hashes:
            assert row[hash_manager.config.scd_hash_column] is not None
