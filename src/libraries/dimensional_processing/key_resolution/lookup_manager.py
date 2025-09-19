"""
Lookup management for dimensional key resolution.
"""

from typing import List, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_date, broadcast
import logging

from ..common.config import KeyResolutionConfig
from ..common.exceptions import KeyResolutionError

logger = logging.getLogger(__name__)


class LookupManager:
    """Manages dimension lookups for key resolution."""
    
    def __init__(self, config: KeyResolutionConfig, spark: SparkSession):
        """
        Initialize LookupManager with configuration and Spark session.
        
        Args:
            config: Key resolution configuration
            spark: Spark session
        """
        self.config = config
        self.spark = spark
        
        logger.info(f"Initialized LookupManager for dimension: {config.dimension_table}")
    
    def get_current_dimension_records(self) -> DataFrame:
        """
        Get current dimension records.
        
        Returns:
            DataFrame with current dimension records
        """
        current_records = self.spark.sql(f"""
            SELECT {', '.join(self.config.business_key_columns)}, 
                   {self.config.surrogate_key_column}
            FROM {self.config.dimension_table}
            WHERE {self.config.is_current_column} = 'Y'
        """)
        
        record_count = current_records.count()
        logger.info(f"Retrieved {record_count} current records from {self.config.dimension_table}")
        return current_records
    
    def get_historical_dimension_records(self) -> DataFrame:
        """
        Get historical dimension records for date-based lookups.
        
        Returns:
            DataFrame with historical dimension records
        """
        historical_records = self.spark.sql(f"""
            SELECT {', '.join(self.config.business_key_columns)}, 
                   {self.config.surrogate_key_column},
                   {self.config.effective_start_column},
                   {self.config.effective_end_column}
            FROM {self.config.dimension_table}
            WHERE {self.config.effective_start_column} <= current_timestamp()
        """)
        
        record_count = historical_records.count()
        logger.info(f"Retrieved {record_count} historical records from {self.config.dimension_table}")
        return historical_records
    
    def resolve_current_keys(self, fact_df: DataFrame, 
                           dimension_df: DataFrame) -> DataFrame:
        """
        Resolve keys for current records.
        
        Args:
            fact_df: Fact table DataFrame
            dimension_df: Current dimension DataFrame
            
        Returns:
            DataFrame with resolved surrogate keys
        """
        logger.info("Resolving keys for current records")
        
        # Use broadcast join for small dimension tables
        if dimension_df.count() <= 100000:
            dimension_df = broadcast(dimension_df)
            logger.info("Using broadcast join for current dimension lookup")
        
        # Build join condition for business key columns
        join_conditions = []
        for bk_col in self.config.business_key_columns:
            join_conditions.append(fact_df[bk_col] == dimension_df[bk_col])
        
        # Combine all conditions with AND
        from pyspark.sql.functions import col
        join_condition = join_conditions[0]
        for condition in join_conditions[1:]:
            join_condition = join_condition & condition
        
        resolved_df = fact_df.join(
            dimension_df,
            join_condition,
            "left"
        )
        
        # Check for unresolved keys
        unresolved_count = resolved_df.filter(
            col(self.config.surrogate_key_column).isNull()
        ).count()
        
        if unresolved_count > 0:
            logger.warning(f"Found {unresolved_count} unresolved keys for current records")
        
        return resolved_df
    
    def resolve_historical_keys(self, fact_df: DataFrame, 
                              business_date_column: str) -> DataFrame:
        """
        Resolve keys for historical records using date-based lookup.
        
        Args:
            fact_df: Fact table DataFrame
            business_date_column: Column containing business date
            
        Returns:
            DataFrame with resolved surrogate keys
        """
        logger.info(f"Resolving keys for historical records using {business_date_column}")
        
        # Get historical dimension records
        historical_dimension = self.get_historical_dimension_records()
        
        # Build join condition for business key columns and date range
        join_conditions = []
        
        # Add business key conditions
        for bk_col in self.config.business_key_columns:
            join_conditions.append(fact_df[bk_col] == historical_dimension[bk_col])
        
        # Add date range conditions using proper column references
        join_conditions.extend([
            fact_df[business_date_column] >= historical_dimension[self.config.effective_start_column],
            (historical_dimension[self.config.effective_end_column].isNull()) | 
            (fact_df[business_date_column] < historical_dimension[self.config.effective_end_column])
        ])
        
        # Combine all conditions with AND
        join_condition = join_conditions[0]
        for condition in join_conditions[1:]:
            join_condition = join_condition & condition
        
        resolved_df = fact_df.join(
            historical_dimension,
            join_condition,
            "left"
        )
        
        # Check for unresolved keys
        unresolved_count = resolved_df.filter(
            col(self.config.surrogate_key_column).isNull()
        ).count()
        
        if unresolved_count > 0:
            logger.warning(f"Found {unresolved_count} unresolved keys for historical records")
        
        return resolved_df
    
    def resolve_mixed_keys(self, fact_df: DataFrame, 
                          business_date_column: str) -> DataFrame:
        """
        Resolve keys for both current and historical records.
        
        Args:
            fact_df: Fact table DataFrame
            business_date_column: Column containing business date
            
        Returns:
            DataFrame with resolved surrogate keys
        """
        logger.info("Resolving keys for mixed current and historical records")
        
        # Split fact data into current and historical
        current_fact = fact_df.filter(col(business_date_column) >= current_date())
        historical_fact = fact_df.filter(col(business_date_column) < current_date())
        
        # Resolve current records
        if not current_fact.isEmpty():
            current_dimension = self.get_current_dimension_records()
            current_resolved = self.resolve_current_keys(current_fact, current_dimension)
        else:
            current_resolved = self.spark.createDataFrame([], fact_df.schema)
        
        # Resolve historical records
        if not historical_fact.isEmpty():
            historical_resolved = self.resolve_historical_keys(historical_fact, business_date_column)
        else:
            historical_resolved = self.spark.createDataFrame([], fact_df.schema)
        
        # Union results
        if current_resolved.count() > 0 and historical_resolved.count() > 0:
            resolved_df = current_resolved.unionByName(historical_resolved)
        elif current_resolved.count() > 0:
            resolved_df = current_resolved
        elif historical_resolved.count() > 0:
            resolved_df = historical_resolved
        else:
            resolved_df = self.spark.createDataFrame([], fact_df.schema)
        
        logger.info(f"Resolved keys for {resolved_df.count()} total records")
        return resolved_df
    
    def validate_resolution_results(self, fact_df: DataFrame, 
                                  resolved_df: DataFrame) -> Dict[str, Any]:
        """
        Validate key resolution results.
        
        Args:
            fact_df: Original fact DataFrame
            resolved_df: Resolved DataFrame
            
        Returns:
            Dictionary with validation results
        """
        original_count = fact_df.count()
        resolved_count = resolved_df.count()
        
        # Check for data loss
        if resolved_count != original_count:
            raise KeyResolutionError(f"Data loss during key resolution: {original_count} -> {resolved_count}")
        
        # Check for unresolved keys
        unresolved_count = resolved_df.filter(
            col(self.config.surrogate_key_column).isNull()
        ).count()
        
        resolution_rate = (original_count - unresolved_count) / original_count if original_count > 0 else 0
        
        validation_results = {
            "original_records": original_count,
            "resolved_records": resolved_count,
            "unresolved_records": unresolved_count,
            "resolution_rate": resolution_rate,
            "data_loss": resolved_count != original_count
        }
        
        logger.info(f"Key resolution validation: {validation_results}")
        return validation_results
    
    def get_dimension_info(self) -> Dict[str, Any]:
        """
        Get information about the dimension table.
        
        Returns:
            Dictionary with dimension information
        """
        try:
            # Get total record count
            total_count = self.spark.sql(f"SELECT COUNT(*) as count FROM {self.config.dimension_table}").collect()[0]["count"]
            
            # Get current record count
            current_count = self.spark.sql(f"""
                SELECT COUNT(*) as count FROM {self.config.dimension_table}
                WHERE {self.config.is_current_column} = 'Y'
            """).collect()[0]["count"]
            
            # Get historical record count
            historical_count = total_count - current_count
            
            return {
                "dimension_table": self.config.dimension_table,
                "total_records": total_count,
                "current_records": current_count,
                "historical_records": historical_count,
                "business_key_columns": self.config.business_key_columns,
                "surrogate_key_column": self.config.surrogate_key_column
            }
            
        except Exception as e:
            logger.error(f"Failed to get dimension info: {str(e)}")
            return {"error": str(e)}
