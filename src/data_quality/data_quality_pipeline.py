"""
DATA QUALITY ENGINEER - DAY 2 STARTER TEMPLATE
Person B - SparkCity Project

This is YOUR main file for Day 2 work!

Author: [Your Name]
Date: January 7, 2026
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, when, isnan, isnull, 
    avg, min, max, stddev,
    lag, lead, first, last,
    trim, upper, lower, round as spark_round,
    to_timestamp, hour, dayofweek, month
)
from pyspark.sql.window import Window

# ============================================================================
# STEP 1: INITIALIZE SPARK
# ============================================================================

def create_spark_session():
    """Create and return a Spark session"""
    spark = SparkSession.builder \
        .appName("SparkCity-DataQuality") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    print("✅ Spark session created!")
    print(f"   Spark version: {spark.version}")
    return spark


# ============================================================================
# STEP 2: DATA QUALITY ASSESSMENT
# ============================================================================

def assess_data_quality(df, dataset_name):
    """
    Generate comprehensive data quality report
    
    Args:
        df: PySpark DataFrame
        dataset_name: String name for the report
        
    Returns:
        Dictionary with quality metrics
    """
    print(f"\n{'='*70}")
    print(f"🔍 DATA QUALITY ASSESSMENT: {dataset_name}")
    print(f"{'='*70}\n")
    
    total_rows = df.count()
    total_cols = len(df.columns)
    
    print("📋 BASIC INFORMATION")
    print(f"   Total Rows: {total_rows:,}")
    print(f"   Total Columns: {total_cols}")
    
    # Schema
    print("\n📝 SCHEMA")
    df.printSchema()
    
    # Missing values
    print("\n❌ MISSING VALUES ANALYSIS")
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        if null_count > 0:
            null_percent = (null_count / total_rows) * 100
            print(f"   {column:30s}: {null_count:6,} ({null_percent:5.2f}%)")
    
    # Sample data
    print("\n🔍 SAMPLE DATA (First 5 rows)")
    df.show(5)
    
    return {
        'total_rows': total_rows,
        'total_cols': total_cols
    }


# ============================================================================
# SIMPLE TEST FUNCTION
# ============================================================================

def test_pipeline():
    """Simple test to make sure everything works"""
    print("\n" + "="*70)
    print("🧪 TESTING DATA QUALITY PIPELINE")
    print("="*70)
    
    # Create Spark session
    spark = create_spark_session()
    
    # Create sample data for testing
    data = [
        ("sensor1", 100, 65.5),
        ("sensor2", 150, 72.0),
        ("sensor3", 120, 68.5)
    ]
    df = spark.createDataFrame(data, ["sensor_id", "count", "temperature"])
    
    # Test the assessment function
    assess_data_quality(df, "Test Data")
    
    print("\n✅ Pipeline test complete!")
    spark.stop()


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    # Run test
    test_pipeline()

    """
Data Quality Pipeline - Day 2
SparkCity Project
"""

print("Hello from Data Quality Module!")
print("Testing imports...")

try:
    from pyspark.sql import SparkSession
    print("✅ PySpark imported successfully!")
    
    # Create a simple Spark session
    spark = SparkSession.builder \
        .appName("SparkCity-Test") \
        .master("local[*]") \
        .getOrCreate()
    
    print(f"✅ Spark session created! Version: {spark.version}")
    
    # Create simple test data
    data = [("sensor1", 100), ("sensor2", 200)]
    df = spark.createDataFrame(data, ["id", "value"])
    
    print("\n📊 Test DataFrame:")
    df.show()
    
    print("\n🎉 Everything works!")
    spark.stop()
    
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Run: pip install pyspark")
except Exception as e:
    print(f"❌ Error: {e}")