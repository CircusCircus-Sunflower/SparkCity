"""
Cross-Sensor Correlation Analysis
Analyzes relationships between traffic and air quality data
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark. sql import functions as F


def load_and_prepare_data(spark: SparkSession, 
                          traffic_path: str, 
                          air_quality_path: str) -> tuple:
    """
    Load traffic and air quality data
    
    Args:
        spark: SparkSession
        traffic_path: Path to traffic CSV
        air_quality_path:  Path to air quality JSON
        
    Returns:
        Tuple of (traffic_df, air_quality_df)
    """
    # Load traffic data
    traffic = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(traffic_path)
    
    traffic = traffic.withColumn(
        "timestamp", 
        F.to_timestamp(F.col("timestamp"))
    )
     # Load air quality data
    air_quality = spark.read \
        .option("inferSchema", "true") \
        .json(air_quality_path)
    
    # Check if timestamp column exists and convert if needed
    if "timestamp" in air_quality.columns:
        # Try to convert - if already timestamp, this is safe
        try:
            air_quality = air_quality.withColumn(
                "timestamp",
                F.to_timestamp(F.col("timestamp"))
            )
        except:
            # If conversion fails, might already be correct format
            pass
        
    air_quality = air_quality.withColumn(
        "timestamp",
        F.to_timestamp(F.col("timestamp"))
    )
    
    return traffic, air_quality


def aggregate_hourly(traffic_df: DataFrame, air_df: DataFrame) -> DataFrame:
    """
    Aggregate both datasets to hourly level and join
    
    Args:
        traffic_df: Traffic DataFrame
        air_df: Air quality DataFrame
        
    Returns: 
        Joined DataFrame with hourly aggregations
    """
    # Aggregate traffic to hourly
    traffic_hourly = traffic_df. withColumn(
        "hour_ts", F.date_trunc("hour", F.col("timestamp"))
    ).groupBy("hour_ts").agg(
        F.avg("vehicle_count").alias("avg_vehicles"),
        F.avg("avg_speed").alias("avg_speed")
    )
    
    # Aggregate air quality to hourly
    air_hourly = air_df.withColumn(
        "hour_ts", F.date_trunc("hour", F.col("timestamp"))
    ).groupBy("hour_ts").agg(
        F.avg("pm25").alias("avg_pm25"),
        F.avg("no2").alias("avg_no2"),
        F.avg("temperature").alias("avg_temp")
    )
    
    # Join datasets
    combined = traffic_hourly.join(air_hourly, "hour_ts", "inner")
    
    return combined


def calculate_correlations(combined_df: DataFrame) -> dict:
    """
    Calculate correlations between traffic and air quality metrics
    
    Args:
        combined_df: Joined DataFrame from aggregate_hourly()
        
    Returns:
        Dictionary of correlation coefficients
    """
    correlations = {
        "traffic_vs_pm25": combined_df.stat.corr("avg_vehicles", "avg_pm25"),
        "traffic_vs_no2":  combined_df.stat.corr("avg_vehicles", "avg_no2"),
        "speed_vs_pm25": combined_df.stat. corr("avg_speed", "avg_pm25"),
        "speed_vs_no2": combined_df.stat.corr("avg_speed", "avg_no2")
    }
    
    return correlations


def run_correlation_analysis(spark: SparkSession,
                             traffic_path: str,
                             air_quality_path: str) -> dict:
    """
    Complete correlation analysis pipeline
    
    Args:
        spark: SparkSession
        traffic_path: Path to traffic data
        air_quality_path:  Path to air quality data
        
    Returns:
        Dictionary with combined data and correlations
    """
    # Load data
    traffic, air_quality = load_and_prepare_data(
        spark, traffic_path, air_quality_path
    )
    
    # Aggregate and join
    combined = aggregate_hourly(traffic, air_quality)
    
    # Calculate correlations
    correlations = calculate_correlations(combined)
    
    return {
        "combined_data":  combined,
        "correlations":  correlations
    }