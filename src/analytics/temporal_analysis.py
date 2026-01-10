"""
Temporal Pattern Analysis for Smart City IoT Data
Extracts time features and calculates traffic patterns
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def extract_time_features(df: DataFrame) -> DataFrame:
    """
    Extract temporal features from timestamp column
    
    Args: 
        df: Input DataFrame with 'timestamp' column
        
    Returns:
        DataFrame with added time features
    """
    df_temporal = df.withColumn("hour", F.hour("timestamp")) \
        .withColumn("day_of_week", F.dayofweek("timestamp")) \
        .withColumn("day_name",
            F.when(F.dayofweek("timestamp") == 1, "Sunday")
            .when(F.dayofweek("timestamp") == 2, "Monday")
            .when(F.dayofweek("timestamp") == 3, "Tuesday")
            .when(F.dayofweek("timestamp") == 4, "Wednesday")
            .when(F.dayofweek("timestamp") == 5, "Thursday")
            .when(F.dayofweek("timestamp") == 6, "Friday")
            .when(F.dayofweek("timestamp") == 7, "Saturday")
        ) \
        .withColumn("is_weekend",
            F.when(F. dayofweek("timestamp").isin([1, 7]), True).otherwise(False)
        )
    
    return df_temporal


def calculate_hourly_patterns(df: DataFrame) -> DataFrame:
    """
    Calculate hourly traffic patterns
    
    Args: 
        df: DataFrame with 'hour' and 'vehicle_count' columns
        
    Returns: 
        DataFrame with hourly aggregations
    """
    hourly = df.groupBy("hour").agg(
        F.avg("vehicle_count").alias("avg_vehicle_count"),
        F.min("vehicle_count").alias("min_vehicle_count"),
        F.max("vehicle_count").alias("max_vehicle_count"),
        F.count("vehicle_count").alias("count_readings")
    ).orderBy("hour")
    
    return hourly


def calculate_daily_patterns(df: DataFrame) -> DataFrame:
    """
    Calculate daily traffic patterns by day of week
    
    Args:
        df: DataFrame with 'day_of_week', 'day_name', and 'vehicle_count'
        
    Returns:
        DataFrame with daily aggregations
    """
    daily = df.groupBy("day_of_week", "day_name").agg(
        F.avg("vehicle_count").alias("avg_vehicle_count"),
        F.count("vehicle_count").alias("count_readings")
    ).orderBy("day_of_week")
    
    return daily


def identify_peak_hours(hourly_df: DataFrame, top_n: int = 3) -> DataFrame:
    """
    Identify top N peak traffic hours
    
    Args: 
        hourly_df: DataFrame from calculate_hourly_patterns()
        top_n: Number of peak hours to return
        
    Returns:
        DataFrame with top N hours
    """
    peaks = hourly_df.orderBy(F.desc("avg_vehicle_count")).limit(top_n)
    return peaks


def analyze_temporal_patterns(df: DataFrame) -> dict:
    """
    Complete temporal analysis pipeline
    
    Args:
        df: Input DataFrame with timestamp and vehicle_count
        
    Returns:
        Dictionary containing all temporal analysis results
    """
    # Extract time features
    df_temporal = extract_time_features(df)
    
    # Calculate patterns
    hourly = calculate_hourly_patterns(df_temporal)
    daily = calculate_daily_patterns(df_temporal)
    peaks = identify_peak_hours(hourly, top_n=3)
    
    return {
        "data_with_features": df_temporal,
        "hourly_patterns": hourly,
        "daily_patterns": daily,
        "peak_hours": peaks
    }