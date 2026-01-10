"""
Feature Engineering for Time Series Prediction
Creates lag features and rolling statistics
"""

from pyspark.sql import DataFrame
from pyspark. sql import functions as F
from pyspark.sql.window import Window


def create_lag_features(df: DataFrame, 
                       partition_col: str = "sensor_id",
                       order_col: str = "timestamp",
                       value_col: str = "vehicle_count") -> DataFrame:
    """
    Create lag features for time series prediction
    
    Args:
        df: Input DataFrame
        partition_col: Column to partition by (e.g., sensor_id)
        order_col: Column to order by (e.g., timestamp)
        value_col: Column to create lags for
        
    Returns: 
        DataFrame with lag features
    """
    window_spec = Window.partitionBy(partition_col).orderBy(order_col)
    
    df_lagged = df \
        .withColumn(f"prev_{value_col}", F.lag(value_col, 1).over(window_spec)) \
        .withColumn(f"prev_2_{value_col}", F.lag(value_col, 2).over(window_spec)) \
        .withColumn(f"next_{value_col}", F.lead(value_col, 1).over(window_spec)) \
        .withColumn(f"{value_col}_change", 
                   F.col(value_col) - F.col(f"prev_{value_col}"))
    
    return df_lagged


def create_rolling_features(df: DataFrame,
                           partition_col: str = "sensor_id",
                           order_col: str = "timestamp",
                           value_col: str = "vehicle_count") -> DataFrame:
    """
    Create rolling statistics features
    
    Args:
        df: Input DataFrame
        partition_col: Column to partition by
        order_col: Column to order by
        value_col: Column to calculate rolling stats for
        
    Returns:
        DataFrame with rolling features
    """
    # 3-hour window
    window_3hr = Window.partitionBy(partition_col) \
        .orderBy(order_col) \
        .rowsBetween(-2, 0)
    
    # 24-hour window (assuming hourly data)
    window_24hr = Window.partitionBy(partition_col) \
        .orderBy(order_col) \
        .rowsBetween(-23, 0)
    
    df_rolling = df \
        .withColumn("rolling_avg_3hr", F.avg(value_col).over(window_3hr)) \
        .withColumn("rolling_max_3hr", F.max(value_col).over(window_3hr)) \
        .withColumn("rolling_min_3hr", F.min(value_col).over(window_3hr)) \
        .withColumn("rolling_avg_24hr", F.avg(value_col).over(window_24hr)) \
        .withColumn("rolling_std_24hr", F.stddev(value_col).over(window_24hr))
    
    return df_rolling


def create_all_features(df: DataFrame,
                       partition_col: str = "sensor_id",
                       order_col: str = "timestamp",
                       value_col:  str = "vehicle_count") -> DataFrame:
    """
    Create all engineered features
    
    Args: 
        df: Input DataFrame
        partition_col: Column to partition by
        order_col: Column to order by
        value_col:  Column to engineer features for
        
    Returns: 
        DataFrame with all features
    """
    # Add row number for reference
    window_spec = Window.partitionBy(partition_col).orderBy(order_col)
    df = df.withColumn("row_num", F.row_number().over(window_spec))
    
    # Create lag features
    df = create_lag_features(df, partition_col, order_col, value_col)
    
    # Create rolling features
    df = create_rolling_features(df, partition_col, order_col, value_col)
    
    return df


def save_features(df: DataFrame, output_path: str):
    """
    Save engineered features to parquet
    
    Args:
        df: DataFrame with features
        output_path: Path to save features
    """
    df.write. mode("overwrite").parquet(output_path)
    print(f"✅ Features saved to:  {output_path}")