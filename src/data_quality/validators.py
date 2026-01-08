
"""B2: Data Validation Framework
Person B - Data Quality Engineer
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull

def check_nulls(df: DataFrame, column: str = None, threshold: float = 0.2) -> dict:
    """
    Check for null values in DataFrame
    
    Args:
        df: PySpark DataFrame
        column: Specific column to check (None = all columns)
        threshold: Max acceptable null percentage (default 20%)
        
    Returns:
        dict: Results with columns exceeding threshold
    """
    total_rows = df.count()
    results = {
        'total_rows': total_rows,
        'violations': []
    }
    
    columns_to_check = [column] if column else df.columns
    
    for col_name in columns_to_check:
        null_count = df.filter(col(col_name).isNull()).count()
        null_percent = (null_count / total_rows) if total_rows > 0 else 0
        
        if null_percent > threshold:
            results['violations'].append({
                'column': col_name,
                'null_count': null_count,
                'null_percent': null_percent,
                'threshold': threshold,
                'status': 'FAIL'
            })
    
    return results


def check_ranges(df: DataFrame, column: str, min_val: float, max_val: float) -> dict:
    """Check if values are within acceptable range"""
    total_rows = df.count()
    
    out_of_range = df.filter(
        (col(column) < min_val) | (col(column) > max_val)
    ).count()
    
    out_of_range_percent = (out_of_range / total_rows * 100) if total_rows > 0 else 0
    
    return {
        'column': column,
        'expected_range': f'[{min_val}, {max_val}]',
        'violations': out_of_range,
        'violation_percent': out_of_range_percent,
        'status': 'PASS' if out_of_range == 0 else 'FAIL'
    }


def check_duplicates(df: DataFrame, subset: list = None) -> dict:
    """Check for duplicate rows"""
    total_rows = df.count()
    
    if subset:
        unique_rows = df.dropDuplicates(subset=subset).count()
    else:
        unique_rows = df.dropDuplicates().count()
    
    duplicate_count = total_rows - unique_rows
    duplicate_percent = (duplicate_count / total_rows * 100) if total_rows > 0 else 0
    
    return {
        'total_rows': total_rows,
        'unique_rows': unique_rows,
        'duplicate_count': duplicate_count,
        'duplicate_percent': duplicate_percent,
        'status': 'PASS' if duplicate_count == 0 else 'WARNING'
    }


def validate_timestamp(df: DataFrame, timestamp_col: str = 'timestamp') -> dict:
    """Validate timestamp consistency for time series"""
    from pyspark.sql.functions import current_timestamp, lag
    from pyspark.sql.window import Window
    
    total_rows = df.count()
    issues = []
    
    null_timestamps = df.filter(col(timestamp_col).isNull()).count()
    if null_timestamps > 0:
        issues.append(f"{null_timestamps} null timestamps")
    
    future_timestamps = df.filter(
        col(timestamp_col) > current_timestamp()
    ).count()
    if future_timestamps > 0:
        issues.append(f"{future_timestamps} future timestamps")
    
    return {
        'total_rows': total_rows,
        'issues': issues,
        'status': 'PASS' if len(issues) == 0 else 'FAIL'
    }


def check_data_types(df: DataFrame, expected_schema: dict) -> dict:
    """Enforce schema - check if data types match expectations"""
    actual_types = {field.name: str(field.dataType) for field in df.schema.fields}
    
    mismatches = []
    for col_name, expected_type in expected_schema.items():
        if col_name not in actual_types:
            mismatches.append({
                'column': col_name,
                'issue': 'MISSING',
                'expected': expected_type,
                'actual': None
            })
    
    return {
        'schema_valid': len(mismatches) == 0,
        'mismatches': mismatches,
        'status': 'PASS' if len(mismatches) == 0 else 'FAIL'
    }


# Test function
def test_validators():
    """Test validators with mock data"""
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.appName("ValidatorTest").master("local[*]").getOrCreate()
    
    mock_data = [
        ('sensor1', 50, 65.0),
        ('sensor2', 200, 150.0),
        ('sensor3', None, 55.0),
    ]
    
    df = spark.createDataFrame(mock_data, ['sensor_id', 'vehicle_count', 'avg_speed'])
    
    print("🧪 Testing Validators\n")
    
    print("1. check_ranges (speed 0-120):")
    result = check_ranges(df, 'avg_speed', 0, 120)
    print(f"   Status: {result['status']}, Violations: {result['violations']}\n")
    
    print("2. check_nulls (threshold 20%):")
    result = check_nulls(df, threshold=0.2)
    print(f"   Violations: {len(result['violations'])}\n")
    
    print("✅ Tests complete!")
    spark.stop()


if __name__ == "__main__":
    test_validators()

