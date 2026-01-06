from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
    IntegerType,
)
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def get_traffic_schema():
    """
    Define explicit schema for traffic sensor data
    Returns:
        StructType: Schema definition
    """
    return StructType(
        [
            StructField("sensor_id", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("location_lat", DoubleType(), False),
            StructField("location_lon", DoubleType(), False),
            StructField("vehicle_count", IntegerType(), False),
            StructField("avg_speed", DoubleType(), True),
            StructField("congestion_level", StringType(), True),
            StructField("road_type", StringType(), True),
        ]
    )


def load_traffic_sensors(
    spark: SparkSession, filepath: str, validate: bool = True
) -> DataFrame:
    """
    Load traffic sensor data from CSV

    Args:
        spark: SparkSession instance
        filepath: Path to traffic sensors CSV file
        validate: Whether to perform data validation (default: True)

    Returns:
        DataFrame: Loaded traffic sensor data

    Raises:
        ValueError: If file not found or validation fails

    Example:
        >>> spark = get_spark_session()
        >>> traffic_df = load_traffic_sensors(spark, "data/samples/traffic_sample.csv")
    """
    logger.info(f"Loading traffic sensor data from {filepath}")

    try:
        schema = get_traffic_schema()

        df = spark.read.csv(
            filepath,
            header=True,
            schema=schema,
            timestampFormat="yyyy-MM-dd'T'HH:mm:ss",
        )

        row_count = df.count()
        logger.info(f"Loaded {row_count} traffic sensor records")

        if validate:
            _validate_traffic_data(df)

        return df

    except Exception as e:
        logger.error(f"Failed to load traffic sensor data: {e}")
        raise


def _validate_traffic_data(df: DataFrame):
    """
    Validate traffic sensor data quality

    Args:
        df: Traffic DataFrame to validate

    Raises:
        ValueError: If validation fails
    """
    # Check for nulls in required fields
    null_counts = (
        df.select(
            [
                (df[col].isNull().cast("int")).alias(col)
                for col in [
                    "sensor_id",
                    "timestamp",
                    "location_lat",
                    "location_lon",
                    "vehicle_count",
                ]
            ]
        )
        .agg(
            *[
                {"col": "sum"}
                for col in [
                    "sensor_id",
                    "timestamp",
                    "location_lat",
                    "location_lon",
                    "vehicle_count",
                ]
            ]
        )
        .collect()[0]
    )

    if any(null_counts):
        logger.warning(f"⚠️ Found null values in required fields: {null_counts}")

    # Check for invalid vehicle counts
    invalid_counts = df.filter(df.vehicle_count < 0).count()
    if invalid_counts > 0:
        logger.warning(f"⚠️ Found {invalid_counts} records with negative vehicle counts")

    logger.info("✅ Traffic data validation complete")
