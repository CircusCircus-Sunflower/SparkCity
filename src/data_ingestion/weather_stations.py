from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
)
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def get_weather_schema():
    """
    Define explicit schema for weather data

    Returns:
        StructType: Schema definition
    """
    return StructType(
        [
            StructField("station_id", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("location_lat", DoubleType(), False),
            StructField("location_lon", DoubleType(), False),
            StructField("temperature", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("wind_speed", DoubleType(), True),
            StructField("wind_direction", StringType(), True),
            StructField("precipitation", DoubleType(), True),
            StructField("pressure", DoubleType(), True),
        ]
    )


def load_weather_data(
    spark: SparkSession, filepath: str, validate: bool = True
) -> DataFrame:
    """
    Load weather station data from Parquet

    Args:
        spark: SparkSession instance
        filepath: Path to weather data Parquet file
        validate: Whether to perform data validation (default: True)

    Returns:
        DataFrame: Loaded weather station data

    Example:
        >>> spark = get_spark_session()
        >>> weather_df = load_weather_data(spark, "data/samples/weather_sample.parquet")
    """
    logger.info(f"Loading weather data from {filepath}")

    try:
        df = spark.read.parquet(filepath)

        row_count = df.count()
        logger.info(f"Loaded {row_count} weather records")

        if validate:
            _validate_weather_data(df)

        return df

    except Exception as e:
        logger.error(f"Failed to load weather data: {e}")
        raise


def _validate_weather_data(df: DataFrame):
    """
    Validate weather data

    Args:
        df: Weather DataFrame to validate
    """
    # Check for unrealistic temperatures
    extreme_cold = df.filter(df.temperature < -50).count()
    extreme_hot = df.filter(df.temperature > 130).count()

    if extreme_cold > 0 or extreme_hot > 0:
        logger.warning(
            f"Found extreme temperatures: cold={extreme_cold}, hot={extreme_hot}"
        )

    # Check humidity range (0-100%)
    invalid_humidity = df.filter((df.humidity < 0) | (df.humidity > 100)).count()
    if invalid_humidity > 0:
        logger.warning(f"Found {invalid_humidity} records with invalid humidity")

    logger.info("Weather data validation complete")
