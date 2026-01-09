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


def get_air_quality_schema():
    """
    Define explicit schema for air quality data

    Returns:
        StructType: Schema definition
    """
    return StructType(
        [
            StructField("sensor_id", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("location_lat", DoubleType(), False),
            StructField("location_lon", DoubleType(), False),
            StructField("pm25", DoubleType(), True),
            StructField("pm10", DoubleType(), True),
            StructField("no2", DoubleType(), True),
            StructField("co", DoubleType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
        ]
    )


def load_air_quality(
    spark: SparkSession, filepath: str, validate: bool = True
) -> DataFrame:
    """
    Load air quality data from JSON

    Args:
        spark: SparkSession instance
        filepath: Path to air quality JSON file
        validate: Whether to perform data validation (default: True)

    Returns:
        DataFrame: Loaded air quality data

    Example:
        >>> spark = get_spark_session()
        >>> air_df = load_air_quality(spark, "data/samples/air_quality_sample.json")
    """
    logger.info(f"Loading air quality data from {filepath}")

    try:
        schema = get_air_quality_schema()

        df = spark.read.json(
            filepath, schema=schema, timestampFormat="yyyy-MM-dd'T'HH:mm:ss"
        )

        row_count = df.count()
        logger.info(f"✅ Loaded {row_count} air quality records")

        if validate:
            _validate_air_quality_data(df)

        return df

    except Exception as e:
        logger.error(f"❌ Failed to load air quality data: {e}")
        raise


def _validate_air_quality_data(df: DataFrame):
    """
    Validate air quality data

    Args:
        df: Air quality DataFrame to validate
    """
    # Check for negative pollutant values
    negative_pm25 = df.filter(df.pm25 < 0).count()
    negative_pm10 = df.filter(df.pm10 < 0).count()

    if negative_pm25 > 0 or negative_pm10 > 0:
        logger.warning(
            f"⚠️ Found negative pollutant values: PM2.5={negative_pm25}, PM10={negative_pm10}"
        )

    # Check for extreme values (likely errors)
    extreme_pm25 = df.filter(df.pm25 > 500).count()
    if extreme_pm25 > 0:
        logger.warning(f"⚠️ Found {extreme_pm25} records with PM2.5 > 500 (hazardous)")

    logger.info("✅ Air quality data validation complete")
