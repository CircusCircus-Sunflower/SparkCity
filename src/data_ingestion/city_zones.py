from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def get_city_zones_schema():
    """
    Define explicit schema for city zones data

    Returns:
        StructType: Schema definition
    """
    return StructType(
        [
            StructField("zone_id", StringType(), False),
            StructField("zone_name", StringType(), False),
            StructField("zone_type", StringType(), True),
            StructField("lat_min", DoubleType(), False),
            StructField("lat_max", DoubleType(), False),
            StructField("lon_min", DoubleType(), False),
            StructField("lon_max", DoubleType(), False),
            StructField("population", IntegerType(), True),
        ]
    )


def load_city_zones(spark: SparkSession, filepath: str) -> DataFrame:
    """
    Load city zones reference data from CSV

    Args:
        spark: SparkSession instance
        filepath: Path to city zones CSV file

    Returns:
        DataFrame: Loaded city zones data

    Example:
        >>> spark = get_spark_session()
        >>> zones_df = load_city_zones(spark, "data/samples/city_zones_sample.csv")
    """
    logger.info(f"Loading city zones data from {filepath}")

    try:
        schema = get_city_zones_schema()

        df = spark.read.csv(filepath, header=True, schema=schema)

        row_count = df.count()
        logger.info(f"Loaded {row_count} city zones")

        return df

    except Exception as e:
        logger.error(f"Failed to load city zones data: {e}")
        raise
