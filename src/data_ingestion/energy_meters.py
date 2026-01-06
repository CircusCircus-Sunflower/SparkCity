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


def get_energy_schema():
    """
    Define explicit schema for energy meter data

    Returns:
        StructType: Schema definition
    """
    return StructType(
        [
            StructField("meter_id", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("building_type", StringType(), True),
            StructField("location_lat", DoubleType(), False),
            StructField("location_lon", DoubleType(), False),
            StructField("power_consumption", DoubleType(), True),
            StructField("voltage", DoubleType(), True),
            StructField("current", DoubleType(), True),
            StructField("power_factor", DoubleType(), True),
        ]
    )


def load_energy_meters(
    spark: SparkSession, filepath: str, validate: bool = True
) -> DataFrame:
    """
    Load energy meter data from CSV

    Args:
        spark: SparkSession instance
        filepath: Path to energy meters CSV file
        validate: Whether to perform data validation (default: True)

    Returns:
        DataFrame: Loaded energy meter data

    Example:
        >>> spark = get_spark_session()
        >>> energy_df = load_energy_meters(spark, "data/samples/energy_sample.csv")
    """
    logger.info(f"Loading energy meter data from {filepath}")

    try:
        schema = get_energy_schema()

        df = spark.read.csv(
            filepath,
            header=True,
            schema=schema,
            timestampFormat="yyyy-MM-dd'T'HH:mm:ss",
        )

        row_count = df.count()
        logger.info(f"Loaded {row_count} energy meter records")

        if validate:
            _validate_energy_data(df)

        return df

    except Exception as e:
        logger.error(f"Failed to load energy meter data: {e}")
        raise


def _validate_energy_data(df: DataFrame):
    """
    Validate energy meter data

    Args:
        df: Energy DataFrame to validate
    """
    # Check for negative power consumption
    negative_power = df.filter(df.power_consumption < 0).count()
    if negative_power > 0:
        logger.warning(
            f"Found {negative_power} records with negative power consumption"
        )

    # Check voltage range (typical 110-125V for residential)
    low_voltage = df.filter(df.voltage < 100).count()
    high_voltage = df.filter(df.voltage > 130).count()

    if low_voltage > 0 or high_voltage > 0:
        logger.warning(f"Found unusual voltage: low={low_voltage}, high={high_voltage}")

    # Check power factor range (0-1)
    invalid_pf = df.filter((df.power_factor < 0) | (df.power_factor > 1)).count()
    if invalid_pf > 0:
        logger.warning(f"Found {invalid_pf} records with invalid power factor")

    logger.info("Energy data validation complete")
