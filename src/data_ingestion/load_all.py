from pyspark.sql import SparkSession
from typing import Dict
from src.utils.logger import setup_logger
from src.utils.config_loader import Config

from src.data_ingestion.traffic_sensors import load_traffic_sensors
from src.data_ingestion.air_quality import load_air_quality
from src.data_ingestion.weather_stations import load_weather_data
from src.data_ingestion.energy_meters import load_energy_meters
from src.data_ingestion.city_zones import load_city_zones

logger = setup_logger(__name__)


def load_all_datasets(
    spark: SparkSession, data_dir: str = "data/samples", validate: bool = True
) -> Dict:
    """
    Load all SmartCity datasets at once

    Args:
        spark: SparkSession instance
        data_dir: Directory containing data files (default: data/samples)
        validate: Whether to perform data validation (default: True)

    Returns:
        Dict: Dictionary containing all loaded DataFrames

    Example:
        >>> from src.utils.spark_session import get_spark_session
        >>> from src.data_ingestion.load_all import load_all_datasets
        >>>
        >>> spark = get_spark_session("DataIngestion")
        >>> datasets = load_all_datasets(spark)
        >>>
        >>> traffic_df = datasets['traffic']
        >>> air_df = datasets['air_quality']
    """
    logger.info("🚀 Loading all SmartCity datasets...")

    datasets = {}

    try:
        # Load traffic sensors
        datasets["traffic"] = load_traffic_sensors(
            spark, f"{data_dir}/traffic_sample.csv", validate=validate
        )

        # Load air quality
        datasets["air_quality"] = load_air_quality(
            spark, f"{data_dir}/air_quality_sample.json", validate=validate
        )

        # Load weather data
        datasets["weather"] = load_weather_data(
            spark, f"{data_dir}/weather_sample.parquet", validate=validate
        )

        # Load energy meters
        datasets["energy"] = load_energy_meters(
            spark, f"{data_dir}/energy_sample.csv", validate=validate
        )

        # Load city zones
        datasets["city_zones"] = load_city_zones(
            spark, f"{data_dir}/city_zones_sample.csv"
        )

        logger.info(" Successfully loaded all datasets!")
        logger.info(f"   • Traffic: {datasets['traffic'].count()} records")
        logger.info(f"   • Air Quality: {datasets['air_quality'].count()} records")
        logger.info(f"   • Weather: {datasets['weather'].count()} records")
        logger.info(f"   • Energy: {datasets['energy'].count()} records")
        logger.info(f"   • City Zones: {datasets['city_zones'].count()} zones")

        return datasets

    except Exception as e:
        logger.error(f"Failed to load datasets: {e}")
        raise


def load_dataset_by_name(
    spark: SparkSession, dataset_name: str, filepath: str, validate: bool = True
):
    """
    Load a single dataset by name

    Args:
        spark: SparkSession instance
        dataset_name: Name of dataset ('traffic', 'air_quality', 'weather', 'energy', 'zones')
        filepath: Path to the data file
        validate: Whether to perform validation

    Returns:
        DataFrame: Loaded dataset

    Example:
        >>> spark = get_spark_session()
        >>> traffic = load_dataset_by_name(spark, 'traffic', 'data/samples/traffic_sample.csv')
    """
    loaders = {
        "traffic": load_traffic_sensors,
        "air_quality": load_air_quality,
        "weather": load_weather_data,
        "energy": load_energy_meters,
        "zones": load_city_zones,
    }

    if dataset_name not in loaders:
        raise ValueError(
            f"Unknown dataset: {dataset_name}. Choose from: {list(loaders.keys())}"
        )

    loader_func = loaders[dataset_name]

    # City zones doesn't have validate parameter
    if dataset_name == "zones":
        return loader_func(spark, filepath)
    else:
        return loader_func(spark, filepath, validate=validate)
