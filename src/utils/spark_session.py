from pyspark.sql import SparkSession
import yaml
import os

# connects your Python code to the Spark Clusster running in Docker.


def load_config():
    """Load configuration from team_config.yaml"""
    config_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "config", "team_config.yaml"
    )
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_spark_session(app_name=None):
    """
    Create or get existing Spark session connected to cluster

    Args:
        app_name (str): Optional custom application name

    Returns:
        SparkSession: Configured Spark session

    Example:
        >>> from src.utils.spark_session import get_spark_session
        >>> spark = get_spark_session("MyAnalysis")
        >>> df = spark.read.csv("data/raw/traffic.csv")
    """
    config = load_config()

    if app_name is None:
        app_name = config["spark"]["app_name"]

    spark = (
        SparkSession.builder.appName(app_name)
        .master(config["spark"]["master_url"])
        .config("spark.executor.memory", config["spark"]["executor_memory"])
        .config("spark.driver.memory", config["spark"]["driver_memory"])
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "localhost")
        .getOrCreate()
    )

    return spark


def stop_spark_session(spark):
    """
    Safely stop Spark session

    Args:
        spark (SparkSession): Spark session to stop
    """
    if spark:
        spark.stop()


# Example usage for testing
if __name__ == "__main__":
    print("Testing Spark session creation")
