from pyspark.sql import SparkSession
import yaml
import os
import socket  # <--- Added this to find IP addresses

# connects your Python code to the Spark Cluster running in Docker.


def load_config():
    """Load configuration from team_config.yaml"""
    config_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "config", "team_config.yaml"
    )
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_local_ip():
    """
    Finds the computer's actual IP address on the network (e.g. 192.168.1.5)
    so Docker containers can send data back to it.
    """
    try:
        # We don't actually connect to Google (8.8.8.8), but we use it
        # to ask the OS "which network card would I use to get to the internet?"
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip_address = s.getsockname()[0]
        s.close()
        return ip_address
    except Exception:
        return "127.0.0.1"


def get_spark_session(app_name=None):
    """
    Create or get existing Spark session connected to cluster
    """
    config = load_config()

    if app_name is None:
        app_name = config["spark"]["app_name"]

    # Calculate the dynamic IP
    my_ip = get_local_ip()
    print(f"🚀 Launching Spark with Driver Host: {my_ip}")

    spark = (
        SparkSession.builder.appName(app_name)
        .master(config["spark"]["master_url"])
        .config("spark.executor.memory", config["spark"]["executor_memory"])
        .config("spark.driver.memory", config["spark"]["driver_memory"])
        # --- CRITICAL CHANGES BELOW ---
        # 1. Tell Docker specifically where to find THIS computer
        .config("spark.driver.host", my_ip)
        # 2. Open the door to receive connections from outside (Docker)
        .config("spark.driver.bindAddress", "0.0.0.0")
        # -----------------------------
        .getOrCreate()
    )

    return spark


def stop_spark_session(spark):
    """
    Safely stop Spark session
    """
    if spark:
        spark.stop()


# Example usage for testing
if __name__ == "__main__":
    print("Testing Spark session creation")
    spark = get_spark_session("TestSession")
    print("Session created successfully!")
    stop_spark_session(spark)


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
