from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    hour,
    dayofweek,
    dayofmonth,
    month,
    year,
    quarter,
    date_format,
    when,
)
from src.utils.config_loader import Config
from src.utils.logger import setup_logger
from datetime import datetime

logger = setup_logger(__name__)


class DatabaseWriter:
    """
    Handles writing Spark DataFrames to PostgreSQL
    """

    def __init__(self, spark: SparkSession):
        """
        Initialize database writer

        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        self.config = Config()
        self.pg_config = self.config.get_postgres_config()

        # JDBC connection properties
        self.jdbc_url = self.pg_config["jdbc_url"]
        self.connection_properties = {
            "user": self.pg_config["user"],
            "password": self.pg_config["password"],
            "driver": "org.postgresql.Driver",
        }

        logger.info(f"DatabaseWriter initialized for {self.jdbc_url}")

    def _get_or_create_time_dimension(self, df: DataFrame) -> DataFrame:
        """
        Ensure time dimension entries exist for all timestamps in DataFrame

        Args:
            df: DataFrame with 'timestamp' column

        Returns:
            DataFrame with time_id added
        """
        logger.info("Processing time dimension...")

        # Extract time attributes
        time_df = df.select("timestamp").distinct()

        time_df = (
            time_df.withColumn("hour", hour("timestamp"))
            .withColumn("day", dayofmonth("timestamp"))
            .withColumn("month", month("timestamp"))
            .withColumn("year", year("timestamp"))
            .withColumn("day_of_week", dayofweek("timestamp"))
            .withColumn("day_name", date_format("timestamp", "EEEE"))
            .withColumn(
                "is_weekend",
                when(dayofweek("timestamp").isin([1, 7]), True).otherwise(False),
            )
            .withColumn("quarter", quarter("timestamp"))
        )

        # Write to dim_time (will skip duplicates due to UNIQUE constraint)
        time_df.write.jdbc(
            url=self.jdbc_url,
            table="dim_time",
            mode="append",
            properties=self.connection_properties,
        )

        logger.info(
            f"✅ Time dimension updated with {time_df.count()} unique timestamps"
        )

        # Now read back with time_ids
        existing_time = self.spark.read.jdbc(
            url=self.jdbc_url, table="dim_time", properties=self.connection_properties
        ).select("time_id", "timestamp")

        # Join to get time_ids
        df_with_time = df.join(existing_time, "timestamp", "left")

        return df_with_time

    def write_traffic_data(self, df: DataFrame, mode: str = "append"):
        """
        Write traffic sensor data to fact_traffic

        Args:
            df: Traffic DataFrame from data ingestion
            mode: Write mode ('append', 'overwrite')
        """
        logger.info("Writing traffic data to database...")

        # Add time dimension
        df_with_time = self._get_or_create_time_dimension(df)

        # Select and rename columns to match fact_traffic schema
        fact_df = df_with_time.select(
            col("sensor_id"),
            col("time_id"),
            col("timestamp"),
            col("vehicle_count"),
            col("avg_speed"),
            col("congestion_level"),
            col("road_type"),
        )

        # Write to database
        fact_df.write.jdbc(
            url=self.jdbc_url,
            table="fact_traffic",
            mode=mode,
            properties=self.connection_properties,
        )

        row_count = fact_df.count()
        logger.info(f"✅ Written {row_count} traffic records to database")

    def write_air_quality_data(self, df: DataFrame, mode: str = "append"):
        """
        Write air quality data to fact_air_quality

        Args:
            df: Air quality DataFrame from data ingestion
            mode: Write mode ('append', 'overwrite')
        """
        logger.info("Writing air quality data to database...")

        # Add time dimension
        df_with_time = self._get_or_create_time_dimension(df)

        # Calculate AQI category based on PM2.5
        df_with_aqi = df_with_time.withColumn(
            "aqi_category",
            when(col("pm25") <= 12, "Good")
            .when(col("pm25") <= 35.4, "Moderate")
            .when(col("pm25") <= 55.4, "Unhealthy for Sensitive")
            .when(col("pm25") <= 150.4, "Unhealthy")
            .when(col("pm25") <= 250.4, "Very Unhealthy")
            .otherwise("Hazardous"),
        )

        # Select columns
        fact_df = df_with_aqi.select(
            col("sensor_id"),
            col("time_id"),
            col("timestamp"),
            col("pm25"),
            col("pm10"),
            col("no2"),
            col("co"),
            col("temperature"),
            col("humidity"),
            col("aqi_category"),
        )

        # Write to database
        fact_df.write.jdbc(
            url=self.jdbc_url,
            table="fact_air_quality",
            mode=mode,
            properties=self.connection_properties,
        )

        row_count = fact_df.count()
        logger.info(f"✅ Written {row_count} air quality records to database")

    def write_weather_data(self, df: DataFrame, mode: str = "append"):
        """
        Write weather station data to fact_weather

        Args:
            df: Weather DataFrame from data ingestion
            mode: Write mode ('append', 'overwrite')
        """
        logger.info("Writing weather data to database...")

        # Add time dimension
        df_with_time = self._get_or_create_time_dimension(df)

        # Rename station_id to match fact table
        fact_df = df_with_time.select(
            col("station_id"),
            col("time_id"),
            col("timestamp"),
            col("temperature"),
            col("humidity"),
            col("wind_speed"),
            col("wind_direction"),
            col("precipitation"),
            col("pressure"),
        )

        # Write to database
        fact_df.write.jdbc(
            url=self.jdbc_url,
            table="fact_weather",
            mode=mode,
            properties=self.connection_properties,
        )

        row_count = fact_df.count()
        logger.info(f"✅ Written {row_count} weather records to database")

    def write_energy_data(self, df: DataFrame, mode: str = "append"):
        """
        Write energy meter data to fact_energy

        Args:
            df: Energy DataFrame from data ingestion
            mode: Write mode ('append', 'overwrite')
        """
        logger.info("Writing energy data to database...")

        # Add time dimension
        df_with_time = self._get_or_create_time_dimension(df)

        # Select columns
        fact_df = df_with_time.select(
            col("meter_id"),
            col("time_id"),
            col("timestamp"),
            col("building_type"),
            col("power_consumption"),
            col("voltage"),
            col("current"),
            col("power_factor"),
        )

        # Write to database
        fact_df.write.jdbc(
            url=self.jdbc_url,
            table="fact_energy",
            mode=mode,
            properties=self.connection_properties,
        )

        row_count = fact_df.count()
        logger.info(f"Written {row_count} energy records to database")

    def write_sensors_dimension(
        self,
        traffic_df: DataFrame,
        air_df: DataFrame,
        weather_df: DataFrame,
        energy_df: DataFrame,
        zones_df: DataFrame,
    ):
        """
        Populate dim_sensors from all sensor DataFrames

        Args:
            traffic_df: Traffic sensors DataFrame
            air_df: Air quality sensors DataFrame
            weather_df: Weather stations DataFrame
            energy_df: Energy meters DataFrame
            zones_df: City zones DataFrame for zone mapping
        """
        logger.info("Populating sensors dimension...")

        # Traffic sensors
        traffic_sensors = (
            traffic_df.select(
                col("sensor_id"), col("location_lat"), col("location_lon")
            )
            .distinct()
            .withColumn("sensor_type", col("sensor_id").substr(1, 7))
        )

        # Air quality sensors
        air_sensors = (
            air_df.select(col("sensor_id"), col("location_lat"), col("location_lon"))
            .distinct()
            .withColumn("sensor_type", col("sensor_id").substr(1, 3))
        )

        # Weather stations
        weather_sensors = (
            weather_df.select(
                col("station_id").alias("sensor_id"),
                col("location_lat"),
                col("location_lon"),
            )
            .distinct()
            .withColumn("sensor_type", col("sensor_id").substr(1, 7))
        )

        # Energy meters
        energy_sensors = (
            energy_df.select(
                col("meter_id").alias("sensor_id"),
                col("location_lat"),
                col("location_lon"),
            )
            .distinct()
            .withColumn("sensor_type", col("sensor_id").substr(1, 6))
        )

        # Union all sensors
        all_sensors = (
            traffic_sensors.union(air_sensors)
            .union(weather_sensors)
            .union(energy_sensors)
        )

        # Map to zones (simple box matching)
        # For each sensor, find which zone it falls into
        sensors_with_zones = (
            all_sensors.crossJoin(
                zones_df.select("zone_id", "lat_min", "lat_max", "lon_min", "lon_max")
            )
            .filter(
                (col("location_lat") >= col("lat_min"))
                & (col("location_lat") <= col("lat_max"))
                & (col("location_lon") >= col("lon_min"))
                & (col("location_lon") <= col("lon_max"))
            )
            .select(
                "sensor_id", "sensor_type", "location_lat", "location_lon", "zone_id"
            )
            .distinct()
        )

        # Write to database
        sensors_with_zones.write.jdbc(
            url=self.jdbc_url,
            table="dim_sensors",
            mode="append",
            properties=self.connection_properties,
        )

        sensor_count = sensors_with_zones.count()
        logger.info(f"Written {sensor_count} sensors to dimension table")

    def write_zones_dimension(self, df: DataFrame):
        """
        Write city zones to dim_zones

        Args:
            df: City zones DataFrame
        """
        logger.info("Writing zones dimension...")

        df.write.jdbc(
            url=self.jdbc_url,
            table="dim_zones",
            mode="append",
            properties=self.connection_properties,
        )

        zone_count = df.count()
        logger.info(f"Written {zone_count} zones to dimension table")

    def write_all_data(self, datasets: dict, mode: str = "append"):
        """
        Write all datasets to database in correct order

        Args:
            datasets: Dictionary with keys: 'traffic', 'air_quality', 'weather', 'energy', 'city_zones'
            mode: Write mode ('append', 'overwrite')
        """
        logger.info("Starting full database write...")

        try:
            # Step 1: Write dimensions first (zones, then sensors)
            logger.info("Step 1: Writing dimension tables...")
            self.write_zones_dimension(datasets["city_zones"])
            self.write_sensors_dimension(
                datasets["traffic"],
                datasets["air_quality"],
                datasets["weather"],
                datasets["energy"],
                datasets["city_zones"],
            )

            # Step 2: Write fact tables
            logger.info("Step 2: Writing fact tables...")
            self.write_traffic_data(datasets["traffic"], mode=mode)
            self.write_air_quality_data(datasets["air_quality"], mode=mode)
            self.write_weather_data(datasets["weather"], mode=mode)
            self.write_energy_data(datasets["energy"], mode=mode)

            logger.info(" All data written to database successfully!")

        except Exception as e:
            logger.error(f"Error writing to database: {e}")
            raise
