import pandas as pd
import random
from datetime import datetime, timedelta
import json
import os

# Create samples directory if it doesn't exist
os.makedirs("data/samples", exist_ok=True)


def generate_traffic_sensors(n=500):
    """Generate sample traffic sensor data"""
    print("Generating traffic sensor data...")
    data = []
    sensor_ids = [f"TRAFFIC_{i:03d}" for i in range(10)]
    road_types = ["Highway", "Main Street", "Side Street", "Avenue"]
    congestion_levels = ["Low", "Medium", "High", "Critical"]

    for i in range(n):
        data.append(
            {
                "sensor_id": random.choice(sensor_ids),
                "timestamp": (
                    datetime.now() - timedelta(hours=random.randint(0, 168))
                ).isoformat(),
                "location_lat": 39.95 + random.uniform(-0.05, 0.05),
                "location_lon": -75.16 + random.uniform(-0.05, 0.05),
                "vehicle_count": random.randint(0, 500),
                "avg_speed": round(random.uniform(15, 70), 2),
                "congestion_level": random.choice(congestion_levels),
                "road_type": random.choice(road_types),
            }
        )

    df = pd.DataFrame(data)
    df.to_csv("data/samples/traffic_sample.csv", index=False)
    print(f"✅ Generated traffic_sample.csv ({len(df)} rows)")
    return df


def generate_air_quality(n=500):
    """Generate sample air quality data"""
    print("Generating air quality data...")
    data = []
    sensor_ids = [f"AIR_{i:03d}" for i in range(8)]

    for i in range(n):
        data.append(
            {
                "sensor_id": random.choice(sensor_ids),
                "timestamp": (
                    datetime.now() - timedelta(hours=random.randint(0, 168))
                ).isoformat(),
                "location_lat": 39.95 + random.uniform(-0.05, 0.05),
                "location_lon": -75.16 + random.uniform(-0.05, 0.05),
                "pm25": round(random.uniform(5, 150), 2),
                "pm10": round(random.uniform(10, 200), 2),
                "no2": round(random.uniform(10, 100), 2),
                "co": round(random.uniform(0.1, 5), 2),
                "temperature": round(random.uniform(50, 95), 2),
                "humidity": round(random.uniform(30, 90), 2),
            }
        )

    # Save as JSON
    with open("data/samples/air_quality_sample.json", "w") as f:
        json.dump(data, f, indent=2)
    print(f"✅ Generated air_quality_sample.json ({len(data)} rows)")
    return data


def generate_weather_data(n=500):
    """Generate sample weather station data"""
    print("Generating weather data...")
    data = []
    station_ids = [f"WEATHER_{i:03d}" for i in range(5)]
    directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]

    for i in range(n):
        data.append(
            {
                "station_id": random.choice(station_ids),
                "timestamp": (
                    datetime.now() - timedelta(hours=random.randint(0, 168))
                ).isoformat(),
                "location_lat": 39.95 + random.uniform(-0.05, 0.05),
                "location_lon": -75.16 + random.uniform(-0.05, 0.05),
                "temperature": round(random.uniform(50, 95), 2),
                "humidity": round(random.uniform(30, 90), 2),
                "wind_speed": round(random.uniform(0, 30), 2),
                "wind_direction": random.choice(directions),
                "precipitation": round(random.uniform(0, 2), 2),
                "pressure": round(random.uniform(29.5, 30.5), 2),
            }
        )

    df = pd.DataFrame(data)
    # Save as parquet
    df.to_parquet("data/samples/weather_sample.parquet", index=False)
    print(f"✅ Generated weather_sample.parquet ({len(df)} rows)")
    return df


def generate_energy_meters(n=500):
    """Generate sample energy meter data"""
    print("Generating energy meter data...")
    data = []
    meter_ids = [f"ENERGY_{i:03d}" for i in range(12)]
    building_types = ["Residential", "Commercial", "Industrial", "Mixed-Use"]

    for i in range(n):
        data.append(
            {
                "meter_id": random.choice(meter_ids),
                "timestamp": (
                    datetime.now() - timedelta(hours=random.randint(0, 168))
                ).isoformat(),
                "building_type": random.choice(building_types),
                "location_lat": 39.95 + random.uniform(-0.05, 0.05),
                "location_lon": -75.16 + random.uniform(-0.05, 0.05),
                "power_consumption": round(random.uniform(10, 500), 2),
                "voltage": round(random.uniform(110, 125), 2),
                "current": round(random.uniform(5, 50), 2),
                "power_factor": round(random.uniform(0.8, 1.0), 2),
            }
        )

    df = pd.DataFrame(data)
    df.to_csv("data/samples/energy_sample.csv", index=False)
    print(f"✅ Generated energy_sample.csv ({len(df)} rows)")
    return df


def generate_city_zones():
    """Generate city zone reference data"""
    print("Generating city zones data...")
    zones = [
        {
            "zone_id": "ZONE_01",
            "zone_name": "Downtown",
            "zone_type": "Commercial",
            "lat_min": 39.93,
            "lat_max": 39.96,
            "lon_min": -75.18,
            "lon_max": -75.15,
            "population": 45000,
        },
        {
            "zone_id": "ZONE_02",
            "zone_name": "University District",
            "zone_type": "Mixed-Use",
            "lat_min": 39.96,
            "lat_max": 39.99,
            "lon_min": -75.18,
            "lon_max": -75.15,
            "population": 32000,
        },
        {
            "zone_id": "ZONE_03",
            "zone_name": "Industrial Park",
            "zone_type": "Industrial",
            "lat_min": 39.93,
            "lat_max": 39.96,
            "lon_min": -75.21,
            "lon_max": -75.18,
            "population": 8000,
        },
        {
            "zone_id": "ZONE_04",
            "zone_name": "Residential North",
            "zone_type": "Residential",
            "lat_min": 39.96,
            "lat_max": 39.99,
            "lon_min": -75.21,
            "lon_max": -75.18,
            "population": 52000,
        },
        {
            "zone_id": "ZONE_05",
            "zone_name": "Residential South",
            "zone_type": "Residential",
            "lat_min": 39.90,
            "lat_max": 39.93,
            "lon_min": -75.18,
            "lon_max": -75.15,
            "population": 48000,
        },
    ]

    df = pd.DataFrame(zones)
    df.to_csv("data/samples/city_zones_sample.csv", index=False)
    print(f"✅ Generated city_zones_sample.csv ({len(df)} rows)")
    return df


if __name__ == "__main__":
    print("🚀 Generating sample datasets for SparkCity...")
    print("=" * 60)

    generate_traffic_sensors(500)
    generate_air_quality(500)
    generate_weather_data(500)
    generate_energy_meters(500)
    generate_city_zones()

    print("=" * 60)
    print("✅ All sample datasets generated successfully!")
    print()
    print("📁 Location: data/samples/")
    print()
    print("Files created:")
    print("  • traffic_sample.csv (500 rows)")
    print("  • air_quality_sample.json (500 rows)")
    print("  • weather_sample.parquet (500 rows)")
    print("  • energy_sample.csv (500 rows)")
    print("  • city_zones_sample.csv (5 zones)")
    print()
    print("🎉 Sample data ready for team development!")
