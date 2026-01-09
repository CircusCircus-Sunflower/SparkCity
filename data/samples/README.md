# Sample Datasets for Development

Small sample datasets (500 rows each) for team development and testing.

## Files

### traffic_sample.csv
Traffic sensor readings from 10 sensors over 1 week.

**Schema:**
- `sensor_id`: string - Unique sensor identifier
- `timestamp`: datetime - Reading timestamp
- `location_lat`: double - Latitude
- `location_lon`: double - Longitude
- `vehicle_count`: integer - Number of vehicles detected
- `avg_speed`: double - Average speed (mph)
- `congestion_level`: string - Low/Medium/High/Critical
- `road_type`: string - Highway/Main Street/Side Street/Avenue

### air_quality_sample.json
Air quality measurements from 8 monitors over 1 week.

**Schema:**
- `sensor_id`: string - Unique sensor identifier
- `timestamp`: datetime - Reading timestamp
- `location_lat`: double - Latitude
- `location_lon`: double - Longitude
- `pm25`: double - PM2.5 level (μg/m³)
- `pm10`: double - PM10 level (μg/m³)
- `no2`: double - Nitrogen dioxide (ppb)
- `co`: double - Carbon monoxide (ppm)
- `temperature`: double - Temperature (°F)
- `humidity`: double - Humidity (%)

### weather_sample.parquet
Weather station data from 5 stations over 1 week.

**Schema:**
- `station_id`: string - Unique station identifier
- `timestamp`: datetime - Reading timestamp
- `location_lat`: double - Latitude
- `location_lon`: double - Longitude
- `temperature`: double - Temperature (°F)
- `humidity`: double - Humidity (%)
- `wind_speed`: double - Wind speed (mph)
- `wind_direction`: string - Wind direction (N/NE/E/SE/S/SW/W/NW)
- `precipitation`: double - Precipitation (inches)
- `pressure`: double - Barometric pressure (inHg)

### energy_sample.csv
Energy meter readings from 12 buildings over 1 week.

**Schema:**
- `meter_id`: string - Unique meter identifier
- `timestamp`: datetime - Reading timestamp
- `building_type`: string - Residential/Commercial/Industrial/Mixed-Use
- `location_lat`: double - Latitude
- `location_lon`: double - Longitude
- `power_consumption`: double - Power usage (kWh)
- `voltage`: double - Voltage (V)
- `current`: double - Current (A)
- `power_factor`: double - Power factor (0-1)

### city_zones_sample.csv
Reference data for 5 city zones.

**Schema:**
- `zone_id`: string - Unique zone identifier
- `zone_name`: string - Zone name
- `zone_type`: string - Commercial/Residential/Industrial/Mixed-Use
- `lat_min`: double - Minimum latitude
- `lat_max`: double - Maximum latitude
- `lon_min`: double - Minimum longitude
- `lon_max`: double - Maximum longitude
- `population`: integer - Zone population

## Usage
```python
from src.utils.spark_session import get_spark_session

spark = get_spark_session("Development")

# Load samples
traffic = spark.read.csv("data/samples/traffic_sample.csv", header=True, inferSchema=True)
air_quality = spark.read.json("data/samples/air_quality_sample.json")
weather = spark.read.parquet("data/samples/weather_sample.parquet")
energy = spark.read.csv("data/samples/energy_sample.csv", header=True, inferSchema=True)
zones = spark.read.csv("data/samples/city_zones_sample.csv", header=True, inferSchema=True)
```

## Notes

- Data is randomly generated for development purposes
- Timestamps span past 7 days from generation time
- Location coordinates centered around Philadelphia (39.95, -75.16)
- Use these samples for testing before working with full datasets
