# Data Ingestion Modules

Production-ready data loaders for all SparkCity datasets.

## Quick Start

### Load All Datasets at Once
```python
from src.utils.spark_session import get_spark_session
from src.data_ingestion.load_all import load_all_datasets

# Create Spark session
spark = get_spark_session("DataIngestion")

# Load everything
datasets = load_all_datasets(spark)

# Access individual datasets
traffic_df = datasets['traffic']
air_quality_df = datasets['air_quality']
weather_df = datasets['weather']
energy_df = datasets['energy']
zones_df = datasets['city_zones']
```

### Load Individual Datasets
```python
from src.data_ingestion.traffic_sensors import load_traffic_sensors
from src.data_ingestion.air_quality import load_air_quality
from src.data_ingestion.weather_stations import load_weather_data
from src.data_ingestion.energy_meters import load_energy_meters
from src.data_ingestion.city_zones import load_city_zones

spark = get_spark_session()

# Load specific dataset
traffic = load_traffic_sensors(spark, "data/samples/traffic_sample.csv")
```

### Load by Name
```python
from src.data_ingestion.load_all import load_dataset_by_name

spark = get_spark_session()

traffic = load_dataset_by_name(spark, 'traffic', 'data/samples/traffic_sample.csv')
```

## Features

✅ **Schema Enforcement** - Explicit schemas for all datasets
✅ **Data Validation** - Built-in quality checks
✅ **Error Handling** - Graceful error messages
✅ **Logging** - Detailed logging for debugging
✅ **Type Safety** - Proper data type handling

## Validation Checks

Each loader performs validation:

**Traffic:**
- Checks for null values in required fields
- Validates vehicle counts are non-negative

**Air Quality:**
- Checks for negative pollutant values
- Flags extreme PM2.5 readings (>500)

**Weather:**
- Validates temperature ranges
- Checks humidity is 0-100%

**Energy:**
- Validates power consumption is non-negative
- Checks voltage ranges
- Validates power factor is 0-1

## Disable Validation
```python
# Skip validation for faster loading during development
datasets = load_all_datasets(spark, validate=False)
```

## File Formats Supported

- **CSV** - Traffic sensors, energy meters, city zones
- **JSON** - Air quality monitors
- **Parquet** - Weather stations

## Error Handling

All loaders raise exceptions with clear error messages:
```python
try:
    traffic = load_traffic_sensors(spark, "missing_file.csv")
except Exception as e:
    print(f"Load failed: {e}")
```