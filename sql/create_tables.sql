
DROP TABLE IF EXISTS fact_traffic CASCADE;
DROP TABLE IF EXISTS fact_air_quality CASCADE;
DROP TABLE IF EXISTS fact_energy CASCADE;
DROP TABLE IF EXISTS fact_weather CASCADE;
DROP TABLE IF EXISTS dim_sensors CASCADE;
DROP TABLE IF EXISTS dim_locations CASCADE;
DROP TABLE IF EXISTS dim_time CASCADE;
DROP TABLE IF EXISTS dim_zones CASCADE;

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

-- Dimension: Time
-- Provides temporal context for all facts
CREATE TABLE dim_time (
    time_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL UNIQUE,
    hour INT NOT NULL,
    day INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    quarter INT NOT NULL
);

CREATE INDEX idx_dim_time_timestamp ON dim_time(timestamp);
CREATE INDEX idx_dim_time_date ON dim_time(year, month, day);
CREATE INDEX idx_dim_time_hour ON dim_time(hour);

-- Dimension: Zones
-- City zones for geographic analysis
CREATE TABLE dim_zones (
    zone_id VARCHAR(20) PRIMARY KEY,
    zone_name VARCHAR(100) NOT NULL,
    zone_type VARCHAR(50) NOT NULL,
    lat_min DOUBLE PRECISION NOT NULL,
    lat_max DOUBLE PRECISION NOT NULL,
    lon_min DOUBLE PRECISION NOT NULL,
    lon_max DOUBLE PRECISION NOT NULL,
    population INT
);

-- Dimension: Sensors/Stations/Meters
-- All monitoring devices
CREATE TABLE dim_sensors (
    sensor_id VARCHAR(50) PRIMARY KEY,
    sensor_type VARCHAR(50) NOT NULL, -- 'traffic', 'air_quality', 'weather', 'energy'
    location_lat DOUBLE PRECISION NOT NULL,
    location_lon DOUBLE PRECISION NOT NULL,
    zone_id VARCHAR(20) REFERENCES dim_zones(zone_id),
    installation_date DATE,
    status VARCHAR(20) DEFAULT 'active'
);

CREATE INDEX idx_dim_sensors_type ON dim_sensors(sensor_type);
CREATE INDEX idx_dim_sensors_zone ON dim_sensors(zone_id);
CREATE INDEX idx_dim_sensors_location ON dim_sensors(location_lat, location_lon);

-- Dimension: Locations (denormalized for quick lookups)
CREATE TABLE dim_locations (
    location_id SERIAL PRIMARY KEY,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    zone_id VARCHAR(20) REFERENCES dim_zones(zone_id),
    address VARCHAR(200),
    UNIQUE(latitude, longitude)
);

CREATE INDEX idx_dim_locations_coords ON dim_locations(latitude, longitude);
CREATE INDEX idx_dim_locations_zone ON dim_locations(zone_id);

-- ============================================================
-- FACT TABLES
-- ============================================================

-- Fact: Traffic Sensor Readings
CREATE TABLE fact_traffic (
    reading_id BIGSERIAL PRIMARY KEY,
    sensor_id VARCHAR(50) NOT NULL REFERENCES dim_sensors(sensor_id),
    time_id INT NOT NULL REFERENCES dim_time(time_id),
    timestamp TIMESTAMP NOT NULL,
    vehicle_count INT NOT NULL,
    avg_speed DOUBLE PRECISION,
    congestion_level VARCHAR(20),
    road_type VARCHAR(50),
    is_anomaly BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_traffic_sensor ON fact_traffic(sensor_id);
CREATE INDEX idx_fact_traffic_time ON fact_traffic(time_id);
CREATE INDEX idx_fact_traffic_timestamp ON fact_traffic(timestamp);
CREATE INDEX idx_fact_traffic_congestion ON fact_traffic(congestion_level);

-- Fact: Air Quality Readings
CREATE TABLE fact_air_quality (
    reading_id BIGSERIAL PRIMARY KEY,
    sensor_id VARCHAR(50) NOT NULL REFERENCES dim_sensors(sensor_id),
    time_id INT NOT NULL REFERENCES dim_time(time_id),
    timestamp TIMESTAMP NOT NULL,
    pm25 DOUBLE PRECISION,
    pm10 DOUBLE PRECISION,
    no2 DOUBLE PRECISION,
    co DOUBLE PRECISION,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    aqi_category VARCHAR(20), -- 'Good', 'Moderate', 'Unhealthy', etc.
    is_anomaly BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_air_sensor ON fact_air_quality(sensor_id);
CREATE INDEX idx_fact_air_time ON fact_air_quality(time_id);
CREATE INDEX idx_fact_air_timestamp ON fact_air_quality(timestamp);
CREATE INDEX idx_fact_air_pm25 ON fact_air_quality(pm25);

-- Fact: Weather Readings
CREATE TABLE fact_weather (
    reading_id BIGSERIAL PRIMARY KEY,
    station_id VARCHAR(50) NOT NULL REFERENCES dim_sensors(sensor_id),
    time_id INT NOT NULL REFERENCES dim_time(time_id),
    timestamp TIMESTAMP NOT NULL,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    wind_speed DOUBLE PRECISION,
    wind_direction VARCHAR(10),
    precipitation DOUBLE PRECISION,
    pressure DOUBLE PRECISION,
    is_anomaly BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_weather_station ON fact_weather(station_id);
CREATE INDEX idx_fact_weather_time ON fact_weather(time_id);
CREATE INDEX idx_fact_weather_timestamp ON fact_weather(timestamp);
CREATE INDEX idx_fact_weather_temp ON fact_weather(temperature);

-- Fact: Energy Consumption
CREATE TABLE fact_energy (
    reading_id BIGSERIAL PRIMARY KEY,
    meter_id VARCHAR(50) NOT NULL REFERENCES dim_sensors(sensor_id),
    time_id INT NOT NULL REFERENCES dim_time(time_id),
    timestamp TIMESTAMP NOT NULL,
    building_type VARCHAR(50),
    power_consumption DOUBLE PRECISION,
    voltage DOUBLE PRECISION,
    current DOUBLE PRECISION,
    power_factor DOUBLE PRECISION,
    is_anomaly BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_energy_meter ON fact_energy(meter_id);
CREATE INDEX idx_fact_energy_time ON fact_energy(time_id);
CREATE INDEX idx_fact_energy_timestamp ON fact_energy(timestamp);
CREATE INDEX idx_fact_energy_building ON fact_energy(building_type);

-- ============================================================
-- ANALYTICAL VIEWS
-- ============================================================

-- View: Hourly Traffic Summary by Zone
CREATE OR REPLACE VIEW vw_hourly_traffic_by_zone AS
SELECT 
    z.zone_id,
    z.zone_name,
    t.hour,
    t.day_of_week,
    AVG(f.vehicle_count) as avg_vehicles,
    AVG(f.avg_speed) as avg_speed,
    COUNT(*) as reading_count,
    SUM(CASE WHEN f.congestion_level = 'High' OR f.congestion_level = 'Critical' THEN 1 ELSE 0 END) as high_congestion_count
FROM fact_traffic f
JOIN dim_sensors s ON f.sensor_id = s.sensor_id
JOIN dim_zones z ON s.zone_id = z.zone_id
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY z.zone_id, z.zone_name, t.hour, t.day_of_week;

-- View: Air Quality Summary by Zone
CREATE OR REPLACE VIEW vw_air_quality_by_zone AS
SELECT 
    z.zone_id,
    z.zone_name,
    DATE(f.timestamp) as date,
    AVG(f.pm25) as avg_pm25,
    AVG(f.pm10) as avg_pm10,
    MAX(f.pm25) as max_pm25,
    COUNT(*) as reading_count
FROM fact_air_quality f
JOIN dim_sensors s ON f.sensor_id = s.sensor_id
JOIN dim_zones z ON s.zone_id = z.zone_id
GROUP BY z.zone_id, z.zone_name, DATE(f.timestamp);

-- View: Energy Consumption by Building Type
CREATE OR REPLACE VIEW vw_energy_by_building_type AS
SELECT 
    f.building_type,
    t.hour,
    t.day_of_week,
    AVG(f.power_consumption) as avg_power,
    SUM(f.power_consumption) as total_power,
    COUNT(*) as reading_count
FROM fact_energy f
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY f.building_type, t.hour, t.day_of_week;

-- View: Recent Anomalies (last 24 hours)
CREATE OR REPLACE VIEW vw_recent_anomalies AS
SELECT 
    'traffic' as source,
    f.sensor_id,
    f.timestamp,
    f.vehicle_count as value,
    s.zone_id
FROM fact_traffic f
JOIN dim_sensors s ON f.sensor_id = s.sensor_id
WHERE f.is_anomaly = TRUE 
  AND f.timestamp > NOW() - INTERVAL '24 hours'
UNION ALL
SELECT 
    'air_quality' as source,
    f.sensor_id,
    f.timestamp,
    f.pm25 as value,
    s.zone_id
FROM fact_air_quality f
JOIN dim_sensors s ON f.sensor_id = s.sensor_id
WHERE f.is_anomaly = TRUE 
  AND f.timestamp > NOW() - INTERVAL '24 hours'
UNION ALL
SELECT 
    'energy' as source,
    f.meter_id as sensor_id,
    f.timestamp,
    f.power_consumption as value,
    s.zone_id
FROM fact_energy f
JOIN dim_sensors s ON f.meter_id = s.sensor_id
WHERE f.is_anomaly = TRUE 
  AND f.timestamp > NOW() - INTERVAL '24 hours';

-- ============================================================
-- HELPER FUNCTIONS
-- ============================================================

-- Function: Get or create time dimension entry
CREATE OR REPLACE FUNCTION get_time_id(ts TIMESTAMP)
RETURNS INT AS $$
DECLARE
    tid INT;
BEGIN
    SELECT time_id INTO tid FROM dim_time WHERE timestamp = ts;

    IF tid IS NULL THEN
        INSERT INTO dim_time (
            timestamp, hour, day, month, year, 
            day_of_week, day_name, is_weekend, quarter
        ) VALUES (
            ts,
            EXTRACT(HOUR FROM ts),
            EXTRACT(DAY FROM ts),
            EXTRACT(MONTH FROM ts),
            EXTRACT(YEAR FROM ts),
            EXTRACT(DOW FROM ts),
            TO_CHAR(ts, 'Day'),
            EXTRACT(DOW FROM ts) IN (0, 6),
            EXTRACT(QUARTER FROM ts)
        )
        RETURNING time_id INTO tid;
    END IF;

    RETURN tid;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- COMMENTS
-- ============================================================

COMMENT ON TABLE fact_traffic IS 'Traffic sensor readings with measures for vehicle count and speed';
COMMENT ON TABLE fact_air_quality IS 'Air quality monitor readings with pollutant levels';
COMMENT ON TABLE fact_weather IS 'Weather station readings with meteorological data';
COMMENT ON TABLE fact_energy IS 'Energy meter readings with power consumption data';

COMMENT ON TABLE dim_time IS 'Time dimension for temporal analysis';
COMMENT ON TABLE dim_sensors IS 'All monitoring devices (sensors, stations, meters)';
COMMENT ON TABLE dim_zones IS 'City zones for geographic analysis';
COMMENT ON TABLE dim_locations IS 'Location coordinates with zone mapping';

-- ============================================================
-- GRANTS (adjust as needed for your team)
-- ============================================================

-- Grant read access to analytical views
GRANT SELECT ON vw_hourly_traffic_by_zone TO PUBLIC;
GRANT SELECT ON vw_air_quality_by_zone TO PUBLIC;
GRANT SELECT ON vw_energy_by_building_type TO PUBLIC;
GRANT SELECT ON vw_recent_anomalies TO PUBLIC;
