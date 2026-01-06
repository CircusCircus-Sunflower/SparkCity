- Query 1: Peak traffic hours by zone
SELECT 
    z.zone_name,
    t.hour,
    AVG(f.vehicle_count) as avg_vehicles,
    AVG(f.avg_speed) as avg_speed
FROM fact_traffic f
JOIN dim_sensors s ON f.sensor_id = s.sensor_id
JOIN dim_zones z ON s.zone_id = z.zone_id
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY z.zone_name, t.hour
ORDER BY z.zone_name, t.hour;

-- Query 2: Congestion hotspots
SELECT 
    s.sensor_id,
    z.zone_name,
    COUNT(*) as high_congestion_events,
    AVG(f.vehicle_count) as avg_vehicles_during_congestion
FROM fact_traffic f
JOIN dim_sensors s ON f.sensor_id = s.sensor_id
JOIN dim_zones z ON s.zone_id = z.zone_id
WHERE f.congestion_level IN ('High', 'Critical')
GROUP BY s.sensor_id, z.zone_name
ORDER BY high_congestion_events DESC
LIMIT 10;

-- Query 3: Weekday vs Weekend traffic patterns
SELECT 
    CASE WHEN t.is_weekend THEN 'Weekend' ELSE 'Weekday' END as period,
    t.hour,
    AVG(f.vehicle_count) as avg_vehicles,
    AVG(f.avg_speed) as avg_speed
FROM fact_traffic f
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY period, t.hour
ORDER BY period, t.hour;

-- ============================================================
-- AIR QUALITY ANALYSIS
-- ============================================================

-- Query 4: Air quality trends by zone
SELECT 
    z.zone_name,
    DATE(f.timestamp) as date,
    AVG(f.pm25) as avg_pm25,
    AVG(f.pm10) as avg_pm10,
    MAX(f.pm25) as max_pm25
FROM fact_air_quality f
JOIN dim_sensors s ON f.sensor_id = s.sensor_id
JOIN dim_zones z ON s.zone_id = z.zone_id
GROUP BY z.zone_name, DATE(f.timestamp)
ORDER BY z.zone_name, date;

-- Query 5: Worst air quality zones
SELECT 
    z.zone_id,
    z.zone_name,
    AVG(f.pm25) as avg_pm25,
    COUNT(*) as reading_count
FROM fact_air_quality f
JOIN dim_sensors s ON f.sensor_id = s.sensor_id
JOIN dim_zones z ON s.zone_id = z.zone_id
GROUP BY z.zone_id, z.zone_name
ORDER BY avg_pm25 DESC;

-- Query 6: Air quality correlation with traffic
SELECT 
    t.hour,
    AVG(traffic.vehicle_count) as avg_vehicles,
    AVG(air.pm25) as avg_pm25
FROM fact_traffic traffic
JOIN fact_air_quality air ON DATE_TRUNC('hour', traffic.timestamp) = DATE_TRUNC('hour', air.timestamp)
JOIN dim_time t ON traffic.time_id = t.time_id
GROUP BY t.hour
ORDER BY t.hour;

-- ============================================================
-- WEATHER ANALYSIS
-- ============================================================

-- Query 7: Weather impact on traffic speed
SELECT 
    CASE 
        WHEN w.precipitation > 0 THEN 'Rainy'
        ELSE 'Dry'
    END as weather_condition,
    AVG(t.avg_speed) as avg_traffic_speed,
    COUNT(*) as observation_count
FROM fact_traffic t
JOIN fact_weather w ON DATE_TRUNC('hour', t.timestamp) = DATE_TRUNC('hour', w.timestamp)
GROUP BY weather_condition;

-- Query 8: Temperature extremes
SELECT 
    DATE(timestamp) as date,
    MIN(temperature) as min_temp,
    MAX(temperature) as max_temp,
    AVG(temperature) as avg_temp
FROM fact_weather
GROUP BY DATE(timestamp)
ORDER BY date;

-- ============================================================
-- ENERGY CONSUMPTION ANALYSIS
-- ============================================================

-- Query 9: Energy consumption by building type and hour
SELECT 
    f.building_type,
    t.hour,
    AVG(f.power_consumption) as avg_power_kwh,
    SUM(f.power_consumption) as total_power_kwh
FROM fact_energy f
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY f.building_type, t.hour
ORDER BY f.building_type, t.hour;

-- Query 10: Peak energy demand hours
SELECT 
    t.hour,
    t.day_name,
    AVG(f.power_consumption) as avg_power,
    COUNT(*) as meter_count
FROM fact_energy f
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY t.hour, t.day_name
ORDER BY avg_power DESC
LIMIT 10;

-- Query 11: Energy consumption vs temperature
SELECT 
    ROUND(w.temperature / 5) * 5 as temp_range,
    AVG(e.power_consumption) as avg_energy_consumption
FROM fact_energy e
JOIN fact_weather w ON DATE_TRUNC('hour', e.timestamp) = DATE_TRUNC('hour', w.timestamp)
GROUP BY temp_range
ORDER BY temp_range;

-- ============================================================
-- CROSS-DOMAIN ANALYSIS
-- ============================================================

-- Query 12: City-wide daily summary
SELECT 
    DATE(t.timestamp) as date,
    COUNT(DISTINCT tr.sensor_id) as active_traffic_sensors,
    AVG(tr.vehicle_count) as avg_daily_traffic,
    COUNT(DISTINCT aq.sensor_id) as active_air_sensors,
    AVG(aq.pm25) as avg_daily_pm25,
    AVG(w.temperature) as avg_daily_temp,
    SUM(e.power_consumption) as total_daily_energy
FROM dim_time t
LEFT JOIN fact_traffic tr ON t.time_id = tr.time_id
LEFT JOIN fact_air_quality aq ON t.time_id = aq.time_id
LEFT JOIN fact_weather w ON t.time_id = w.time_id
LEFT JOIN fact_energy e ON t.time_id = e.time_id
GROUP BY DATE(t.timestamp)
ORDER BY date DESC;

-- Query 13: Zone performance scorecard
SELECT 
    z.zone_id,
    z.zone_name,
    z.zone_type,
    AVG(tr.vehicle_count) as avg_traffic,
    AVG(aq.pm25) as avg_air_quality_pm25,
    SUM(e.power_consumption) as total_energy_usage,
    COUNT(DISTINCT CASE WHEN tr.is_anomaly THEN tr.reading_id END) as traffic_anomalies,
    COUNT(DISTINCT CASE WHEN aq.is_anomaly THEN aq.reading_id END) as air_anomalies
FROM dim_zones z
LEFT JOIN dim_sensors s ON z.zone_id = s.zone_id
LEFT JOIN fact_traffic tr ON s.sensor_id = tr.sensor_id
LEFT JOIN fact_air_quality aq ON s.sensor_id = aq.sensor_id
LEFT JOIN fact_energy e ON s.sensor_id = e.meter_id
GROUP BY z.zone_id, z.zone_name, z.zone_type
ORDER BY z.zone_name;

-- ============================================================
-- ANOMALY DETECTION QUERIES
-- ============================================================

-- Query 14: Recent anomalies summary
SELECT 
    'traffic' as type,
    COUNT(*) as anomaly_count,
    MIN(timestamp) as first_anomaly,
    MAX(timestamp) as last_anomaly
FROM fact_traffic
WHERE is_anomaly = TRUE
  AND timestamp > NOW() - INTERVAL '24 hours'
UNION ALL
SELECT 
    'air_quality' as type,
    COUNT(*) as anomaly_count,
    MIN(timestamp) as first_anomaly,
    MAX(timestamp) as last_anomaly
FROM fact_air_quality
WHERE is_anomaly = TRUE
  AND timestamp > NOW() - INTERVAL '24 hours'
UNION ALL
SELECT 
    'energy' as type,
    COUNT(*) as anomaly_count,
    MIN(timestamp) as first_anomaly,
    MAX(timestamp) as last_anomaly
FROM fact_energy
WHERE is_anomaly = TRUE
  AND timestamp > NOW() - INTERVAL '24 hours';

-- Query 15: Sensors with most anomalies
SELECT 
    s.sensor_id,
    s.sensor_type,
    z.zone_name,
    COUNT(*) as anomaly_count
FROM (
    SELECT sensor_id, timestamp FROM fact_traffic WHERE is_anomaly = TRUE
    UNION ALL
    SELECT sensor_id, timestamp FROM fact_air_quality WHERE is_anomaly = TRUE
    UNION ALL
    SELECT meter_id as sensor_id, timestamp FROM fact_energy WHERE is_anomaly = TRUE
) anomalies
JOIN dim_sensors s ON anomalies.sensor_id = s.sensor_id
JOIN dim_zones z ON s.zone_id = z.zone_id
WHERE anomalies.timestamp > NOW() - INTERVAL '7 days'
GROUP BY s.sensor_id, s.sensor_type, z.zone_name
ORDER BY anomaly_count DESC
LIMIT 20;

-- ============================================================
-- DASHBOARD QUERIES
-- ============================================================

-- Query 16: Real-time city metrics (last hour)
SELECT 
    COUNT(DISTINCT tr.sensor_id) as active_sensors,
    AVG(tr.vehicle_count) as current_avg_traffic,
    AVG(aq.pm25) as current_avg_pm25,
    AVG(w.temperature) as current_temp,
    SUM(e.power_consumption) as current_power_usage
FROM fact_traffic tr
FULL OUTER JOIN fact_air_quality aq ON DATE_TRUNC('hour', tr.timestamp) = DATE_TRUNC('hour', aq.timestamp)
FULL OUTER JOIN fact_weather w ON DATE_TRUNC('hour', tr.timestamp) = DATE_TRUNC('hour', w.timestamp)
FULL OUTER JOIN fact_energy e ON DATE_TRUNC('hour', tr.timestamp) = DATE_TRUNC('hour', e.timestamp)
WHERE tr.timestamp > NOW() - INTERVAL '1 hour'
   OR aq.timestamp > NOW() - INTERVAL '1 hour'
   OR w.timestamp > NOW() - INTERVAL '1 hour'
   OR e.timestamp > NOW() - INTERVAL '1 hour';

-- Query 17: Top 5 congested zones right now
SELECT 
    z.zone_name,
    AVG(f.vehicle_count) as current_traffic,
    AVG(f.avg_speed) as current_speed,
    COUNT(*) as sensor_count
FROM fact_traffic f
JOIN dim_sensors s ON f.sensor_id = s.sensor_id
JOIN dim_zones z ON s.zone_id = z.zone_id
WHERE f.timestamp > NOW() - INTERVAL '1 hour'
GROUP BY z.zone_name
ORDER BY current_traffic DESC
LIMIT 5;