-- Time series analysis: Hourly traffic pattern by day of week
SELECT 
    dt.day_of_week,
    dt.hour,
    AVG(ft.vehicle_count) AS avg_vehicle_count,
    AVG(ft.avg_speed) AS avg_speed,
    AVG(ft.occupancy_rate) AS avg_occupancy
FROM 
    fact_traffic_measurements ft
JOIN 
    dim_time dt ON ft.time_key = dt.time_key
WHERE 
    dt.year = 2023 AND
    dt.month = 3
GROUP BY 
    dt.day_of_week, dt.hour
ORDER BY 
    dt.day_of_week, dt.hour; 