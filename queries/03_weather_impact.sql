-- Weather impact analysis: Traffic metrics by weather condition
SELECT 
    dw.condition,
    AVG(ft.vehicle_count) AS avg_vehicle_count,
    AVG(ft.avg_speed) AS avg_speed,
    AVG(ft.occupancy_rate) AS avg_occupancy,
    COUNT(*) AS measurement_count
FROM 
    fact_traffic_measurements ft
JOIN 
    dim_weather dw ON ft.weather_key = dw.weather_key
GROUP BY 
    dw.condition
ORDER BY 
    avg_speed ASC; 