-- Multidimensional comparison: Traffic by vehicle type, road type, and weather
SELECT 
    dv.vehicle_type,
    dl.road_type,
    dw.condition,
    AVG(ft.avg_speed) AS avg_speed,
    AVG(ft.occupancy_rate) AS avg_occupancy,
    COUNT(*) AS measurement_count
FROM 
    fact_traffic_measurements ft
JOIN 
    dim_vehicle dv ON ft.vehicle_key = dv.vehicle_key
JOIN 
    dim_location dl ON ft.location_key = dl.location_key
JOIN 
    dim_weather dw ON ft.weather_key = dw.weather_key
GROUP BY 
    CUBE(dv.vehicle_type, dl.road_type, dw.condition)
ORDER BY 
    dv.vehicle_type NULLS FIRST, 
    dl.road_type NULLS FIRST, 
    dw.condition NULLS FIRST; 