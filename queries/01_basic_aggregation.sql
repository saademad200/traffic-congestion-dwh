-- Basic aggregation: Daily traffic volume by district
SELECT 
    dl.district,
    dt.year,
    dt.month,
    dt.day,
    SUM(ft.vehicle_count) AS total_vehicles,
    AVG(ft.avg_speed) AS average_speed
FROM 
    fact_traffic_measurements ft
JOIN 
    dim_location dl ON ft.location_key = dl.location_key
JOIN 
    dim_time dt ON ft.time_key = dt.time_key
WHERE 
    dt.year = 2023
GROUP BY 
    dl.district, dt.year, dt.month, dt.day
ORDER BY 
    dl.district, dt.year, dt.month, dt.day; 