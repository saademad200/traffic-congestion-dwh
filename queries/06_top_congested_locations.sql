-- Top congested locations: Ranking with window functions
SELECT 
    dl.district,
    dl.intersection_id,
    dl.street_name,
    AVG(ft.occupancy_rate) AS avg_occupancy,
    AVG(ft.avg_speed) AS avg_speed,
    AVG(ft.congestion_index) AS avg_congestion,
    RANK() OVER (PARTITION BY dl.district ORDER BY AVG(ft.congestion_index) DESC) as congestion_rank
FROM 
    fact_traffic_measurements ft
JOIN 
    dim_location dl ON ft.location_key = dl.location_key
JOIN 
    dim_time dt ON ft.time_key = dt.time_key
WHERE 
    dt.year = 2023 AND
    dt.month = 3 AND
    dt.hour BETWEEN 7 AND 19  -- Daytime hours only
GROUP BY 
    dl.district, dl.intersection_id, dl.street_name
HAVING 
    COUNT(*) > 100  -- Minimum number of measurements
ORDER BY 
    dl.district, congestion_rank; 