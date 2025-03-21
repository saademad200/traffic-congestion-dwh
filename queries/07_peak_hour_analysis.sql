-- Peak hour analysis: Rolling averages with window functions
SELECT 
    dt.date,
    dt.hour,
    AVG(ft.vehicle_count) AS hourly_volume,
    AVG(AVG(ft.vehicle_count)) OVER (
        ORDER BY dt.date, dt.hour 
        ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
    ) AS rolling_avg_volume,
    CASE 
        WHEN AVG(ft.vehicle_count) > 
            1.25 * AVG(AVG(ft.vehicle_count)) OVER (PARTITION BY dt.date) 
        THEN 'Peak Hour'
        ELSE 'Normal'
    END AS hour_classification
FROM 
    fact_traffic_measurements ft
JOIN 
    dim_time dt ON ft.time_key = dt.time_key
WHERE 
    dt.year = 2023 AND
    dt.month = 3
GROUP BY 
    dt.date, dt.hour
ORDER BY 
    dt.date, dt.hour; 