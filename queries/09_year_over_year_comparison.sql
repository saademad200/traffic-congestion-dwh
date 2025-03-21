-- Year over year comparison: Monthly traffic volume trends
WITH monthly_volumes AS (
    SELECT 
        dt.year,
        dt.month,
        SUM(ft.vehicle_count) AS monthly_volume
    FROM 
        fact_traffic_measurements ft
    JOIN 
        dim_time dt ON ft.time_key = dt.time_key
    WHERE 
        dt.year IN (2022, 2023)
    GROUP BY 
        dt.year, dt.month
)
SELECT 
    mv_current.month,
    mv_current.monthly_volume AS volume_2023,
    mv_prev.monthly_volume AS volume_2022,
    (mv_current.monthly_volume - mv_prev.monthly_volume) / mv_prev.monthly_volume * 100 AS pct_change
FROM 
    monthly_volumes mv_current
JOIN 
    monthly_volumes mv_prev 
    ON mv_current.month = mv_prev.month AND mv_current.year = 2023 AND mv_prev.year = 2022
ORDER BY 
    mv_current.month; 