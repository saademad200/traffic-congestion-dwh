-- Event impact analysis: Traffic before, during, and after events
WITH event_periods AS (
    SELECT
        ft.measurement_key,
        CASE
            WHEN ft.measurement_timestamp BETWEEN de.start_time - INTERVAL '1 hour' AND de.start_time THEN 'Before Event'
            WHEN ft.measurement_timestamp BETWEEN de.start_time AND de.end_time THEN 'During Event'
            WHEN ft.measurement_timestamp BETWEEN de.end_time AND de.end_time + INTERVAL '1 hour' THEN 'After Event'
            ELSE 'No Event'
        END AS event_period
    FROM 
        fact_traffic_measurements ft
    LEFT JOIN
        dim_event de ON ft.event_key = de.event_key
)
SELECT 
    ep.event_period,
    AVG(ft.vehicle_count) AS avg_vehicle_count,
    AVG(ft.avg_speed) AS avg_speed,
    AVG(ft.occupancy_rate) AS avg_occupancy
FROM 
    fact_traffic_measurements ft
JOIN 
    event_periods ep ON ft.measurement_key = ep.measurement_key
WHERE 
    ep.event_period != 'No Event'
GROUP BY 
    ep.event_period
ORDER BY 
    CASE 
        WHEN ep.event_period = 'Before Event' THEN 1
        WHEN ep.event_period = 'During Event' THEN 2
        WHEN ep.event_period = 'After Event' THEN 3
        ELSE 4
    END; 