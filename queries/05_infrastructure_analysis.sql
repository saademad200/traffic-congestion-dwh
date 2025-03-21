-- Infrastructure analysis: Traffic performance by signal type and road condition
SELECT 
    di.signal_type,
    di.road_condition,
    AVG(ft.avg_speed) AS avg_speed,
    AVG(ft.travel_time) AS avg_travel_time,
    AVG(ft.vehicle_count) AS avg_vehicle_count,
    COUNT(*) AS measurement_count
FROM 
    fact_traffic_measurements ft
JOIN 
    dim_infrastructure di ON ft.infrastructure_key = di.infrastructure_key
GROUP BY 
    di.signal_type, di.road_condition
ORDER BY 
    di.signal_type, di.road_condition; 