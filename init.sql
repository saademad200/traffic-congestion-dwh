-- Dimension Tables
CREATE TABLE dim_location (
    location_key SERIAL PRIMARY KEY,
    intersection_id VARCHAR(50) NOT NULL,
    street_name VARCHAR(100),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    district VARCHAR(50),
    road_type VARCHAR(50),
    lanes INTEGER,
    speed_limit INTEGER,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

CREATE TABLE dim_time (
    time_key SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    hour INTEGER,
    day INTEGER,
    day_of_week INTEGER,
    month INTEGER,
    quarter INTEGER,
    year INTEGER,
    is_holiday BOOLEAN
);

CREATE TABLE dim_weather (
    weather_key SERIAL PRIMARY KEY,
    condition VARCHAR(50),
    temperature DECIMAL(4,1),
    precipitation DECIMAL(4,2),
    visibility DECIMAL(5,2),
    wind_speed DECIMAL(4,1),
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

-- Add missing dimension tables
CREATE TABLE dim_event (
    event_key SERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    event_location VARCHAR(100),
    event_size INTEGER,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    impact_radius DECIMAL(5,2),
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

CREATE TABLE dim_vehicle (
    vehicle_key SERIAL PRIMARY KEY,
    vehicle_type VARCHAR(50) NOT NULL,
    vehicle_class VARCHAR(50) NOT NULL,
    size_category VARCHAR(20),
    passenger_capacity INTEGER,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

CREATE TABLE dim_infrastructure (
    infrastructure_key SERIAL PRIMARY KEY,
    signal_type VARCHAR(50) NOT NULL,
    road_condition VARCHAR(50),
    construction_status VARCHAR(50),
    last_maintenance_date TIMESTAMP,
    capacity INTEGER,
    special_features TEXT[],
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

-- Fact Table
CREATE TABLE fact_traffic_measurements (
    measurement_key BIGSERIAL PRIMARY KEY,
    location_key INTEGER REFERENCES dim_location,
    time_key INTEGER REFERENCES dim_time,
    weather_key INTEGER REFERENCES dim_weather,
    vehicle_count INTEGER,
    avg_speed DECIMAL(5,2),
    occupancy_rate DECIMAL(5,2),
    queue_length INTEGER,
    travel_time DECIMAL(6,2),
    measurement_timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    delay_time DECIMAL(6,2),
    congestion_index DECIMAL(5,2),
    signal_cycle_time INTEGER,
    incident_count INTEGER,
    throughput_rate DECIMAL(8,2),
    event_key INTEGER REFERENCES dim_event,
    vehicle_key INTEGER REFERENCES dim_vehicle,
    infrastructure_key INTEGER REFERENCES dim_infrastructure
) PARTITION BY RANGE (measurement_timestamp);

-- Create partitions by month
CREATE TABLE fact_traffic_measurements_y2024m01 PARTITION OF fact_traffic_measurements
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

-- Add indexes
CREATE INDEX idx_location_current ON dim_location(is_current);
CREATE INDEX idx_weather_current ON dim_weather(is_current);
CREATE INDEX idx_fact_timestamp ON fact_traffic_measurements(measurement_timestamp);

-- Ensure fact table has all KPIs
ALTER TABLE fact_traffic_measurements 
ADD COLUMN IF NOT EXISTS delay_time DECIMAL(6,2),
ADD COLUMN IF NOT EXISTS congestion_index DECIMAL(5,2),
ADD COLUMN IF NOT EXISTS signal_cycle_time INTEGER,
ADD COLUMN IF NOT EXISTS incident_count INTEGER,
ADD COLUMN IF NOT EXISTS throughput_rate DECIMAL(8,2),
ADD COLUMN IF NOT EXISTS event_key INTEGER REFERENCES dim_event,
ADD COLUMN IF NOT EXISTS vehicle_key INTEGER REFERENCES dim_vehicle,
ADD COLUMN IF NOT EXISTS infrastructure_key INTEGER REFERENCES dim_infrastructure;

-- Create missing dimension tables if they don't exist
CREATE TABLE IF NOT EXISTS dim_event (
    event_key SERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    event_location VARCHAR(100) NOT NULL,
    event_size INTEGER,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    impact_radius DECIMAL(5,2),
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dim_vehicle (
    vehicle_key SERIAL PRIMARY KEY,
    vehicle_type VARCHAR(50) NOT NULL,
    vehicle_class VARCHAR(50) NOT NULL,
    size_category VARCHAR(20),
    passenger_capacity INTEGER,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dim_infrastructure (
    infrastructure_key SERIAL PRIMARY KEY,
    signal_type VARCHAR(50) NOT NULL,
    road_condition VARCHAR(50),
    construction_status VARCHAR(50),
    last_maintenance_date TIMESTAMP,
    capacity INTEGER,
    special_features TEXT[],
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
); 