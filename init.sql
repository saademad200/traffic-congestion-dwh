-- Dimension Tables
CREATE TABLE "DimLocation" (
    location_key SERIAL PRIMARY KEY,
    location_name VARCHAR(100) NOT NULL,
    location_source VARCHAR(50) NOT NULL  -- Source table name
);

CREATE TABLE "DimDate" (
    date_key INTEGER PRIMARY KEY, -- YYYYMMDD format
    date DATE NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    month INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN NOT NULL,
    season VARCHAR(10) NOT NULL
);

CREATE TABLE "DimTime" (
    time_key INTEGER PRIMARY KEY, -- HHMM format
    time_of_day TIME NOT NULL,
    hour INTEGER NOT NULL,
    minute INTEGER NOT NULL,
    peak_hour_flag BOOLEAN NOT NULL,
    day_segment VARCHAR(20) NOT NULL -- Morning, Afternoon, Evening, Night
);

CREATE TABLE "DimVehicle" (
    vehicle_key SERIAL PRIMARY KEY,
    vehicle_id INTEGER NOT NULL, -- Natural key
    vehicle_type VARCHAR(50) NOT NULL,
    vehicle_category VARCHAR(50) NOT NULL -- Derived from vehicle_type
);

CREATE TABLE "DimEventType" (
    event_type_key SERIAL PRIMARY KEY,
    event_type_id VARCHAR(20) NOT NULL, -- Natural key
    event_category VARCHAR(50) NOT NULL, -- Flow, Accident, Violation, Congestion, Closure
    event_description VARCHAR(100) NOT NULL,
    severity_scale INTEGER NOT NULL -- Standardized severity scale (1-10)
);

CREATE TABLE "DimEnvironmental" (
    environmental_key SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    temperature_c DECIMAL(5,2),
    weather_condition VARCHAR(50)
);

-- Fact Table
CREATE TABLE "FactTrafficEvents" (
    event_id SERIAL PRIMARY KEY,
    date_key INTEGER REFERENCES "DimDate"(date_key),
    time_key INTEGER REFERENCES "DimTime"(time_key),
    location_key INTEGER REFERENCES "DimLocation"(location_key),
    vehicle_key INTEGER REFERENCES "DimVehicle"(vehicle_key),
    event_type_key INTEGER REFERENCES "DimEventType"(event_type_key),
    environmental_key INTEGER REFERENCES "DimEnvironmental"(environmental_key),
    -- Measures
    vehicle_count INTEGER,
    avg_speed DECIMAL(5,2),
    vehicles_involved INTEGER,
    incident_severity_score DECIMAL(3,1),
    speed_excess DECIMAL(5,2),
    duration_minutes INTEGER,
    congestion_level_score DECIMAL(3,1)
);

-- Create indexes for better performance
CREATE INDEX idx_factevents_date ON "FactTrafficEvents"(date_key);
CREATE INDEX idx_factevents_location ON "FactTrafficEvents"(location_key);
CREATE INDEX idx_factevents_event_type ON "FactTrafficEvents"(event_type_key);
CREATE INDEX idx_dimlocation_source ON "DimLocation"(location_source);
CREATE INDEX idx_dimenv_date ON "DimEnvironmental"(date); 