from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Union, List, Dict


@dataclass
class DimLocation:
    location_key: int  # Surrogate key
    location_source: str  # Source table name
    location_name: str  # Original location name


@dataclass
class DimDate:
    date_key: int  # Surrogate key
    date: datetime.date
    day: int
    day_of_week: int
    month: int
    quarter: int
    year: int
    is_weekend: bool
    is_holiday: bool
    season: str


@dataclass
class DimTime:
    time_key: int  # Surrogate key
    time_of_day: datetime.time
    hour: int
    minute: int
    peak_hour_flag: bool
    day_segment: str  # Morning, Afternoon, Evening, Night


@dataclass
class DimVehicle:
    vehicle_key: int  # Surrogate key
    vehicle_id: int  # Natural key
    vehicle_type: str
    vehicle_category: str  # Derived from vehicle_type


@dataclass
class DimEventType:
    event_type_key: int  # Surrogate key
    event_type_id: str  # Natural key
    event_category: str  # Flow, Accident, Violation, Congestion, Closure
    event_description: str
    severity_scale: int  # Standardized severity scale (1-10)


@dataclass
class DimEnvironmental:
    environmental_key: int  # Surrogate key
    temperature_c: Optional[float]
    weather_condition: Optional[str]


@dataclass
class FactTrafficEvents:
    event_id: int  # Surrogate key
    date_key: int  # FK to DIM_Date
    time_key: int  # FK to DIM_Time
    location_key: int  # FK to DIM_Location
    vehicle_key: Optional[int]  # FK to DIM_Vehicle, when applicable
    event_type_key: int  # FK to DIM_EventType
    environmental_key: Optional[int]  # FK to DIM_Environmental
    # Non-dimensional attributes
    vehicle_count: Optional[int]  # Fully Additive
    avg_speed: Optional[float]  # Non-Additive
    vehicles_involved: Optional[int]  # Fully Additive
    incident_severity_score: Optional[float]  # Non-Additive
    speed_excess: Optional[float]  # Fully Additive
    duration_minutes: Optional[int]  # Semi-Additive
    congestion_level_score: Optional[float]  # Non-Additive 