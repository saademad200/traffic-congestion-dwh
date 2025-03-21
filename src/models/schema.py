from datetime import datetime
from dataclasses import dataclass
from typing import Optional, List, Dict, Any


@dataclass
class Location:
    intersection_id: str
    street_name: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    district: Optional[str] = None
    road_type: Optional[str] = None
    lanes: Optional[int] = None
    speed_limit: Optional[int] = None
    location_key: Optional[int] = None
    valid_from: datetime = datetime.now()
    valid_to: Optional[datetime] = None
    is_current: bool = True


@dataclass
class TimeRecord:
    timestamp: datetime
    hour: int
    day: int
    day_of_week: int
    month: int
    quarter: int
    year: int
    is_holiday: bool = False
    time_key: Optional[int] = None


@dataclass
class Weather:
    condition: Optional[str] = None
    temperature: Optional[float] = None
    precipitation: Optional[float] = None
    visibility: Optional[float] = None
    wind_speed: Optional[float] = None
    weather_key: Optional[int] = None
    valid_from: datetime = datetime.now()
    valid_to: Optional[datetime] = None
    is_current: bool = True


@dataclass
class TrafficMeasurement:
    vehicle_count: int
    avg_speed: float
    occupancy_rate: float
    queue_length: Optional[int] = None
    travel_time: Optional[float] = None
    delay_time: Optional[float] = None
    congestion_index: Optional[float] = None
    signal_cycle_time: Optional[int] = None
    incident_count: Optional[int] = None
    throughput_rate: Optional[float] = None
    measurement_timestamp: datetime = datetime.now()
    measurement_key: Optional[int] = None
    location_key: Optional[int] = None
    time_key: Optional[int] = None
    weather_key: Optional[int] = None
    event_key: Optional[int] = None
    vehicle_key: Optional[int] = None
    infrastructure_key: Optional[int] = None


@dataclass
class Event:
    event_type: str
    event_location: str
    start_time: datetime
    event_size: Optional[int] = None
    end_time: Optional[datetime] = None
    impact_radius: Optional[float] = None
    event_key: Optional[int] = None
    valid_from: datetime = datetime.now()
    valid_to: Optional[datetime] = None
    is_current: bool = True


@dataclass
class Vehicle:
    vehicle_type: str
    vehicle_class: str
    size_category: Optional[str] = None
    passenger_capacity: Optional[int] = None
    vehicle_key: Optional[int] = None
    valid_from: datetime = datetime.now()
    valid_to: Optional[datetime] = None
    is_current: bool = True


@dataclass
class Infrastructure:
    signal_type: str
    road_condition: Optional[str] = None
    construction_status: Optional[str] = None
    last_maintenance_date: Optional[datetime] = None
    capacity: Optional[int] = None
    special_features: Optional[List[str]] = None
    infrastructure_key: Optional[int] = None
    valid_from: datetime = datetime.now()
    valid_to: Optional[datetime] = None
    is_current: bool = True 