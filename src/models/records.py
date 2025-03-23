from pydantic import BaseModel, Field
from typing import Optional

class FactTrafficEventBase(BaseModel):
    """Base model for all traffic events"""
    event_id: int
    date_key: Optional[int] = Field(default=0, description="Date key for the event, defaults to 0 if not provided")
    time_key: Optional[int] = Field(default=0, description="Time key for the event, defaults to 0 if not provided")
    location_key: Optional[int] = Field(default=0, description="Location key for the event, defaults to 0 if not provided")
    vehicle_key: Optional[int] = Field(default=0, description="Vehicle key for the event, defaults to 0 if not provided")
    event_type_key: Optional[int] = Field(default=0, description="Event type key for the event, defaults to 0 if not provided")
    environmental_key: Optional[int] = Field(default=0, description="Environmental key for the event, defaults to 0 if not provided")

class TrafficFlowEvent(FactTrafficEventBase):
    """Model for traffic flow events"""
    vehicle_count: Optional[int] = Field(default=0, description="Number of vehicles involved in the traffic flow event, defaults to 0")
    avg_speed: Optional[float] = Field(default=0.0, description="Average speed of vehicles during the traffic flow event, defaults to 0.0")
    vehicles_involved: Optional[int] = Field(default=0, description="Total number of vehicles involved in the event, defaults to 0")
    incident_severity_score: Optional[float] = Field(default=0.0, description="Severity score of the incident, defaults to 0.0")
    speed_excess: Optional[float] = Field(default=0.0, description="Excess speed over the limit, defaults to 0.0")
    duration_minutes: Optional[int] = Field(default=0, description="Duration of the event in minutes, defaults to 0")
    congestion_level_score: Optional[float] = Field(default=0.0, description="Congestion level score for the event, defaults to 0.0")

class AccidentEvent(FactTrafficEventBase):
    """Model for accident events"""
    vehicle_count: Optional[int] = Field(default=0, description="Number of vehicles involved in the accident, defaults to 0")
    avg_speed: Optional[float] = Field(default=0.0, description="Average speed of vehicles during the accident, defaults to 0.0")
    vehicles_involved: Optional[int] = Field(default=0, description="Total number of vehicles involved in the accident, defaults to 0")
    incident_severity_score: Optional[float] = Field(default=0.0, description="Severity score of the accident, defaults to 0.0")
    speed_excess: Optional[float] = Field(default=0.0, description="Excess speed over the limit during the accident, defaults to 0.0")
    duration_minutes: Optional[int] = Field(default=0, description="Duration of the accident in minutes, defaults to 0")
    congestion_level_score: Optional[float] = Field(default=0.0, description="Congestion level score for the accident, defaults to 0.0")

class CongestionEvent(FactTrafficEventBase):
    """Model for congestion events"""
    vehicle_count: Optional[int] = Field(default=0, description="Number of vehicles involved in the congestion, defaults to 0")
    avg_speed: Optional[float] = Field(default=0.0, description="Average speed of vehicles during the congestion, defaults to 0.0")
    vehicles_involved: Optional[int] = Field(default=0, description="Total number of vehicles involved in the congestion, defaults to 0")
    incident_severity_score: Optional[float] = Field(default=0.0, description="Severity score of the congestion, defaults to 0.0")
    speed_excess: Optional[float] = Field(default=0.0, description="Excess speed over the limit during the congestion, defaults to 0.0")
    duration_minutes: Optional[int] = Field(default=0, description="Duration of the congestion in minutes, defaults to 0")
    congestion_level_score: Optional[float] = Field(default=0.0, description="Congestion level score for the congestion, defaults to 0.0")

class SpeedViolationEvent(FactTrafficEventBase):
    """Model for speed violation events"""
    vehicle_count: Optional[int] = Field(default=0, description="Number of vehicles involved in the speed violation, defaults to 0")
    avg_speed: Optional[float] = Field(default=0.0, description="Average speed of vehicles during the speed violation, defaults to 0.0")
    vehicles_involved: Optional[int] = Field(default=0, description="Total number of vehicles involved in the speed violation, defaults to 0")
    incident_severity_score: Optional[float] = Field(default=0.0, description="Severity score of the speed violation, defaults to 0.0")
    speed_excess: Optional[float] = Field(default=0.0, description="Excess speed over the limit during the violation, defaults to 0.0")
    duration_minutes: Optional[int] = Field(default=0, description="Duration of the speed violation in minutes, defaults to 0")
    congestion_level_score: Optional[float] = Field(default=0.0, description="Congestion level score for the speed violation, defaults to 0.0")

class RoadClosureEvent(FactTrafficEventBase):
    """Model for road closure events"""
    vehicle_count: Optional[int] = Field(default=0, description="Number of vehicles affected by the road closure, defaults to 0")
    avg_speed: Optional[float] = Field(default=0.0, description="Average speed of vehicles during the road closure, defaults to 0.0")
    vehicles_involved: Optional[int] = Field(default=0, description="Total number of vehicles affected by the road closure, defaults to 0")
    incident_severity_score: Optional[float] = Field(default=0.0, description="Severity score of the road closure, defaults to 0.0")
    speed_excess: Optional[float] = Field(default=0.0, description="Excess speed over the limit during the road closure, defaults to 0.0")
    duration_minutes: Optional[int] = Field(default=0, description="Duration of the road closure in minutes, defaults to 0")
    congestion_level_score: Optional[float] = Field(default=0.0, description="Congestion level score for the road closure, defaults to 0.0")