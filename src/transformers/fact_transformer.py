import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging

from .base_transformer import BaseTransformer

logger = logging.getLogger(__name__)

class FactTableTransformer(BaseTransformer):
    """Transformer for Fact_TrafficEvents"""
    
    def _get_date_key(self, date_df: pd.DataFrame, date_val) -> Optional[int]:
        """Get date key from date dimension"""
        if pd.isna(date_val):
            return None
            
        date_obj = pd.to_datetime(date_val).date()
        matching_date = date_df[date_df['date'] == date_obj]
        
        if not matching_date.empty:
            return matching_date.iloc[0]['date_key']
        return None
    
    def _get_time_key(self, time_df: pd.DataFrame, time_val) -> Optional[int]:
        """Get time key from time dimension"""
        if pd.isna(time_val):
            return None
            
        time_obj = pd.to_datetime(time_val).time()
        hour = time_obj.hour
        minute = time_obj.minute
        time_key = hour * 100 + minute
        
        matching_time = time_df[time_df['time_key'] == time_key]
        
        if not matching_time.empty:
            return matching_time.iloc[0]['time_key']
        return None
    
    def _get_location_key(self, location_df: pd.DataFrame, location_name: str, source: str) -> Optional[int]:
        """Get location key from location dimension"""
        if pd.isna(location_name):
            return None
            
        matching_loc = location_df[
            (location_df['location_name'] == location_name) & 
            (location_df['location_source'] == source)
        ]
        
        if not matching_loc.empty:
            return matching_loc.iloc[0]['location_key']
        return None
    
    def _get_vehicle_key(self, vehicle_df: pd.DataFrame, vehicle_id: int) -> Optional[int]:
        """Get vehicle key from vehicle dimension"""
        if pd.isna(vehicle_id):
            return None
            
        matching_vehicle = vehicle_df[vehicle_df['vehicle_id'] == vehicle_id]
        
        if not matching_vehicle.empty:
            return matching_vehicle.iloc[0]['vehicle_key']
        return None
    
    def _get_event_type_key(self, event_type_df: pd.DataFrame, event_type: str) -> Optional[int]:
        """Get event type key from event type dimension"""
        if pd.isna(event_type):
            return None
            
        matching_event = event_type_df[event_type_df['event_type_id'] == event_type]
        
        if not matching_event.empty:
            return matching_event.iloc[0]['event_type_key']
        return None
    
    def _get_environmental_key(self, env_df: pd.DataFrame, date_val) -> Optional[int]:
        """Get environmental key from environmental dimension"""
        if pd.isna(date_val):
            return None
            
        date_obj = pd.to_datetime(date_val).date()
        matching_env = env_df[env_df['date'] == date_obj]
        
        if not matching_env.empty:
            return matching_env.iloc[0]['environmental_key']
        return None
    
    def _map_congestion_level(self, level: str) -> float:
        """Map congestion level string to numeric score"""
        level_map = {
            'Low': 1.0,
            'Medium': 2.0,
            'High': 3.0,
            'Severe': 4.0
        }
        return level_map.get(level, 0.0)
    
    def _map_accident_severity(self, severity: str) -> float:
        """Map accident severity string to numeric score"""
        severity_map = {
            'Minor': 1.0,
            'Moderate': 2.0,
            'Severe': 3.0,
            'Fatal': 4.0
        }
        return severity_map.get(severity, 0.0)
    
    def transform(self, data: Dict[str, pd.DataFrame], 
                 dimensions: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Transform source data into fact table records
        """
        # Initialize empty list to store all fact records
        fact_records = []
        record_id = 1  # Starting ID for fact records
        
        # Get dimension dataframes
        date_df = dimensions.get('DimDate')
        time_df = dimensions.get('DimTime')
        location_df = dimensions.get('DimLocation')
        vehicle_df = dimensions.get('DimVehicle')
        event_type_df = dimensions.get('DimEventType')
        env_df = dimensions.get('DimEnvironmental')
        
        if not all([date_df is not None, time_df is not None, location_df is not None, 
                    event_type_df is not None]):
            logger.error("Missing required dimension tables")
            return pd.DataFrame()
        
        # Process TrafficFlow data
        if 'TrafficFlow' in data:
            df = data['TrafficFlow'].copy()
            for _, row in df.iterrows():
                record = {
                    'event_id': record_id,
                    'date_key': self._get_date_key(date_df, row['Timestamp']),
                    'time_key': self._get_time_key(time_df, row['Timestamp']),
                    'location_key': self._get_location_key(location_df, row['Location'], 'TrafficFlow'),
                    'vehicle_key': None,  # No vehicle associated
                    'event_type_key': self._get_event_type_key(event_type_df, 'FLOW'),
                    'environmental_key': self._get_environmental_key(env_df, row['Timestamp']),
                    'vehicle_count': row['VehicleCount'],
                    'avg_speed': None,
                    'vehicles_involved': None,
                    'incident_severity_score': None,
                    'speed_excess': None,
                    'duration_minutes': None,
                    'congestion_level_score': None
                }
                fact_records.append(record)
                record_id += 1
        
        # Process Accidents data
        if 'Accidents' in data:
            df = data['Accidents'].copy()
            for _, row in df.iterrows():
                record = {
                    'event_id': record_id,
                    'date_key': self._get_date_key(date_df, row['ReportedAt']),
                    'time_key': self._get_time_key(time_df, row['ReportedAt']),
                    'location_key': self._get_location_key(location_df, row['Location'], 'Accidents'),
                    'vehicle_key': None,  # No specific vehicle (could be multiple)
                    'event_type_key': self._get_event_type_key(
                        event_type_df, f'ACC_{row["Severity"].upper()}' if 'Severity' in row else 'ACC_MODERATE'
                    ),
                    'environmental_key': self._get_environmental_key(env_df, row['ReportedAt']),
                    'vehicle_count': None,
                    'avg_speed': None,
                    'vehicles_involved': row['VehiclesInvolved'],
                    'incident_severity_score': self._map_accident_severity(row['Severity']),
                    'speed_excess': None,
                    'duration_minutes': None,
                    'congestion_level_score': None
                }
                fact_records.append(record)
                record_id += 1
        
        # Process CongestionLevels data
        if 'CongestionLevels' in data:
            df = data['CongestionLevels'].copy()
            for _, row in df.iterrows():
                record = {
                    'event_id': record_id,
                    'date_key': self._get_date_key(date_df, row['RecordedAt']),
                    'time_key': self._get_time_key(time_df, row['RecordedAt']),
                    'location_key': self._get_location_key(location_df, row['Location'], 'CongestionLevels'),
                    'vehicle_key': None,
                    'event_type_key': self._get_event_type_key(
                        event_type_df, f'CONGESTION_{row["Level"].upper()}'
                    ),
                    'environmental_key': self._get_environmental_key(env_df, row['RecordedAt']),
                    'vehicle_count': None,
                    'avg_speed': None,
                    'vehicles_involved': None,
                    'incident_severity_score': None,
                    'speed_excess': None,
                    'duration_minutes': None,
                    'congestion_level_score': self._map_congestion_level(row['Level'])
                }
                fact_records.append(record)
                record_id += 1
        
        # Process SpeedViolations data
        if 'SpeedViolations' in data:
            df = data['SpeedViolations'].copy()
            for _, row in df.iterrows():
                # Check if 'Location' exists, otherwise use 'LocationID' or other alternative
                location = row.get('Location')
                if location is None and 'LocationID' in row:
                    location = row['LocationID']
                # Or use a default location if missing
                if location is None:
                    location = "Unknown"
                
                excess = row['SpeedRecorded'] - row['SpeedLimit'] if row['SpeedRecorded'] > row['SpeedLimit'] else 0
                record = {
                    'event_id': record_id,
                    'date_key': self._get_date_key(date_df, row['Timestamp']),
                    'time_key': self._get_time_key(time_df, row['Timestamp']),
                    'location_key': self._get_location_key(location_df, location, 'SpeedViolations'),
                    'vehicle_key': self._get_vehicle_key(vehicle_df, row['VehicleID']),
                    'event_type_key': self._get_event_type_key(event_type_df, 'SPEED_VIOLATION'),
                    'environmental_key': self._get_environmental_key(env_df, row['Timestamp']),
                    'vehicle_count': None,
                    'avg_speed': row['SpeedRecorded'],
                    'vehicles_involved': None,
                    'incident_severity_score': None,
                    'speed_excess': excess,
                    'duration_minutes': None,
                    'congestion_level_score': None
                }
                fact_records.append(record)
                record_id += 1
        
        # Process RoadClosures data
        if 'RoadClosures' in data:
            df = data['RoadClosures'].copy()
            for _, row in df.iterrows():
                # Assuming closures last 120 minutes on average if duration not available
                duration = 120  
                record = {
                    'event_id': record_id,
                    'date_key': self._get_date_key(date_df, row['ClosedAt']),
                    'time_key': self._get_time_key(time_df, row['ClosedAt']),
                    'location_key': self._get_location_key(location_df, row['Location'], 'RoadClosures'),
                    'vehicle_key': None,
                    'event_type_key': self._get_event_type_key(event_type_df, 'ROAD_CLOSURE'),
                    'environmental_key': self._get_environmental_key(env_df, row['ClosedAt']),
                    'vehicle_count': None,
                    'avg_speed': None,
                    'vehicles_involved': None,
                    'incident_severity_score': None,
                    'speed_excess': None,
                    'duration_minutes': duration,
                    'congestion_level_score': None
                }
                fact_records.append(record)
                record_id += 1
        
        # Create dataframe from all records
        fact_df = pd.DataFrame(fact_records)
        
        logger.info(f"Created Fact_TrafficEvents with {len(fact_df)} records")
        return fact_df 