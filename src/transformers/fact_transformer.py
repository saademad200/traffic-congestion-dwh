import pandas as pd
from typing import Dict, List, Tuple, Callable, TypeVar
import logging

from .base_transformer import BaseTransformer
from src.models.records import (
    FactTrafficEventBase, TrafficFlowEvent, AccidentEvent, 
    CongestionEvent, SpeedViolationEvent, RoadClosureEvent
)

logger = logging.getLogger(__name__)

# Define a generic type for the event models
T = TypeVar('T', bound=FactTrafficEventBase)

class FactTableTransformer(BaseTransformer):
    """Transformer for Fact_TrafficEvents"""
    
    # Constants
    DEFAULT_KEY = 0
    DEFAULT_DURATION = 120  # minutes
    DEFAULT_LOCATION = "Unknown"
    
    def _get_dimension_key(self, df: pd.DataFrame, column_name: str, value, key_column: str) -> int:
        """Generic method to get dimension key from any dimension table"""
        if pd.isna(value):
            return self.DEFAULT_KEY
            
        matching_rows = df[df[column_name] == value]
        
        if not matching_rows.empty:
            return matching_rows.iloc[0][key_column]
        return self.DEFAULT_KEY
    
    def _get_date_key(self, date_df: pd.DataFrame, date_val) -> int:
        """Get date key from date dimension"""
        if pd.isna(date_val):
            return self.DEFAULT_KEY
            
        date_obj = pd.to_datetime(date_val).date()
        return self._get_dimension_key(date_df, 'date', date_obj, 'date_key')
    
    def _get_time_key(self, time_df: pd.DataFrame, time_val) -> int:
        """Get time key from time dimension"""
        if pd.isna(time_val):
            return self.DEFAULT_KEY
            
        time_obj = pd.to_datetime(time_val).time()
        time_key = time_obj.hour * 100 + time_obj.minute
        
        matching_time = time_df[time_df['time_key'] == time_key]
        
        if not matching_time.empty:
            return matching_time.iloc[0]['time_key']
        return self.DEFAULT_KEY
    
    def _get_location_key(self, location_df: pd.DataFrame, location_name: str, source: str) -> int:
        """Get location key from location dimension"""
        if pd.isna(location_name):
            return self.DEFAULT_KEY
            
        matching_loc = location_df[
            (location_df['location_name'] == location_name) & 
            (location_df['location_source'] == source)
        ]
        
        if not matching_loc.empty:
            return matching_loc.iloc[0]['location_key']
        return self.DEFAULT_KEY
    
    def _get_vehicle_key(self, vehicle_df: pd.DataFrame, vehicle_id: int) -> int:
        """Get vehicle key from vehicle dimension"""
        return self._get_dimension_key(vehicle_df, 'vehicle_id', vehicle_id, 'vehicle_key')
    
    def _get_event_type_key(self, event_type_df: pd.DataFrame, event_type: str) -> int:
        """Get event type key from event type dimension"""
        return self._get_dimension_key(event_type_df, 'event_type_id', event_type, 'event_type_key')
    
    def _get_environmental_key(self, env_df: pd.DataFrame, date_val) -> int:
        """Get environmental key from environmental dimension"""
        if pd.isna(date_val):
            return self.DEFAULT_KEY
            
        date_obj = pd.to_datetime(date_val).date()
        return self._get_dimension_key(env_df, 'date', date_obj, 'environmental_key')
    
    # Create a mapping dictionary for severity levels
    _CONGESTION_LEVEL_MAP = {
            'Low': 1.0,
            'Moderate': 2.0,
            'High': 3.0,
            'Severe': 4.0
        }
    
    def _map_congestion_level(self, level: str) -> float:
        """Map congestion level string to numeric score"""
        return self._CONGESTION_LEVEL_MAP.get(level)
    
    _ACCIDENT_SEVERITY_MAP = {
            'Minor': 1.0,
            'Moderate': 2.0,
            'Severe': 3.0,
            'Fatal': 4.0
        }
    
    def _map_accident_severity(self, severity: str) -> float:
        """Map accident severity string to numeric score"""
        return self._ACCIDENT_SEVERITY_MAP.get(severity)
    
    def _process_data_source(self, 
                           source_name: str,
                           data: Dict[str, pd.DataFrame],
                           dimensions: Dict[str, pd.DataFrame],
                           record_id: int,
                           record_factory: Callable[[pd.Series, int, Dict[str, pd.DataFrame]], T]) -> Tuple[List[T], int]:
        """Process a data source and return a list of records and the updated record ID"""
        records = []
        skipped = 0
        if source_name in data:
            df = data[source_name].copy()
            
            # Pre-process speed violations to handle data quality issues
            if source_name == 'SpeedViolations':
                # Count records before filtering
                original_count = len(df)
                # Filter out invalid speed violations (recorded speed < speed limit)
                df = df[df['SpeedRecorded'] > df['SpeedLimit']]
                filtered_count = original_count - len(df)
                if filtered_count > 0:
                    logger.warning(f"Filtered out {filtered_count} invalid speed violations where recorded speed <= speed limit")
                    skipped = filtered_count
            
            for _, row in df.iterrows():
                try:
                    record = record_factory(row, record_id, dimensions)
                    records.append(record)
                    record_id += 1
                except Exception as e:
                    logger.error(f"Error processing {source_name} record: {str(e)}")
                    skipped += 1
                    continue
                    
        if skipped > 0:
            logger.info(f"Skipped {skipped} records from {source_name} due to data quality issues or errors")
            
        return records, record_id
       
    def _create_traffic_flow_record(self, row: pd.Series, record_id: int, 
                                  dimensions: Dict[str, pd.DataFrame]) -> TrafficFlowEvent:
        """Create a TrafficFlowEvent from row data"""
        date_df = dimensions.get('DimDate')
        time_df = dimensions.get('DimTime')
        location_df = dimensions.get('DimLocation')
        event_type_df = dimensions.get('DimEventType')
        env_df = dimensions.get('DimEnvironmental')
        
        return TrafficFlowEvent(
            event_id=record_id,
            date_key=self._get_date_key(date_df, row['Timestamp']),
            time_key=self._get_time_key(time_df, row['Timestamp']),
            location_key=self._get_location_key(location_df, row['Location'], 'TrafficFlow'),
            vehicle_key=self.DEFAULT_KEY,
            event_type_key=self._get_event_type_key(event_type_df, 'FLOW'),
            environmental_key=self._get_environmental_key(env_df, row['Timestamp']),
            vehicle_count=row['VehicleCount']
        )
    
    def _create_accident_record(self, row: pd.Series, record_id: int, 
                              dimensions: Dict[str, pd.DataFrame]) -> AccidentEvent:
        """Create an AccidentEvent from row data"""
        date_df = dimensions.get('DimDate')
        time_df = dimensions.get('DimTime')
        location_df = dimensions.get('DimLocation')
        event_type_df = dimensions.get('DimEventType')
        env_df = dimensions.get('DimEnvironmental')
        
        return AccidentEvent(
            event_id=record_id,
            date_key=self._get_date_key(date_df, row['ReportedAt']),
            time_key=self._get_time_key(time_df, row['ReportedAt']),
            location_key=self._get_location_key(location_df, row['Location'], 'Accidents'),
            vehicle_key=self.DEFAULT_KEY,
            event_type_key=self._get_event_type_key(
                event_type_df, f'ACC_{row["Severity"].upper()}' if 'Severity' in row else 'ACC_MODERATE'
            ),
            environmental_key=self._get_environmental_key(env_df, row['ReportedAt']),
            vehicles_involved=row['VehiclesInvolved'],
            incident_severity_score=self._map_accident_severity(row['Severity'])
        )
    
    def _create_congestion_record(self, row: pd.Series, record_id: int, 
                                dimensions: Dict[str, pd.DataFrame]) -> CongestionEvent:
        """Create a CongestionEvent from row data"""
        date_df = dimensions.get('DimDate')
        time_df = dimensions.get('DimTime')
        location_df = dimensions.get('DimLocation')
        event_type_df = dimensions.get('DimEventType')
        env_df = dimensions.get('DimEnvironmental')
        
        return CongestionEvent(
            event_id=record_id,
            date_key=self._get_date_key(date_df, row['RecordedAt']),
            time_key=self._get_time_key(time_df, row['RecordedAt']),
            location_key=self._get_location_key(location_df, row['Location'], 'CongestionLevels'),
            vehicle_key=self.DEFAULT_KEY,
            event_type_key=self._get_event_type_key(
                event_type_df, f'CONGESTION_{row["Level"].upper()}'
            ),
            environmental_key=self._get_environmental_key(env_df, row['RecordedAt']),
            congestion_level_score=self._map_congestion_level(row['Level'])
        )
    
    def _create_speed_violation_record(self, row: pd.Series, record_id: int, 
                                     dimensions: Dict[str, pd.DataFrame]) -> SpeedViolationEvent:
        """Create a SpeedViolationEvent from row data"""
        date_df = dimensions.get('DimDate')
        time_df = dimensions.get('DimTime')
        location_df = dimensions.get('DimLocation')
        vehicle_df = dimensions.get('DimVehicle')
        event_type_df = dimensions.get('DimEventType')
        env_df = dimensions.get('DimEnvironmental')
        
        # Check if 'Location' exists, otherwise use 'LocationID' or other alternative
        location = row.get('Location')
        if location is None and 'LocationID' in row:
            location = row['LocationID']
        # Or use a default location if missing
        if location is None:
            location = self.DEFAULT_LOCATION
                
        # Calculate speed excess - we now know SpeedRecorded > SpeedLimit because of our filter
        excess = row['SpeedRecorded'] - row['SpeedLimit']
        
        return SpeedViolationEvent(
            event_id=record_id,
            date_key=self._get_date_key(date_df, row['Timestamp']),
            time_key=self._get_time_key(time_df, row['Timestamp']),
            location_key=self._get_location_key(location_df, location, 'SpeedViolations'),
            vehicle_key=self._get_vehicle_key(vehicle_df, row['VehicleID']),
            event_type_key=self._get_event_type_key(event_type_df, 'SPEED_VIOLATION'),
            environmental_key=self._get_environmental_key(env_df, row['Timestamp']),
            avg_speed=row['SpeedRecorded'],
            speed_excess=excess
        )
    
    def _create_road_closure_record(self, row: pd.Series, record_id: int, 
                                  dimensions: Dict[str, pd.DataFrame]) -> RoadClosureEvent:
        """Create a RoadClosureEvent from row data"""
        date_df = dimensions.get('DimDate')
        time_df = dimensions.get('DimTime')
        location_df = dimensions.get('DimLocation')
        event_type_df = dimensions.get('DimEventType')
        env_df = dimensions.get('DimEnvironmental')
        
        return RoadClosureEvent(
            event_id=record_id,
            date_key=self._get_date_key(date_df, row['ClosedAt']),
            time_key=self._get_time_key(time_df, row['ClosedAt']),
            location_key=self._get_location_key(location_df, row['Location'], 'RoadClosures'),
            vehicle_key=self.DEFAULT_KEY,
            event_type_key=self._get_event_type_key(event_type_df, 'ROAD_CLOSURE'),
            environmental_key=self._get_environmental_key(env_df, row['ClosedAt']),
            duration_minutes=self.DEFAULT_DURATION
        )
    
    def transform(self, data: Dict[str, pd.DataFrame], 
                 dimensions: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Transform source data into fact table records
        """
        # Initialize empty list to store all fact records
        fact_records: List[FactTrafficEventBase] = []
        record_id = 1  # Starting ID for fact records
        
        # Data quality counters
        total_source_records = 0
        for source_name in data:
            if source_name in data and data[source_name] is not None:
                total_source_records += len(data[source_name])
        
        # Validate required dimensions
        required_dimensions = ['DimDate', 'DimTime', 'DimLocation', 'DimEventType']
        for dim in required_dimensions:
            if dim not in dimensions or dimensions[dim] is None:
                logger.error(f"Missing required dimension table: {dim}")
                return pd.DataFrame()
        
        # Process each data source
        data_sources = [
            ('TrafficFlow', self._create_traffic_flow_record),
            ('Accidents', self._create_accident_record),
            ('CongestionLevels', self._create_congestion_record),
            ('SpeedViolations', self._create_speed_violation_record),
            ('RoadClosures', self._create_road_closure_record)
        ]
        
        # Process each data source
        for source_name, factory_func in data_sources:
            records, record_id = self._process_data_source(
                source_name, data, dimensions, record_id, factory_func
            )
            fact_records.extend(records)
        
        # Create dataframe from records - convert Pydantic models to dictionaries
        if not fact_records:
            logger.warning("No fact records created")
            return pd.DataFrame()
            
        fact_df = pd.DataFrame([record.model_dump() for record in fact_records])
        
        # Log data quality summary
        records_processed = len(fact_df)
        if records_processed < total_source_records:
            logger.info(f"Data quality summary: Processed {records_processed} of {total_source_records} source records")
            logger.info(f"Filtered out {total_source_records - records_processed} records due to data quality issues")
        
        logger.info(f"Created Fact_TrafficEvents with {len(fact_df)} records")
        return fact_df 