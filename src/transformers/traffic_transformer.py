import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Any, List, Tuple

from src.transformers.base_transformer import BaseTransformer
from src.models.schema import TrafficMeasurement, TimeRecord


class TrafficTransformer(BaseTransformer):
    """Transformer for traffic data"""
    
    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform traffic data"""
        try:
            traffic_data = data.get("traffic_data", [])
            if not traffic_data:
                self.logger.warning("No traffic data to transform")
                return {"traffic_measurements": [], "time_records": []}
            
            # Convert to DataFrame if it's a list of dictionaries
            if isinstance(traffic_data, list):
                df = pd.DataFrame(traffic_data)
            else:
                df = traffic_data
                
            # Clean data
            df_cleaned = self._clean_data(df)
            
            # Create traffic measurements
            traffic_measurements = self._create_traffic_measurements(df_cleaned)
            
            # Create time dimension records
            time_records = self._create_time_records(df_cleaned)
            
            return {
                "traffic_measurements": traffic_measurements,
                "time_records": time_records
            }
            
        except Exception as e:
            self.logger.error(f"Error transforming traffic data: {str(e)}")
            raise
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean traffic data"""
        # Convert measurement_timestamp to datetime if it's not
        if 'measurement_timestamp' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['measurement_timestamp']):
            df['measurement_timestamp'] = pd.to_datetime(df['measurement_timestamp'])
        
        # Fill missing values
        if 'vehicle_count' in df.columns:
            df['vehicle_count'] = df['vehicle_count'].fillna(0).astype(int)
        
        if 'avg_speed' in df.columns:
            df['avg_speed'] = df['avg_speed'].fillna(0)
            
        if 'occupancy_rate' in df.columns:
            df['occupancy_rate'] = df['occupancy_rate'].fillna(0)
            # Ensure occupancy rate is between 0 and 1
            df['occupancy_rate'] = df['occupancy_rate'].clip(0, 1)
        
        # Drop duplicates
        df = df.drop_duplicates(subset=['measurement_timestamp'], keep='first')
        
        return df
    
    def _create_traffic_measurements(self, df: pd.DataFrame) -> List[TrafficMeasurement]:
        """Create traffic measurement objects from DataFrame"""
        measurements = []
        
        for _, row in df.iterrows():
            measurement = TrafficMeasurement(
                vehicle_count=int(row['vehicle_count']),
                avg_speed=float(row['avg_speed']),
                occupancy_rate=float(row['occupancy_rate']),
                queue_length=int(row.get('queue_length', 0)) if pd.notna(row.get('queue_length', np.nan)) else None,
                travel_time=float(row.get('travel_time', 0)) if pd.notna(row.get('travel_time', np.nan)) else None,
                measurement_timestamp=row['measurement_timestamp']
            )
            measurements.append(measurement)
        
        return measurements
    
    def _create_time_records(self, df: pd.DataFrame) -> List[TimeRecord]:
        """Create time dimension records from DataFrame"""
        unique_timestamps = df['measurement_timestamp'].unique()
        time_records = []
        
        for ts in unique_timestamps:
            dt = pd.to_datetime(ts)
            time_record = TimeRecord(
                timestamp=dt,
                hour=dt.hour,
                day=dt.day,
                day_of_week=dt.dayofweek,
                month=dt.month,
                quarter=dt.quarter,
                year=dt.year,
                is_holiday=self._is_holiday(dt)
            )
            time_records.append(time_record)
        
        return time_records
    
    def _is_holiday(self, dt: datetime) -> bool:
        """Check if date is a holiday (simplified)"""
        # For a real implementation, use a holiday calendar library
        # This is a simplified example
        if dt.month == 1 and dt.day == 1:  # New Year's Day
            return True
        elif dt.month == 12 and dt.day == 25:  # Christmas
            return True
        elif dt.month == 7 and dt.day == 4:  # Independence Day (US)
            return True
        return False 