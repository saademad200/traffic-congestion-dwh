from typing import Dict, Any, List
import pandas as pd
from src.transformers.base_transformer import BaseTransformer
from src.models.schema import Event
from datetime import datetime

class EventTransformer(BaseTransformer):
    """Transformer for event data"""
    
    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform event data"""
        try:
            event_data = data.get("event_data", [])
            if not event_data:
                self.logger.warning("No event data to transform")
                return {"event_records": []}
            
            # Convert to DataFrame if it's a list of dictionaries
            if isinstance(event_data, list):
                df = pd.DataFrame(event_data)
            else:
                df = event_data
            
            # Clean and standardize event data
            df_cleaned = self._clean_data(df)
            
            # Create event dimension records
            event_records = self._create_event_records(df_cleaned)
            
            return {
                "event_records": event_records
            }
            
        except Exception as e:
            self.logger.error(f"Error transforming event data: {str(e)}")
            raise
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean event data"""
        # Required columns
        required_cols = ['event_type', 'event_location', 'start_time']
        for col in required_cols:
            if col not in df.columns:
                self.logger.warning(f"Required column '{col}' not found in event data")
                df[col] = "Unknown" if col != 'start_time' else datetime.now()
        
        # Convert timestamps to datetime objects
        timestamp_cols = ['start_time', 'end_time']
        for col in timestamp_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Fill end_time with start_time + 1 hour if missing
        if 'end_time' in df.columns and df['end_time'].isna().any():
            df.loc[df['end_time'].isna(), 'end_time'] = df.loc[df['end_time'].isna(), 'start_time'] + pd.Timedelta(hours=1)
        
        # Fill missing numeric values
        if 'event_size' in df.columns:
            df['event_size'] = pd.to_numeric(df['event_size'], errors='coerce').fillna(0)
        
        if 'impact_radius' in df.columns:
            df['impact_radius'] = pd.to_numeric(df['impact_radius'], errors='coerce').fillna(0)
        
        return df
    
    def _create_event_records(self, df: pd.DataFrame) -> List[Event]:
        """Create event dimension records from DataFrame"""
        event_records = []
        
        for _, row in df.iterrows():
            event = Event(
                event_type=row.get('event_type', 'Unknown'),
                event_location=row.get('event_location', 'Unknown'),
                event_size=int(row['event_size']) if 'event_size' in row and pd.notna(row['event_size']) else None,
                start_time=row['start_time'],
                end_time=row['end_time'] if 'end_time' in row and pd.notna(row['end_time']) else None,
                impact_radius=float(row['impact_radius']) if 'impact_radius' in row and pd.notna(row['impact_radius']) else None
            )
            event_records.append(event)
        
        return event_records 