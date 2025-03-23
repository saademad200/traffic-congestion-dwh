import pandas as pd
from typing import Dict, Any
import logging
from ..base_transformer import BaseTransformer

logger = logging.getLogger(__name__)

class EnvironmentalDimensionTransformer(BaseTransformer):
    """Transformer for Environmental dimension"""
    
    def transform(self, data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Create Environmental dimension from WeatherData only
        Road condition data has been removed from this dimension
        """
        env_records = []
        
        # Primary key of unknown should be 0
        env_records.append({
            'date': None,
            'temperature_c': None,
            'weather_condition': "Unknown"
        })
        
        # Process Weather Data
        if 'WeatherData' in data:
            weather_df = data['WeatherData'].copy()
            
            # Group by date to reduce cardinality
            weather_df['date'] = pd.to_datetime(weather_df['Timestamp']).dt.date
            
            # Group and get typical conditions for each day
            daily_weather = weather_df.groupby('date').agg({
                'Temperature_C': 'mean',
                'Condition': lambda x: x.value_counts().index[0]  # most common condition
            }).reset_index()
            
            for _, row in daily_weather.iterrows():
                # Create record with weather data
                env_records.append({
                    'date': row['date'],
                    'temperature_c': row['Temperature_C'],
                    'weather_condition': row['Condition']
                })
        
        # Create dataframe from all records
        env_df = pd.DataFrame(env_records)
        
        # Ensure the unknown record is not overwritten
        if not env_df.empty:
            env_df.reset_index(inplace=True)
            env_df.rename(columns={'index': 'environmental_key'}, inplace=True)
            env_df['environmental_key'] += 1  # Start keys at 1, but keep the first record as 0
            env_df.loc[0, 'environmental_key'] = 0  # Ensure the unknown record retains its key
        else:
            # Create empty dataframe with correct columns if no data
            env_df = pd.DataFrame(columns=['environmental_key', 'date', 'temperature_c', 'weather_condition'])
        
        logger.info(f"Created Environmental dimension with {len(env_df)} records")
        return env_df 