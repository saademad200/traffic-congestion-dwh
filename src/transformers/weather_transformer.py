import pandas as pd
from datetime import datetime
from typing import Dict, Any, List

from src.transformers.base_transformer import BaseTransformer
from src.models.schema import Weather


class WeatherTransformer(BaseTransformer):
    """Transformer for weather data"""
    
    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform weather data"""
        try:
            weather_data = data.get("weather_data", [])
            if not weather_data:
                self.logger.warning("No weather data to transform")
                return {"weather_records": []}
            
            # Convert to DataFrame if it's a list of dictionaries
            if isinstance(weather_data, list):
                df = pd.DataFrame(weather_data)
            else:
                df = weather_data
            
            # Clean and standardize weather data
            df_cleaned = self._clean_data(df)
            
            # Create weather dimension records
            weather_records = self._create_weather_records(df_cleaned)
            
            return {
                "weather_records": weather_records
            }
            
        except Exception as e:
            self.logger.error(f"Error transforming weather data: {str(e)}")
            raise
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean weather data"""
        # Convert timestamp to datetime if it's not
        if 'timestamp' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Fill missing values for numeric columns
        numeric_cols = ['temperature', 'precipitation', 'visibility', 'wind_speed']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0)
        
        # Standardize condition names
        if 'condition' in df.columns:
            df['condition'] = df['condition'].fillna('Unknown')
            condition_mapping = {
                'clear': 'Clear',
                'sunny': 'Clear',
                'partly cloudy': 'Cloudy',
                'cloudy': 'Cloudy',
                'overcast': 'Cloudy',
                'rain': 'Rain',
                'raining': 'Rain',
                'snow': 'Snow',
                'snowing': 'Snow',
                'fog': 'Fog',
                'foggy': 'Fog'
            }
            df['condition'] = df['condition'].str.lower().map(
                lambda x: condition_mapping.get(x, 'Unknown')
            )
        
        # Drop duplicates
        df = df.drop_duplicates(subset=['timestamp'], keep='first')
        
        return df
    
    def _create_weather_records(self, df: pd.DataFrame) -> List[Weather]:
        """Create weather dimension records from DataFrame"""
        weather_records = []
        
        for _, row in df.iterrows():
            weather = Weather(
                condition=row.get('condition', None),
                temperature=float(row.get('temperature', 0)) if pd.notna(row.get('temperature', None)) else None,
                precipitation=float(row.get('precipitation', 0)) if pd.notna(row.get('precipitation', None)) else None,
                visibility=float(row.get('visibility', 0)) if pd.notna(row.get('visibility', None)) else None,
                wind_speed=float(row.get('wind_speed', 0)) if pd.notna(row.get('wind_speed', None)) else None,
                valid_from=row['timestamp'] if 'timestamp' in row else datetime.now()
            )
            weather_records.append(weather)
        
        return weather_records 