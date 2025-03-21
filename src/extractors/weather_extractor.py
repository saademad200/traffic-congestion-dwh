import aiohttp
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List

from src.extractors.base_extractor import BaseExtractor


class WeatherExtractor(BaseExtractor):
    """Extractor for weather data"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__()
        self.config = config
        self.use_sample_data = config.get('use_sample_data', True)
        self.api_endpoint = config.get('api_endpoint')
    
    async def extract(self) -> Dict[str, Any]:
        """Extract weather data from API or generate sample data"""
        try:
            if self.use_sample_data:
                self.logger.info("Generating sample weather data")
                days = self.config.get('sample_days', 7)
                weather_data = self._generate_sample_weather(days)
                return {"weather_data": weather_data}
            else:
                self.logger.info(f"Extracting weather data from {self.api_endpoint}")
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.api_endpoint) as response:
                        if response.status == 200:
                            data = await response.json()
                            return {"weather_data": data}
                        else:
                            self.logger.error(f"Failed to extract weather data: {response.status}")
                            return {"weather_data": []}
        except Exception as e:
            self.logger.error(f"Error extracting weather data: {str(e)}")
            raise
    
    def _generate_sample_weather(self, days: int) -> List[Dict[str, Any]]:
        """Generate sample weather data"""
        start_date = datetime.now() - timedelta(days=days)
        dates = pd.date_range(start=start_date, periods=days*24, freq='1H')  # hourly data
        
        # Weather conditions
        conditions = ['Clear', 'Cloudy', 'Rain', 'Snow', 'Fog']
        probabilities = [0.5, 0.25, 0.15, 0.05, 0.05]
        
        weather_data = []
        for date in dates:
            condition = np.random.choice(conditions, p=probabilities)
            temp_base = 15 + 10 * np.sin((date.hour - 12) / 12 * np.pi)  # Temperature curve
            
            weather = {
                'timestamp': date.strftime('%Y-%m-%d %H:%M:%S'),
                'condition': condition,
                'temperature': round(temp_base + np.random.uniform(-2, 2), 1),
                'precipitation': round(np.random.uniform(0, 0.5), 2) if condition == 'Rain' else 0,
                'visibility': round(np.random.uniform(5, 10), 1) if condition != 'Fog' else round(np.random.uniform(0.5, 3), 1),
                'wind_speed': round(np.random.uniform(0, 20), 1)
            }
            weather_data.append(weather)
        
        return weather_data 