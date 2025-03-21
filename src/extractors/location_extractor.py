import pandas as pd
import numpy as np
from typing import Dict, Any, List

from src.extractors.base_extractor import BaseExtractor


class LocationExtractor(BaseExtractor):
    """Extractor for location data"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__()
        self.config = config
        self.csv_path = config.get('csv_path')
        self.use_sample_data = config.get('use_sample_data', True)
    
    async def extract(self) -> Dict[str, Any]:
        """Extract location data from CSV or generate sample data"""
        try:
            if self.use_sample_data:
                self.logger.info("Generating sample location data")
                location_count = self.config.get('location_count', 50)
                location_data = self._generate_sample_locations(location_count)
                return {"location_data": location_data}
            elif self.csv_path:
                self.logger.info(f"Extracting location data from {self.csv_path}")
                df = pd.read_csv(self.csv_path)
                return {"location_data": df.to_dict('records')}
            else:
                self.logger.error("No CSV path provided and not using sample data")
                return {"location_data": []}
        except Exception as e:
            self.logger.error(f"Error extracting location data: {str(e)}")
            raise
    
    def _generate_sample_locations(self, count: int) -> List[Dict[str, Any]]:
        """Generate sample location data"""
        # Base coordinates (example: center of a city)
        base_lat, base_lon = 34.0522, -118.2437
        
        road_types = ['Highway', 'Arterial', 'Collector', 'Local']
        districts = ['Downtown', 'North', 'South', 'East', 'West', 'Central']
        
        locations = []
        for i in range(count):
            # Generate nearby locations within ~5km
            lat = base_lat + np.random.uniform(-0.05, 0.05)
            lon = base_lon + np.random.uniform(-0.05, 0.05)
            
            # Random attributes
            district = np.random.choice(districts)
            road_type = np.random.choice(road_types)
            lanes = np.random.randint(1, 5)
            speed_limit = np.random.choice([25, 35, 45, 55, 65])
            
            location = {
                'intersection_id': f'INT{i:04d}',
                'street_name': f'Intersection {i}',
                'latitude': round(lat, 6),
                'longitude': round(lon, 6),
                'district': district,
                'road_type': road_type,
                'lanes': lanes,
                'speed_limit': speed_limit
            }
            locations.append(location)
        
        return locations 