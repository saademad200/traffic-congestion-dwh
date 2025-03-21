import aiohttp
import json
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Optional

from src.extractors.base_extractor import BaseExtractor
from src.utils.sample_data_generator import generate_sample_traffic_data


class TrafficSensorExtractor(BaseExtractor):
    """Extractor for traffic sensor data"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__()
        self.config = config
        self.use_sample_data = config.get('use_sample_data', True)
        self.api_endpoint = config.get('api_endpoint')
    
    async def extract(self) -> Dict[str, Any]:
        """Extract traffic sensor data from API or generate sample data"""
        try:
            if self.use_sample_data:
                self.logger.info("Generating sample traffic sensor data")
                days = self.config.get('sample_days', 7)
                df = generate_sample_traffic_data(days)
                return {"traffic_data": df.to_dict('records')}
            else:
                self.logger.info(f"Extracting traffic sensor data from {self.api_endpoint}")
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.api_endpoint) as response:
                        if response.status == 200:
                            data = await response.json()
                            return {"traffic_data": data}
                        else:
                            self.logger.error(f"Failed to extract traffic data: {response.status}")
                            return {"traffic_data": []}
        except Exception as e:
            self.logger.error(f"Error extracting traffic data: {str(e)}")
            raise 