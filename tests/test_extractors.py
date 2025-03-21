import pytest
import pandas as pd
from datetime import datetime, timedelta
from src.extractors.traffic_sensor_extractor import TrafficSensorExtractor
from src.extractors.weather_extractor import WeatherExtractor
from src.extractors.location_extractor import LocationExtractor
from src.extractors.event_extractor import EventExtractor
from src.extractors.vehicle_extractor import VehicleExtractor
from src.extractors.infrastructure_extractor import InfrastructureExtractor
from src.extractors.excel_extractor import ExcelExtractor

class TestExtractors:
    
    @pytest.fixture
    def setup_configs(self):
        base_config = {
            'use_sample_data': True
        }
        
        traffic_config = base_config.copy()
        traffic_config.update({'sample_days': 3})
        
        weather_config = base_config.copy()
        weather_config.update({'sample_days': 3})
        
        location_config = base_config.copy()
        location_config.update({'location_count': 10})
        
        event_config = base_config.copy()
        event_config.update({'event_count': 5})
        
        vehicle_config = base_config.copy()
        vehicle_config.update({'vehicle_count': 5})
        
        infrastructure_config = base_config.copy()
        infrastructure_config.update({'infrastructure_count': 5})
        
        excel_config = {
            'excel_file_path': 'tests/data/test_traffic_data.xlsx',
            'sheet_mappings': {
                'traffic_data': 'Traffic Measurements',
                'location_data': 'Locations'
            }
        }
        
        return {
            'traffic': traffic_config,
            'weather': weather_config,
            'location': location_config,
            'event': event_config,
            'vehicle': vehicle_config,
            'infrastructure': infrastructure_config,
            'excel': excel_config
        }
    
    @pytest.mark.asyncio
    async def test_traffic_sensor_extractor(self, setup_configs):
        extractor = TrafficSensorExtractor(setup_configs['traffic'])
        result = await extractor.extract()
        
        assert result is not None
        assert 'traffic_data' in result
        assert isinstance(result['traffic_data'], list)
        assert len(result['traffic_data']) > 0
        
        # Verify data structure
        first_record = result['traffic_data'][0]
        assert 'measurement_timestamp' in first_record
        assert 'vehicle_count' in first_record
        assert 'avg_speed' in first_record
        assert 'occupancy_rate' in first_record
    
    @pytest.mark.asyncio
    async def test_weather_extractor(self, setup_configs):
        extractor = WeatherExtractor(setup_configs['weather'])
        result = await extractor.extract()
        
        assert result is not None
        assert 'weather_data' in result
        assert isinstance(result['weather_data'], list)
        assert len(result['weather_data']) > 0
        
        # Verify data structure
        first_record = result['weather_data'][0]
        assert 'timestamp' in first_record
        assert 'condition' in first_record
        assert 'temperature' in first_record
    
    @pytest.mark.asyncio
    async def test_location_extractor(self, setup_configs):
        extractor = LocationExtractor(setup_configs['location'])
        result = await extractor.extract()
        
        assert result is not None
        assert 'location_data' in result
        assert isinstance(result['location_data'], list)
        assert len(result['location_data']) > 0
        
        # Verify data structure
        first_record = result['location_data'][0]
        assert 'intersection_id' in first_record
        assert 'latitude' in first_record
        assert 'longitude' in first_record
    
    @pytest.mark.asyncio
    async def test_event_extractor(self, setup_configs):
        extractor = EventExtractor(setup_configs['event'])
        result = await extractor.extract()
        
        assert result is not None
        assert 'event_data' in result
        assert isinstance(result['event_data'], list)
        assert len(result['event_data']) > 0
        
        # Verify data structure
        first_record = result['event_data'][0]
        assert 'event_type' in first_record
        assert 'event_location' in first_record
        assert 'start_time' in first_record 