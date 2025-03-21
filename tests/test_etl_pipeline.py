import pytest
import os
import pandas as pd
from datetime import datetime
from src.config.config import CONFIG
from src.extractors.excel_extractor import ExcelExtractor
from src.transformers.traffic_transformer import TrafficTransformer
from src.transformers.weather_transformer import WeatherTransformer
from src.loaders.database_loader import DatabaseLoader
from src.models.schema import TrafficMeasurement, Location, Weather, TimeRecord

class TestETLPipeline:
    
    @pytest.fixture
    def setup_test_data(self):
        # Setup test data path
        os.environ['EXCEL_FILE_PATH'] = 'tests/data/test_traffic_data.xlsx'
        
        # Create test database config
        db_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'test_traffic_dwh',
            'user': 'test_user',
            'password': 'test_password'
        }
        
        return {
            'db_config': db_config,
            'excel_config': {
                'excel_file_path': 'tests/data/test_traffic_data.xlsx',
                'sheet_mappings': {
                    'traffic_data': 'Traffic Measurements',
                    'location_data': 'Locations',
                    'weather_data': 'Weather'
                }
            }
        }
    
    @pytest.mark.asyncio
    async def test_extraction_transformation_integration(self, setup_test_data, mocker):
        # Mock the extraction since we might not have the actual file
        mock_excel_extractor = mocker.patch('src.extractors.excel_extractor.ExcelExtractor')
        mock_excel_extractor.return_value.extract.return_value = {
            'traffic_data': [
                {
                    'measurement_timestamp': datetime.now(),
                    'vehicle_count': 45,
                    'avg_speed': 32.5,
                    'occupancy_rate': 0.45,
                    'location_id': 'INT0001'
                }
            ],
            'location_data': [
                {
                    'intersection_id': 'INT0001',
                    'street_name': 'Main St & 1st Ave',
                    'latitude': 34.05,
                    'longitude': -118.25,
                    'district': 'Downtown',
                    'road_type': 'Arterial',
                    'lanes': 4,
                    'speed_limit': 35
                }
            ]
        }
        
        # Create transformers
        traffic_transformer = TrafficTransformer()
        location_transformer = LocationTransformer()
        
        # Extract data
        extractor = mock_excel_extractor()
        extracted_data = await extractor.extract()
        
        # Transform data
        traffic_transformed = traffic_transformer.transform({
            'traffic_data': extracted_data.get('traffic_data', [])
        })
        
        location_transformed = location_transformer.transform({
            'location_data': extracted_data.get('location_data', [])
        })
        
        # Assertions
        assert traffic_transformed is not None
        assert 'traffic_measurements' in traffic_transformed
        assert 'time_records' in traffic_transformed
        assert len(traffic_transformed['traffic_measurements']) > 0
        
        assert location_transformed is not None
        assert 'location_records' in location_transformed
        assert len(location_transformed['location_records']) > 0
    
    @pytest.mark.asyncio
    async def test_loader_with_mocks(self, setup_test_data, mocker):
        # Mock the database session
        mock_session = mocker.Mock()
        mock_session.execute.return_value.fetchone.return_value = None
        mock_session.execute.return_value.scalar_one.return_value = 1
        mock_engine = mocker.Mock()
        mock_engine.begin.return_value.__aenter__.return_value = mock_session
        
        # Create sample data
        locations = [
            Location(
                intersection_id='INT0001',
                street_name='Main St & 1st Ave',
                latitude=34.05,
                longitude=-118.25,
                district='Downtown',
                road_type='Arterial',
                lanes=4,
                speed_limit=35
            )
        ]
        
        # Mock database loader
        mock_create_engine = mocker.patch('sqlalchemy.ext.asyncio.create_async_engine')
        mock_create_engine.return_value = mock_engine
        
        # Initialize loader with mock engine
        db_loader = DatabaseLoader(setup_test_data['db_config'])
        
        # Test load_locations
        location_keys = await db_loader.load_locations(locations)
        
        # Verify interactions
        assert mock_session.execute.called
        assert 'INT0001' in location_keys 