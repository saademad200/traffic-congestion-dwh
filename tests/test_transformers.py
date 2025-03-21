import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.transformers.traffic_transformer import TrafficTransformer
from src.transformers.weather_transformer import WeatherTransformer
from src.transformers.location_transformer import LocationTransformer
from src.transformers.event_transformer import EventTransformer
from src.transformers.vehicle_transformer import VehicleTransformer
from src.transformers.infrastructure_transformer import InfrastructureTransformer

class TestTransformers:
    
    @pytest.fixture
    def sample_traffic_data(self):
        # Create sample traffic data
        now = datetime.now()
        data = []
        
        for i in range(10):
            timestamp = now - timedelta(minutes=i*5)
            data.append({
                'measurement_timestamp': timestamp,
                'vehicle_count': np.random.randint(0, 100),
                'avg_speed': np.random.uniform(0, 60),
                'occupancy_rate': np.random.uniform(0, 1),
                'queue_length': np.random.randint(0, 20),
                'travel_time': np.random.uniform(0, 300)
            })
            
        return {'traffic_data': data}
    
    @pytest.fixture
    def sample_weather_data(self):
        # Create sample weather data
        now = datetime.now()
        data = []
        
        conditions = ['Clear', 'Cloudy', 'Rain', 'Fog']
        
        for i in range(10):
            timestamp = now - timedelta(hours=i)
            data.append({
                'timestamp': timestamp,
                'condition': np.random.choice(conditions),
                'temperature': np.random.uniform(0, 30),
                'precipitation': np.random.uniform(0, 10),
                'visibility': np.random.uniform(0, 10),
                'wind_speed': np.random.uniform(0, 20)
            })
            
        return {'weather_data': data}
    
    @pytest.fixture
    def sample_location_data(self):
        # Create sample location data
        data = []
        
        districts = ['Downtown', 'North', 'South', 'East', 'West']
        road_types = ['Highway', 'Arterial', 'Collector', 'Local']
        
        for i in range(10):
            data.append({
                'intersection_id': f'INT{i:04d}',
                'street_name': f'Intersection {i}',
                'latitude': np.random.uniform(34.0, 34.1),
                'longitude': np.random.uniform(-118.3, -118.2),
                'district': np.random.choice(districts),
                'road_type': np.random.choice(road_types),
                'lanes': np.random.randint(1, 6),
                'speed_limit': np.random.choice([25, 35, 45, 55, 65])
            })
            
        return {'location_data': data}
    
    def test_traffic_transformer(self, sample_traffic_data):
        transformer = TrafficTransformer()
        result = transformer.transform(sample_traffic_data)
        
        assert result is not None
        assert 'traffic_measurements' in result
        assert 'time_records' in result
        
        assert len(result['traffic_measurements']) > 0
        assert len(result['time_records']) > 0
        
        # Verify transformed objects
        first_measurement = result['traffic_measurements'][0]
        assert hasattr(first_measurement, 'vehicle_count')
        assert hasattr(first_measurement, 'avg_speed')
        assert hasattr(first_measurement, 'measurement_timestamp')
        
        first_time_record = result['time_records'][0]
        assert hasattr(first_time_record, 'timestamp')
        assert hasattr(first_time_record, 'hour')
        assert hasattr(first_time_record, 'day_of_week')
    
    def test_weather_transformer(self, sample_weather_data):
        transformer = WeatherTransformer()
        result = transformer.transform(sample_weather_data)
        
        assert result is not None
        assert 'weather_records' in result
        assert len(result['weather_records']) > 0
        
        # Verify transformed objects
        first_record = result['weather_records'][0]
        assert hasattr(first_record, 'condition')
        assert hasattr(first_record, 'temperature')
        assert hasattr(first_record, 'valid_from')
    
    def test_location_transformer(self, sample_location_data):
        transformer = LocationTransformer()
        result = transformer.transform(sample_location_data)
        
        assert result is not None
        assert 'location_records' in result
        assert len(result['location_records']) > 0
        
        # Verify transformed objects
        first_record = result['location_records'][0]
        assert hasattr(first_record, 'intersection_id')
        assert hasattr(first_record, 'street_name')
        assert hasattr(first_record, 'latitude')
        assert hasattr(first_record, 'longitude') 