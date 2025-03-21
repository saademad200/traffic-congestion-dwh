import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.transformers.traffic_transformer import TrafficTransformer
from src.transformers.weather_transformer import WeatherTransformer

class TestDataQuality:
    
    @pytest.fixture
    def problematic_traffic_data(self):
        # Create problematic traffic data with missing values, outliers, etc.
        now = datetime.now()
        data = [
            {
                'measurement_timestamp': now,
                'vehicle_count': 45,
                'avg_speed': 32.5,
                'occupancy_rate': 0.45
            },
            {
                'measurement_timestamp': now - timedelta(minutes=5),
                'vehicle_count': None,  # Missing value
                'avg_speed': 30.0,
                'occupancy_rate': 0.4
            },
            {
                'measurement_timestamp': now - timedelta(minutes=10),
                'vehicle_count': 50,
                'avg_speed': 200.0,  # Outlier
                'occupancy_rate': 0.5
            },
            {
                'measurement_timestamp': now - timedelta(minutes=15),
                'vehicle_count': 55,
                'avg_speed': 35.0,
                'occupancy_rate': 1.5  # Invalid value (>1)
            }
        ]
        return {'traffic_data': data}
    
    def test_traffic_transformer_handles_missing_values(self, problematic_traffic_data):
        transformer = TrafficTransformer()
        result = transformer.transform(problematic_traffic_data)
        
        # All records should be transformed, even with problems
        assert len(result['traffic_measurements']) == 4
        
        # Check missing vehicle count was handled
        measurements = result['traffic_measurements']
        second_record = [m for m in measurements 
                        if m.measurement_timestamp == problematic_traffic_data['traffic_data'][1]['measurement_timestamp']][0]
        assert second_record.vehicle_count is not None
        assert second_record.vehicle_count == 0  # Should have been filled with 0
        
        # Check outlier speed was handled
        third_record = [m for m in measurements 
                       if m.measurement_timestamp == problematic_traffic_data['traffic_data'][2]['measurement_timestamp']][0]
        assert third_record.avg_speed <= 100  # Should have been capped
        
        # Check invalid occupancy rate was handled
        fourth_record = [m for m in measurements 
                       if m.measurement_timestamp == problematic_traffic_data['traffic_data'][3]['measurement_timestamp']][0]
        assert fourth_record.occupancy_rate <= 1.0  # Should have been capped at 1.0 