import pytest
from src.extractors.traffic_sensor_extractor import TrafficSensorExtractor
from src.transformers.traffic_transformer import TrafficTransformer

class TestETLPipeline:
    @pytest.fixture
    def setup_test_data(self):
        # Setup test data
        pass

    async def test_extraction(self):
        extractor = TrafficSensorExtractor(test_config)
        data = await extractor.extract()
        assert data is not None
        assert len(data) > 0

    async def test_transformation(self):
        transformer = TrafficTransformer()
        transformed_data = transformer.transform(test_data)
        assert transformed_data.schema == expected_schema 