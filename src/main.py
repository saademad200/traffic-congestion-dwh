import asyncio
import logging
from datetime import datetime
import os

# Import configuration directly
from src.config.config import CONFIG

from src.extractors.excel_extractor import ExcelExtractor

from src.transformers.traffic_transformer import TrafficTransformer
from src.transformers.weather_transformer import WeatherTransformer
from src.transformers.location_transformer import LocationTransformer
from src.transformers.event_transformer import EventTransformer
from src.transformers.vehicle_transformer import VehicleTransformer
from src.transformers.infrastructure_transformer import InfrastructureTransformer

from src.loaders.database_loader import DatabaseLoader


def setup_logging():
    """Setup logging configuration based on config"""
    log_config = CONFIG['logging']
    
    handlers = []
    if log_config['log_to_file']:
        handlers.append(logging.FileHandler(log_config['log_file']))
    handlers.append(logging.StreamHandler())
    
    logging.basicConfig(
        level=getattr(logging, log_config['level']),
        format=log_config['format'],
        handlers=handlers
    )


async def run_etl_pipeline():
    """Run the ETL pipeline"""
    logger = logging.getLogger("ETLPipeline")
    logger.info("Starting ETL pipeline")
    
    start_time = datetime.now()
    
    try:
        # 1. Extract data from Excel
        logger.info("Starting extraction phase")
        
        excel_extractor = ExcelExtractor(CONFIG['excel'])
        extracted_data = await excel_extractor.extract()
        
        logger.info("Extraction phase completed")
        
        # 2. Transform
        logger.info("Starting transformation phase")
        
        # Transform traffic data
        traffic_transformer = TrafficTransformer()
        traffic_transformed = traffic_transformer.transform({
            'traffic_data': extracted_data.get('traffic_data', [])
        })
        
        # Transform weather data
        weather_transformer = WeatherTransformer()
        weather_transformed = weather_transformer.transform({
            'weather_data': extracted_data.get('weather_data', [])
        })
        
        # Transform location data
        location_transformer = LocationTransformer()
        location_transformed = location_transformer.transform({
            'location_data': extracted_data.get('location_data', [])
        })
        
        # Transform event data
        event_transformer = EventTransformer()
        event_transformed = event_transformer.transform({
            'event_data': extracted_data.get('event_data', [])
        })
        
        # Transform vehicle data
        vehicle_transformer = VehicleTransformer()
        vehicle_transformed = vehicle_transformer.transform({
            'vehicle_data': extracted_data.get('vehicle_data', [])
        })
        
        # Transform infrastructure data
        infrastructure_transformer = InfrastructureTransformer()
        infrastructure_transformed = infrastructure_transformer.transform({
            'infrastructure_data': extracted_data.get('infrastructure_data', [])
        })
        
        logger.info("Transformation phase completed")
        
        # 3. Load
        logger.info("Starting loading phase")
        
        # Initialize database loader
        db_loader = DatabaseLoader(CONFIG['database'])
        
        # Load dimensions first
        location_keys = await db_loader.load_locations(location_transformed['location_records'])
        time_keys = await db_loader.load_time_records(traffic_transformed['time_records'])
        weather_keys = await db_loader.load_weather_records(weather_transformed['weather_records'])
        event_keys = await db_loader.load_events(event_transformed['event_records'])
        vehicle_keys = await db_loader.load_vehicles(vehicle_transformed['vehicle_records'])
        infrastructure_keys = await db_loader.load_infrastructure(infrastructure_transformed['infrastructure_records'])
        
        # Load facts
        await db_loader.load_traffic_measurements(
            traffic_transformed['traffic_measurements'],
            location_keys,
            time_keys,
            weather_keys,
            event_keys,
            vehicle_keys,
            infrastructure_keys
        )
        
        logger.info("Loading phase completed")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"ETL pipeline completed successfully in {duration:.2f} seconds")
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}")
        raise


if __name__ == "__main__":
    setup_logging()
    asyncio.run(run_etl_pipeline()) 