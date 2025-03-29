import pandas as pd
import logging
import os
import sys
from typing import Dict, Any
from datetime import datetime

# Import config module
from config.config import CONFIG

# Import extractors and transformers
from extractors import TrafficDataExtractor
from transformers.dimension import (
    LocationDimensionTransformer,
    DateDimensionTransformer,
    TimeDimensionTransformer,
    VehicleDimensionTransformer,
    EventTypeDimensionTransformer,
    EnvironmentalDimensionTransformer
)
from transformers import FactTableTransformer

# Import loaders
from loaders.warehouse_loader import WarehouseLoader

# Configure logging
log_level = getattr(logging, CONFIG['processing']['log_level'])
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/traffic_etl.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def main():
    """Main ETL process"""
    start_time = datetime.now()
    logger.info("Starting Traffic Flow ETL process")
    
    try:
        # Use configuration from config.py
        config = CONFIG
        
        # 1. EXTRACT
        logger.info("Starting data extraction")
        extractor = TrafficDataExtractor(config)
        source_data = extractor.extract_all()
        logger.info(f"Extracted data from {len(source_data)} tables")
        
        # 2. TRANSFORM DIMENSIONS
        logger.info("Starting dimension transformations")
        
        # Transform each dimension
        dimensions = {
            'DimLocation': LocationDimensionTransformer(config).transform(source_data),
            'DimDate': DateDimensionTransformer(config).transform(),
            'DimTime': TimeDimensionTransformer(config).transform(),
            'DimVehicle': VehicleDimensionTransformer(config).transform(source_data),
            'DimEventType': EventTypeDimensionTransformer(config).transform(),
            'DimEnvironmental': EnvironmentalDimensionTransformer(config).transform(source_data)
        }
        # 3. TRANSFORM FACT TABLE
        logger.info("Starting fact table transformation")
        fact_transformer = FactTableTransformer(config)
        fact_traffic_events = fact_transformer.transform(source_data, dimensions)
        
        # 4. LOAD DATA WAREHOUSE
        logger.info("Starting data warehouse loading")
        loader = WarehouseLoader(config)
        
        # Remove date column from environmental dimension (It was there for mapping purposes)
        if 'DimEnvironmental' in dimensions:
            dimensions['DimEnvironmental'] = dimensions['DimEnvironmental'].drop('date', axis=1)
        
        # Load dimensions first (in correct order for foreign keys)
        for dim_name, dim_df in dimensions.items():
            loader.load_table(dim_name, dim_df)
        
        # Load fact table
        loader.load_table('FactTrafficEvents', fact_traffic_events)
        
        # Log completion
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"ETL process completed successfully in {duration:.2f} seconds")
    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main() 