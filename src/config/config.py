"""
Configuration settings for the Traffic Flow Data Warehouse.
Environment variables can be used to override default settings.
"""

import os
from typing import Dict, Any

# Database Configuration
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'warehouse_db'),
    'port': int(os.environ.get('DB_PORT', 5432)),
    'database': os.environ.get('DB_NAME', 'traffic_dwh'),
    'user': os.environ.get('DB_USER', 'dwh_user'),
    'password': os.environ.get('DB_PASSWORD', 'dwh_password')
}

# Excel File Configuration
EXCEL_CONFIG = {
    'excel_file_path': os.environ.get('EXCEL_FILE_PATH', 'data/traffic_flow_data.xlsx'),
    'sheet_mappings': {
        'traffic_data': 'Traffic Measurements',
        'location_data': 'Locations',
        'weather_data': 'Weather',
        'event_data': 'Events',
        'vehicle_data': 'Vehicles',
        'infrastructure_data': 'Infrastructure'
    }
}

# Logging Configuration
LOGGING_CONFIG = {
    'level': os.environ.get('LOGGING_LEVEL', 'INFO'),
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'log_to_file': os.environ.get('LOG_TO_FILE', 'True').lower() == 'true',
    'log_file': os.environ.get('LOG_FILE', 'etl.log')
}

# ETL Pipeline Configuration
ETL_CONFIG = {
    'batch_size': int(os.environ.get('ETL_BATCH_SIZE', 1000)),
    'retry_attempts': int(os.environ.get('ETL_RETRY_ATTEMPTS', 3)),
    'retry_delay': int(os.environ.get('ETL_RETRY_DELAY', 5))  # seconds
}

# Assemble the complete configuration
CONFIG = {
    'database': DB_CONFIG,
    'excel': EXCEL_CONFIG,
    'logging': LOGGING_CONFIG,
    'etl': ETL_CONFIG
} 