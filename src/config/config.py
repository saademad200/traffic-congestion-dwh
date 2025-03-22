"""
Configuration settings for the Traffic Flow Data Warehouse.
Environment variables can be used to override default settings.
"""

import os
from typing import Dict, Any

# Database Configuration
DB_CONFIG = {
    'type': os.environ.get('DB_TYPE', 'postgresql'),
    'host': os.environ.get('DB_HOST', 'warehouse_db'),
    'port': int(os.environ.get('DB_PORT', 5432)),
    'database': os.environ.get('DB_NAME', 'traffic_dwh'),
    'schema': os.environ.get('DB_SCHEMA', 'public'),
    'user': os.environ.get('DB_USER', 'dwh_user'),
    'password': os.environ.get('DB_PASSWORD', 'dwh_password')
}

# Source Data Configuration
SOURCE_CONFIG = {
    'source_file': os.environ.get('SOURCE_FILE', '/app/data/traffic_flow_data.xlsx'),
    'required_tables': [
        'TrafficFlow',
        'Accidents',
        'CongestionLevels',
        'Vehicles',
        'RoadConditions',
        'WeatherData',
        'SpeedViolations',
        'RoadClosures'
    ]
}

# Processing Configuration
PROCESSING_CONFIG = {
    'log_level': os.environ.get('LOG_LEVEL', 'INFO'),
    'error_handling': os.environ.get('ERROR_HANDLING', 'continue'),
    'error_threshold': int(os.environ.get('ERROR_THRESHOLD', 100))
}


# Assemble the complete configuration
CONFIG = {
    'database': DB_CONFIG,
    'source': SOURCE_CONFIG,
    'processing': PROCESSING_CONFIG
} 