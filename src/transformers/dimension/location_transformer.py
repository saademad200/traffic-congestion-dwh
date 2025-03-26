import pandas as pd
from typing import Dict, Any
import logging
from ..base_transformer import BaseTransformer

logger = logging.getLogger(__name__)

class LocationDimensionTransformer(BaseTransformer):
    """Transformer for Location dimension"""
    
    def transform(self, data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Create Location dimension from all source tables with location data
        Note: EDA showed zero overlap between locations across source tables
        """
        # Tables with Location attribute
        location_tables = ['TrafficFlow', 'Accidents', 'CongestionLevels', 
                          'SpeedViolations', 'RoadClosures']
        
        # Collect all locations with their source
        locations = []
        
        # First collect all locations from source tables
        for table_name in location_tables:
            if table_name in data:
                df = data[table_name]
                if 'Location' in df.columns:
                    # Extract unique locations from this table
                    unique_locs = df['Location'].unique()
                    for loc in unique_locs:
                        if pd.notna(loc) and loc != 'Unknown':  # Skip nulls and 'Unknown'
                            locations.append({
                                'location_name': loc,
                                'location_source': table_name
                            })
        
        # Create dataframe
        location_df = pd.DataFrame(locations)
        
        # Add surrogate key - start from 1 for all regular locations
        if not location_df.empty:
            # Create a new clean sequence of IDs starting from 1
            location_df['location_key'] = range(1, len(location_df) + 1)
        else:
            # Create empty dataframe with correct columns
            location_df = pd.DataFrame(columns=['location_key', 'location_name', 'location_source'])
        
        # Add the unknown record with key 0 at the beginning
        unknown_record = pd.DataFrame([{
            'location_key': 0,
            'location_name': 'Unknown',
            'location_source': 'Unknown'
        }])
        
        # Concatenate the unknown record at the beginning
        location_df = pd.concat([unknown_record, location_df], ignore_index=True)
        
        logger.info(f"Created Location dimension with {len(location_df)} records")
        return location_df 