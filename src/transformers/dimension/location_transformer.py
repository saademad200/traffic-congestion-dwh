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
        
        for table_name in location_tables:
            if table_name in data:
                df = data[table_name]
                if 'Location' in df.columns:
                    # Extract unique locations from this table
                    unique_locs = df['Location'].unique()
                    for loc in unique_locs:
                        locations.append({
                            'location_name': loc,
                            'location_source': table_name
                        })
        
        # Create dataframe
        location_df = pd.DataFrame(locations)
        
        # Add surrogate key
        if not location_df.empty:
            location_df.reset_index(inplace=True)
            location_df.rename(columns={'index': 'location_key'}, inplace=True)
            location_df['location_key'] += 1  # Start keys at 1
        else:
            # Create empty dataframe with correct columns
            location_df = pd.DataFrame(columns=['location_key', 'location_name', 'location_source'])
        
        logger.info(f"Created Location dimension with {len(location_df)} records")
        return location_df 