import pandas as pd
from typing import Dict, Any, Optional
import logging
from ..base_transformer import BaseTransformer

logger = logging.getLogger(__name__)

class EventTypeDimensionTransformer(BaseTransformer):
    """Transformer for EventType dimension"""
    
    def transform(self, data: Optional[Dict[str, pd.DataFrame]] = None) -> pd.DataFrame:
        """
        Create EventType dimension
        This is a static dimension based on our data model
        """
        event_types = [
            {
                'event_type_id': 'FLOW',
                'event_category': 'Flow',
                'event_description': 'Regular traffic flow measurement',
                'severity_scale': 0  # Not applicable for regular flow
            },
            {
                'event_type_id': 'ACC_MINOR',
                'event_category': 'Accident',
                'event_description': 'Minor accident',
                'severity_scale': 3
            },
            {
                'event_type_id': 'ACC_MODERATE',
                'event_category': 'Accident',
                'event_description': 'Moderate accident',
                'severity_scale': 6
            },
            {
                'event_type_id': 'ACC_SEVERE',
                'event_category': 'Accident',
                'event_description': 'Severe accident',
                'severity_scale': 9
            },
            {
                'event_type_id': 'CONGESTION_LOW',
                'event_category': 'Congestion',
                'event_description': 'Low congestion',
                'severity_scale': 2
            },
            {
                'event_type_id': 'CONGESTION_MEDIUM',
                'event_category': 'Congestion',
                'event_description': 'Medium congestion',
                'severity_scale': 5
            },
            {
                'event_type_id': 'CONGESTION_HIGH',
                'event_category': 'Congestion',
                'event_description': 'High congestion',
                'severity_scale': 8
            },
            {
                'event_type_id': 'SPEED_VIOLATION',
                'event_category': 'Violation',
                'event_description': 'Speed limit violation',
                'severity_scale': 4
            },
            {
                'event_type_id': 'ROAD_CLOSURE',
                'event_category': 'Closure',
                'event_description': 'Road closure',
                'severity_scale': 7
            },
            {
                'event_type_id': 'TRAFFIC_LIGHT_STATUS',
                'event_category': 'Infrastructure',
                'event_description': 'Traffic light status update',
                'severity_scale': 1
            }
        ]
        
        # Create dataframe
        event_type_df = pd.DataFrame(event_types)
        
        # Add surrogate key
        event_type_df.reset_index(inplace=True)
        event_type_df.rename(columns={'index': 'event_type_key'}, inplace=True)
        event_type_df['event_type_key'] = event_type_df.index  # Set key for UNKNOWN to 0
        event_type_df.loc[0, 'event_type_key'] = 0  # Ensure the unknown record retains its key
        logger.info(f"Created EventType dimension with {len(event_type_df)} records")
        return event_type_df 