import pandas as pd
from typing import Dict, Any
import logging
from ..base_transformer import BaseTransformer

logger = logging.getLogger(__name__)

class VehicleDimensionTransformer(BaseTransformer):
    """Transformer for Vehicle dimension"""
    
    def transform(self, data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Create Vehicle dimension from Vehicles OLTP table"""
        if 'Vehicles' not in data:
            logger.error("Vehicles table not found in source data")
            return pd.DataFrame(columns=['vehicle_key', 'vehicle_id', 'vehicle_type', 'vehicle_category'])
        
        # Extract vehicle data
        vehicles_df = data['Vehicles'].copy()
        
        # Create vehicle category based on vehicle type
        vehicle_categories = {
            'Sedan': 'Passenger',
            'SUV': 'Passenger',
            'Truck': 'Commercial',
            'Bus': 'Public Transport',
            'Van': 'Commercial',
            'Motorcycle': 'Passenger',
            'Taxi': 'Service',
            'Emergency': 'Service'
        }
        
        # Apply categorization (with default for unknown types)
        vehicles_df['vehicle_category'] = vehicles_df['VehicleType'].apply(
            lambda x: vehicle_categories.get(x, 'Other')
        )
        
        # Rename columns to match dimension schema
        vehicles_df.rename(columns={
            'VehicleID': 'vehicle_id',
            'VehicleType': 'vehicle_type'
        }, inplace=True)
        
        # Add surrogate key
        vehicles_df.reset_index(inplace=True)
        vehicles_df.rename(columns={'index': 'vehicle_key'}, inplace=True)
        vehicles_df['vehicle_key'] += 1  # Start keys at 1
        
        # Select only needed columns
        result_df = vehicles_df[['vehicle_key', 'vehicle_id', 'vehicle_type', 'vehicle_category']]
        
        # Add unknown vehicle with key 0
        unknown_vehicle = {
            'vehicle_key': 0,
            'vehicle_id': 'Unknown',
            'vehicle_type': 'Unknown',
            'vehicle_category': 'Unknown'
        }
        result_df = pd.concat([pd.DataFrame([unknown_vehicle]), result_df], ignore_index=True)
        logger.info(f"Created Vehicle dimension with {len(result_df)} records")
        return result_df 