import pandas as pd
from typing import Dict, Any, List

from src.transformers.base_transformer import BaseTransformer
from src.models.schema import Vehicle


class VehicleTransformer(BaseTransformer):
    """Transformer for vehicle data"""
    
    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform vehicle data"""
        try:
            vehicle_data = data.get("vehicle_data", [])
            if not vehicle_data:
                self.logger.warning("No vehicle data to transform")
                return {"vehicle_records": []}
            
            # Convert to DataFrame if it's a list of dictionaries
            if isinstance(vehicle_data, list):
                df = pd.DataFrame(vehicle_data)
            else:
                df = vehicle_data
            
            # Clean and standardize vehicle data
            df_cleaned = self._clean_data(df)
            
            # Create vehicle dimension records
            vehicle_records = self._create_vehicle_records(df_cleaned)
            
            return {
                "vehicle_records": vehicle_records
            }
            
        except Exception as e:
            self.logger.error(f"Error transforming vehicle data: {str(e)}")
            raise
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean vehicle data"""
        # Required columns
        required_cols = ['vehicle_type', 'vehicle_class']
        for col in required_cols:
            if col not in df.columns:
                self.logger.warning(f"Required column '{col}' not found in vehicle data")
                df[col] = "Unknown"
        
        # Fill missing values for string columns
        string_cols = ['size_category']
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].fillna('Unknown')
        
        # Fill missing numeric values
        if 'passenger_capacity' in df.columns:
            df['passenger_capacity'] = pd.to_numeric(df['passenger_capacity'], errors='coerce').fillna(0).astype(int)
            
        # Standardize vehicle types and classes
        if 'vehicle_type' in df.columns:
            df['vehicle_type'] = df['vehicle_type'].str.lower().str.capitalize()
            
        if 'vehicle_class' in df.columns:
            df['vehicle_class'] = df['vehicle_class'].str.lower().str.capitalize()
        
        return df
    
    def _create_vehicle_records(self, df: pd.DataFrame) -> List[Vehicle]:
        """Create vehicle dimension records from DataFrame"""
        vehicle_records = []
        
        for _, row in df.iterrows():
            vehicle = Vehicle(
                vehicle_type=row.get('vehicle_type', 'Unknown'),
                vehicle_class=row.get('vehicle_class', 'Unknown'),
                size_category=row.get('size_category', None),
                passenger_capacity=int(row['passenger_capacity']) if 'passenger_capacity' in row and pd.notna(row['passenger_capacity']) else None
            )
            vehicle_records.append(vehicle)
        
        return vehicle_records 