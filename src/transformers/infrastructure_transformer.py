import pandas as pd
from datetime import datetime
from typing import Dict, Any, List

from src.transformers.base_transformer import BaseTransformer
from src.models.schema import Infrastructure


class InfrastructureTransformer(BaseTransformer):
    """Transformer for infrastructure data"""
    
    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform infrastructure data"""
        try:
            infrastructure_data = data.get("infrastructure_data", [])
            if not infrastructure_data:
                self.logger.warning("No infrastructure data to transform")
                return {"infrastructure_records": []}
            
            # Convert to DataFrame if it's a list of dictionaries
            if isinstance(infrastructure_data, list):
                df = pd.DataFrame(infrastructure_data)
            else:
                df = infrastructure_data
            
            # Clean and standardize infrastructure data
            df_cleaned = self._clean_data(df)
            
            # Create infrastructure dimension records
            infrastructure_records = self._create_infrastructure_records(df_cleaned)
            
            return {
                "infrastructure_records": infrastructure_records
            }
            
        except Exception as e:
            self.logger.error(f"Error transforming infrastructure data: {str(e)}")
            raise
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean infrastructure data"""
        # Required columns
        required_cols = ['signal_type']
        for col in required_cols:
            if col not in df.columns:
                self.logger.warning(f"Required column '{col}' not found in infrastructure data")
                df[col] = "Unknown"
        
        # Fill missing values for string columns
        string_cols = ['road_condition', 'construction_status']
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].fillna('Unknown')
        
        # Convert date columns
        if 'last_maintenance_date' in df.columns:
            df['last_maintenance_date'] = pd.to_datetime(df['last_maintenance_date'], errors='coerce')
        
        # Fill missing numeric values
        if 'capacity' in df.columns:
            df['capacity'] = pd.to_numeric(df['capacity'], errors='coerce').fillna(0).astype(int)
        
        # Handle special features list
        if 'special_features' in df.columns:
            # Convert string representations to actual lists
            df['special_features'] = df['special_features'].apply(
                lambda x: x.split(',') if isinstance(x, str) else 
                ([] if pd.isna(x) else x)
            )
        
        return df
    
    def _create_infrastructure_records(self, df: pd.DataFrame) -> List[Infrastructure]:
        """Create infrastructure dimension records from DataFrame"""
        infrastructure_records = []
        
        for _, row in df.iterrows():
            infrastructure = Infrastructure(
                signal_type=row.get('signal_type', 'Unknown'),
                road_condition=row.get('road_condition', None),
                construction_status=row.get('construction_status', None),
                last_maintenance_date=row['last_maintenance_date'] if 'last_maintenance_date' in row and pd.notna(row['last_maintenance_date']) else None,
                capacity=int(row['capacity']) if 'capacity' in row and pd.notna(row['capacity']) else None,
                special_features=row.get('special_features', None)
            )
            infrastructure_records.append(infrastructure)
        
        return infrastructure_records 