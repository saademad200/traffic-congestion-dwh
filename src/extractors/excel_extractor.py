import pandas as pd
import logging
import os
from typing import Dict, Any, List, Optional

from src.extractors.base_extractor import BaseExtractor


class ExcelExtractor(BaseExtractor):
    """Extractor for data from Excel files"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.excel_file_path = config.get('excel_file_path')
        self.sheet_mappings = config.get('sheet_mappings', {})
        
        if not self.excel_file_path:
            raise ValueError("Excel file path is required")
        
        if not os.path.exists(self.excel_file_path):
            raise FileNotFoundError(f"Excel file not found at {self.excel_file_path}")
    
    async def extract(self) -> Dict[str, Any]:
        """Extract data from different sheets of the Excel file"""
        try:
            self.logger.info(f"Extracting data from Excel file: {self.excel_file_path}")
            
            # Initialize the result dictionary
            result = {}
            
            # Read the Excel file
            xl = pd.ExcelFile(self.excel_file_path)
            
            # Get available sheet names
            available_sheets = xl.sheet_names
            self.logger.info(f"Available sheets: {available_sheets}")
            
            # Extract data from each sheet based on mappings
            for data_key, sheet_name in self.sheet_mappings.items():
                if sheet_name in available_sheets:
                    self.logger.info(f"Reading sheet '{sheet_name}' into '{data_key}'")
                    df = pd.read_excel(xl, sheet_name=sheet_name)
                    
                    # Clean up column names
                    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
                    
                    # Convert DataFrame to records
                    result[data_key] = df.to_dict('records')
                    self.logger.info(f"Extracted {len(df)} records from '{sheet_name}'")
                else:
                    self.logger.warning(f"Sheet '{sheet_name}' not found in Excel file")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error extracting data from Excel: {str(e)}")
            raise 