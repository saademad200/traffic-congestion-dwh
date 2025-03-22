import pandas as pd
from typing import Dict, Any, List
import logging
from .base_extractor import BaseExtractor

logger = logging.getLogger(__name__)


class ExcelExtractor(BaseExtractor):
    """Extractor for Excel files"""
    
    def extract(self, sheet_name: str) -> pd.DataFrame:
        """Extract data from specified Excel sheet"""
        try:
            logger.info(f"Extracting data from {self.source_file}, sheet: {sheet_name}")
            df = pd.read_excel(self.source_file, sheet_name=sheet_name)
            logger.info(f"Extracted {len(df)} rows from {sheet_name}")
            return df
        except Exception as e:
            logger.error(f"Error extracting data from {sheet_name}: {str(e)}")
            raise


class TrafficDataExtractor:
    """Extracts all required tables for the traffic flow data warehouse"""
    
    def __init__(self, config: Dict[str, Any]):
        self.excel_extractor = ExcelExtractor(config)
        self.required_tables = config['source']['required_tables']
    
    def extract_all(self) -> Dict[str, pd.DataFrame]:
        """Extract all required tables"""
        data = {}
        for table in self.required_tables:
            data[table] = self.excel_extractor.extract(table)
        return data 