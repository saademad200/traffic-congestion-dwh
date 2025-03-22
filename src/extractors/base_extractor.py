import pandas as pd
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class BaseExtractor:
    """Base class for data extractors"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.source_file = config['source']['source_file']
    
    def extract(self) -> pd.DataFrame:
        """Extract data from source"""
        raise NotImplementedError("Subclasses must implement extract method")
