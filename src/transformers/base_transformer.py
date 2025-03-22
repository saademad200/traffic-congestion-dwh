import pandas as pd
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class BaseTransformer:
    """Base class for all transformers"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        logger.debug(f"Initialized {self.__class__.__name__}")
    
    def transform(self, data: Optional[Dict[str, pd.DataFrame]] = None) -> pd.DataFrame:
        """Transform source data into target format"""
        raise NotImplementedError("Subclasses must implement transform method") 