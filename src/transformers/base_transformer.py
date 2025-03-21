import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List

class BaseTransformer(ABC):
    """Base class for all transformers"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform extracted data"""
        pass
    
    def validate_transformation(self, data: Dict[str, Any]) -> bool:
        """Validate transformed data"""
        if data is None:
            self.logger.error("Transformed data is None")
            return False
        return True 