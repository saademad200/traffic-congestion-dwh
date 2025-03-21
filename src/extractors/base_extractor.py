import logging
from abc import ABC, abstractmethod
from typing import Any, Dict

class BaseExtractor(ABC):
    """Base class for all data extractors"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    async def extract(self) -> Dict[str, Any]:
        """Extract data from source"""
        pass
    
    def validate_extraction(self, data: Dict[str, Any]) -> bool:
        """Validate extracted data"""
        if data is None:
            self.logger.error("Extracted data is None")
            return False
        return True 