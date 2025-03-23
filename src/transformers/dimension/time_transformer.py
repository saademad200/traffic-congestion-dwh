import pandas as pd
from typing import Dict, Any, Optional
import logging
from datetime import datetime
from ..base_transformer import BaseTransformer

logger = logging.getLogger(__name__)

class TimeDimensionTransformer(BaseTransformer):
    """Transformer for Time dimension"""
    
    def transform(self, data: Optional[Dict[str, pd.DataFrame]] = None) -> pd.DataFrame:
        """
        Create Time dimension with minute-level granularity (1440 records)
        Independent of source data
        """
        times = []
        # Primary key of unknown should be 0
        times.append({
            'time_of_day': None,
            'hour': None,
            'minute': None,
            'peak_hour_flag': None,
            'day_segment': None,
            'time_key': 0  # Set primary key for unknown record
        })
        # Peak hours based on eda
        morning_peak_start = 13
        morning_peak_end = 13
        evening_peak_start = 18
        evening_peak_end = 19
        
        for hour in range(24):
            for minute in range(60):
                time_obj = datetime.strptime(f"{hour:02d}:{minute:02d}", "%H:%M").time()
                
                # Determine if peak hour
                is_peak = ((morning_peak_start <= hour <= morning_peak_end) or 
                           (evening_peak_start <= hour <= evening_peak_end))
                
                # Determine day segment
                if 5 <= hour < 12:
                    day_segment = 'Morning'
                elif 12 <= hour < 17:
                    day_segment = 'Afternoon'
                elif 17 <= hour < 21:
                    day_segment = 'Evening'
                else:
                    day_segment = 'Night'
                
                times.append({
                    'time_of_day': time_obj,
                    'hour': hour,
                    'minute': minute,
                    'peak_hour_flag': is_peak,
                    'day_segment': day_segment,
                    'time_key': hour * 100 + minute  # Set primary key for each time record
                })
        
        # Create dataframe
        time_df = pd.DataFrame(times)
        
        logger.info(f"Created Time dimension with {len(time_df)} records")
        return time_df 