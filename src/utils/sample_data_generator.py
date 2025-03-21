import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_sample_traffic_data(days=7):
    # Generate time series data
    start_date = datetime.now() - timedelta(days=days)
    dates = pd.date_range(start=start_date, periods=days*288, freq='5T')
    
    # Generate sample measurements
    data = {
        'measurement_timestamp': dates,
        'vehicle_count': np.random.randint(0, 100, size=len(dates)),
        'avg_speed': np.random.uniform(0, 60, size=len(dates)),
        'occupancy_rate': np.random.uniform(0, 1, size=len(dates))
    }
    
    return pd.DataFrame(data) 