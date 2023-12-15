import re
from datetime import datetime
import pandas as pd

def _validate_dates(start, end):
    start = pd.to_datetime(start) if start else datetime(1980, 1, 1)
    end = pd.to_datetime(end) if end else datetime.today()
    return start, end