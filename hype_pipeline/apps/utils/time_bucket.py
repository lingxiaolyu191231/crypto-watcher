# apps/utils/time_bucket.py
import pandas as pd

def to_utc_series(x):
    # robust utc parsing for strings/naive timestamps
    return pd.to_datetime(x, utc=True, errors="coerce")

def hour_bucket(ts_series):
    # floor to hour, always UTC
    return ts_series.dt.floor("h")
