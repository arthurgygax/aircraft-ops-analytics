import pandas as pd
import numpy as np

def load_data(parquet_path, filters=None):
    try:
        df = pd.read_parquet(parquet_path, filters=filters)
        return df
    except Exception as e:
        print(f"Error loading data from Parquet: {e}")
        return pd.DataFrame()

def classify_flight_direction(df_flight, bbox):
    df = df_flight.sort_values('timestamp')
    if df.empty: return "UNKNOWN"
    
    first_pt = df.iloc[0]
    last_pt = df.iloc[-1]
    
    start_in_box = (bbox[0] < first_pt.latitude < bbox[2]) and (bbox[1] < first_pt.longitude < bbox[3])
    end_in_box = (bbox[0] < last_pt.latitude < bbox[2]) and (bbox[1] < last_pt.longitude < bbox[3])
    
    alt_change = last_pt['altitude'] - first_pt['altitude']

    if start_in_box and not end_in_box: return "DEPARTURE"
    if not start_in_box and end_in_box: return "ARRIVAL"
    
    if start_in_box and end_in_box:
        if alt_change > 500: return "DEPARTURE"
        if alt_change < -500: return "ARRIVAL"
        return "UNKNOWN"
    
    dist_start_sq = (first_pt.latitude - 47.46)**2 + (first_pt.longitude - 8.55)**2
    if dist_start_sq < 0.04 and alt_change > 2000:
        return "DEPARTURE"
        
    return "OVERFLIGHT"

def clean_flight_data(df):
    """
    Strict physics-based filtering to remove sensor glitches.
    """
    # 1. Hard Limits: Remove impossible altitudes/speeds
    df = df[
        (df['altitude'] > -1500) & (df['altitude'] < 40000) &
        (df['groundspeed'] >= 0) & (df['groundspeed'] < 800)
    ].copy()

    # 2. Implied Speed Check (The "Teleport" Killer)
    # We calculate the speed implied by the jump between points.
    df.sort_values('timestamp', inplace=True)
    
    # Vectorized calculation of distance/time diffs
    df['dt'] = df['timestamp'].diff().dt.total_seconds()
    d_lat = df['latitude'].diff()
    d_lon = df['longitude'].diff()
    
    # Euclidean distance in degrees (approximate but sufficient for glitch detection)
    # 1 degree ~ 60nm ~ 111km
    dist_deg = np.sqrt(d_lat**2 + d_lon**2)
    
    # Calculate implied speed in degrees/second
    # Commercial jets cruise approx 0.002 deg/s.
    # We set threshold at 0.003 (approx 1200 km/h or Mach 1) to be safe but strict.
    implied_speed = dist_deg / df['dt']
    
    # Filter Logic:
    # Keep row IF:
    # 1. It's the first point (dt is NaN)
    # 2. OR The time gap is huge (> 1 hour) - allows oceanic flights to reappear
    # 3. OR The speed required to get here was realistic (< Mach 1)
    is_valid = (df['dt'].isna()) | (df['dt'] > 3600) | (implied_speed < 0.003)
    
    return df[is_valid].drop(columns=['dt'])

def detect_phases(df_flight):
    df = df_flight.sort_values('timestamp').copy()
    
    # Check for required columns (handling both 'onground' and 'on_ground' just in case)
    if 'on_ground' in df.columns:
        df = df.rename(columns={'on_ground': 'onground'})

    req_cols = ['vertical_rate', 'groundspeed', 'altitude', 'onground']
    if not all(col in df.columns for col in req_cols):
        df['phase'] = 'UNKNOWN'
        return df

    # CLEANING: Remove glitches before processing phases
    df = clean_flight_data(df)
    
    if df.empty:
        df['phase'] = 'UNKNOWN'
        return df

    # Phase Logic
    vs_smooth = df['vertical_rate'].rolling(window=5, min_periods=1, center=True).mean().fillna(df['vertical_rate'])
    
    is_taxi = (df['onground'] == True) | ((df['altitude'] < 150) & (df['groundspeed'] < 30))
    is_climb = (~is_taxi) & (vs_smooth > 8.3)
    is_descent = (~is_taxi) & (vs_smooth < -8.3)
    is_level = (~is_taxi) & (vs_smooth.between(-8.3, 8.3))
    is_below_cruise = df['altitude'] < 15000 
    is_level_off = is_level & is_below_cruise

    df['phase'] = 'CRUISE'
    df.loc[is_level_off, 'phase'] = 'LEVEL_OFF'
    df.loc[is_descent, 'phase'] = 'DESCENT'
    df.loc[is_climb, 'phase'] = 'CLIMB'
    df.loc[is_taxi, 'phase'] = 'TAXI'
    
    return df