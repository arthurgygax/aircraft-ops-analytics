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
    df = df.dropna(subset=['latitude', 'longitude', 'altitude', 'timestamp']).copy()
    if df.empty: return df

    df = df[
        (df['altitude'] > -1500) & (df['altitude'] < 60000) &
        (df['groundspeed'] >= 0) & (df['groundspeed'] < 800)
    ].copy()

    if df.empty: return df

    df.sort_values('timestamp', inplace=True)
    df['dt'] = df['timestamp'].diff().dt.total_seconds()
    d_lat = df['latitude'].diff()
    d_lon = df['longitude'].diff()
    d_alt = df['altitude'].diff().abs() 
    
    dist_deg = np.sqrt(d_lat**2 + d_lon**2)
    implied_groundspeed = dist_deg / df['dt']
    implied_vertrate = d_alt / df['dt']
    
    is_valid = (
        (df['dt'].isna()) | 
        (df['dt'] > 3600) | 
        ((implied_groundspeed < 0.003) & (implied_vertrate < 150))
    )
    
    return df[is_valid].drop(columns=['dt'])

def detect_phases(df_flight):
    if 'on_ground' in df_flight.columns:
        df_flight = df_flight.rename(columns={'on_ground': 'onground'})

    df = clean_flight_data(df_flight)
    
    req_cols = ['vertical_rate', 'groundspeed', 'altitude', 'onground']
    if df.empty or not all(col in df.columns for col in req_cols):
        df_empty = pd.DataFrame(columns=df_flight.columns)
        df_empty['phase'] = 'UNKNOWN'
        return df_empty

    vs_smooth = df['vertical_rate'].rolling(window=40, min_periods=1).mean().fillna(df['vertical_rate'])
    
    is_taxi = (df['onground'] == True) | ((df['altitude'] < 600) & (df['groundspeed'] < 50))
    
    is_climb = (~is_taxi) & (vs_smooth > 8.3)
    is_descent = (~is_taxi) & (vs_smooth < -8.3)
    
    is_level = (~is_taxi) & (vs_smooth.between(-8.3, 8.3))
    is_below_cruise = df['altitude'] < 18000 
    is_level_off = is_level & is_below_cruise

    df['phase'] = 'CRUISE'
    df.loc[is_level_off, 'phase'] = 'LEVEL_OFF'
    df.loc[is_descent, 'phase'] = 'DESCENT'
    df.loc[is_climb, 'phase'] = 'CLIMB'
    df.loc[is_taxi, 'phase'] = 'TAXI'
    
    return df