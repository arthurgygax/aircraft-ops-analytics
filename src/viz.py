import plotly.express as px
import plotly.graph_objects as go
import os
import pandas as pd
import numpy as np

def get_mapbox_token():
    token = os.getenv("MAPBOX_TOKEN")
    if not token: return ""
    return token

def calculate_segments(df):
    """Adds segment_id to handle gaps in data."""
    df = df.copy()
    df.sort_values(['callsign', 'timestamp'], inplace=True)
    
    # Calculate gaps
    df['time_diff'] = df.groupby('callsign')['timestamp'].diff().dt.total_seconds()
    
    # Break segment if gap > 5 mins (300s)
    GAP_THRESHOLD = 90000
    
    condition = (
        (df['time_diff'] > GAP_THRESHOLD) | 
        (df['callsign'] != df['callsign'].shift()) |
        (df['phase'] != df['phase'].shift())
    )
    df['segment_id'] = condition.astype(int).cumsum()
    return df

def draw_flight_map(df, selected_callsign=None):
    fig = go.Figure()
    
    # 1. Clean 0,0 points and calc segments
    df = df[(df['latitude'] != 0) | (df['longitude'] != 0)].copy()
    df = calculate_segments(df)

    color_map = {
        "TAXI": "#F1C40F", "CLIMB": "#2ECC71", "LEVEL_OFF": "#E74C3C",
        "CRUISE": "#3498DB", "DESCENT": "#E67E22"
    }

    # 2. Draw Background (Optimized: One trace per phase)
    bg_data = df[df['callsign'] != selected_callsign] if selected_callsign else df
    
    for phase, color in color_map.items():
        phase_data = bg_data[bg_data['phase'] == phase]
        if phase_data.empty: continue

        lats, lons, texts = [], [], []
        
        # We must insert None between segments to break lines
        for _, seg in phase_data.groupby('segment_id'):
            lats.extend(seg['latitude'].tolist() + [None])
            lons.extend(seg['longitude'].tolist() + [None])
            # Hover text needs to match length
            texts.extend([seg['callsign'].iloc[0]] * len(seg) + [None])

        mode = "lines+markers" if phase == "TAXI" else "lines"
        width = 1 if phase == "TAXI" else 0.5
        marker = dict(size=2, color=color) if phase == "TAXI" else None
        
        fig.add_trace(go.Scattermapbox(
            mode=mode, lat=lats, lon=lons,
            line=dict(width=width, color=color),
            marker=marker,
            opacity=0.3 if selected_callsign else 0.6,
            hovertext=texts, hoverinfo='text',
            name=phase, showlegend=True
        ))

    # 3. Draw Foreground (Selected)
    if selected_callsign:
        fg_data = df[df['callsign'] == selected_callsign]
        for _, segment in fg_data.groupby('segment_id'):
            phase = segment.iloc[0]['phase']
            hover_text = [
                f"<b>{row.callsign}</b><br>{phase}<br>{int(row.altitude)}ft<br>{int(row.groundspeed)}kts" 
                for _, row in segment.iterrows()
            ]
            fig.add_trace(go.Scattermapbox(
                mode="lines+markers",
                lat=segment['latitude'], lon=segment['longitude'],
                line=dict(width=3, color=color_map.get(phase, 'white')),
                marker=dict(size=4, color=color_map.get(phase, 'white')),
                opacity=1.0, hovertext=hover_text, hoverinfo="text", showlegend=False
            ))

    fig.update_layout(
        mapbox_style="carto-darkmatter", mapbox_accesstoken=get_mapbox_token(),
        margin={"r":0,"t":0,"l":0,"b":0}, uirevision='constant', height=700,
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01, bgcolor="rgba(0,0,0,0.5)", font=dict(color="white"))
    )
    return fig

def draw_altitude_profile(df):
    if 'segment_id' not in df.columns:
        df = calculate_segments(df)

    # FIX: Calculate time relative to FLIGHT start, not segment start
    starts = df.groupby('callsign')['timestamp'].transform('min')
    df['minutes_since_start'] = (df['timestamp'] - starts).dt.total_seconds() / 60
    
    # Filter first 10 mins
    df_10min = df[df['minutes_since_start'] <= 10]
    
    if df_10min.empty:
        return go.Figure().update_layout(title="No data for first 10 mins")

    fig = px.line(
        df_10min, x="minutes_since_start", y="altitude", color="callsign",
        # FIX: Group by segment_id to break lines at gaps
        line_group="segment_id",
        title="Altitude Profile (First 10 Mins)",
        labels={"minutes_since_start": "Time (min)", "altitude": "Altitude (ft)"}
    )
    
    fig.update_traces(line=dict(width=1), opacity=0.5)
    fig.update_layout(template="plotly_dark", showlegend=False, hovermode="x unified", height=400)
    return fig

def draw_stats_charts(df):
    df_no_nan = df.dropna(subset=['timestamp', 'callsign', 'phase']).copy()
    
    if not df_no_nan.empty:
        if 'segment_id' not in df_no_nan.columns:
            df_no_nan = calculate_segments(df_no_nan)

        taxi_times = df_no_nan[df_no_nan['phase'] == 'TAXI'].groupby('segment_id')['timestamp'].apply(
            lambda x: (x.max() - x.min()).total_seconds()/60
        ).reset_index(name='minutes')
        taxi_times = taxi_times[(taxi_times['minutes'] > 1) & (taxi_times['minutes'] < 120)]

        fig_taxi = px.histogram(taxi_times, x="minutes", nbins=30, title="Taxi-Out Duration (Min)", color_discrete_sequence=['#F1C40F'])
        fig_taxi.update_layout(template="plotly_dark")

        lo_data = df_no_nan[df_no_nan['phase'] == 'LEVEL_OFF'].groupby('callsign').size().reset_index(name='seconds')
        lo_data = lo_data[lo_data['seconds'] > 30].sort_values('seconds', ascending=False).head(15)
        
        fig_lo = px.bar(lo_data, x='callsign', y='seconds', title="Longest Level-Offs (Seconds)", color='seconds', color_continuous_scale='Reds')
        fig_lo.update_layout(template="plotly_dark")
    else:
        fig_taxi, fig_lo = go.Figure(), go.Figure()
        
    return fig_taxi, fig_lo