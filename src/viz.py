import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import pandas as pd
import numpy as np

def get_mapbox_token():
    token = os.getenv("MAPBOX_TOKEN")
    if not token: return ""
    return token

def calculate_segments(df):
    df = df.copy()
    df.sort_values(['callsign', 'timestamp'], inplace=True)
    df['time_diff'] = df.groupby('callsign')['timestamp'].diff().dt.total_seconds()
    GAP_THRESHOLD = 300
    condition = (
        (df['time_diff'] > GAP_THRESHOLD) | 
        (df['callsign'] != df['callsign'].shift()) |
        (df['phase'] != df['phase'].shift())
    )
    df['segment_id'] = condition.astype(int).cumsum()
    return df

def draw_flight_map(df, selected_callsign=None):
    fig = go.Figure()
    # Filter valid coordinates
    df = df[(df['latitude'] != 0) | (df['longitude'] != 0)].copy()
    df = calculate_segments(df)

    color_map = {
        "TAXI": "#F1C40F", "CLIMB": "#2ECC71", "LEVEL_OFF": "#E74C3C",
        "CRUISE": "#3498DB", "DESCENT": "#E67E22"
    }

    # 1. Draw Background Flights (or all if none selected)
    bg_data = df[df['callsign'] != selected_callsign] if selected_callsign else df
    
    for phase, color in color_map.items():
        phase_data = bg_data[bg_data['phase'] == phase]
        if phase_data.empty: continue

        lats, lons, texts, custom_data = [], [], [], []
        for _, seg in phase_data.groupby('segment_id'):
            lats.extend(seg['latitude'].tolist() + [None])
            lons.extend(seg['longitude'].tolist() + [None])
            
            # Prepare hover and click data
            c_sign = seg['callsign'].iloc[0]
            texts.extend([f"{c_sign} ({phase})"] * len(seg) + [None])
            custom_data.extend([c_sign] * len(seg) + [None])

        # --- THE TRICK: Invisible Markers ---
        # We use 'lines+markers' so there are points to click.
        # If it's TAXI, we show them. If flying, we hide them (opacity=0).
        is_taxi = (phase == "TAXI")
        
        fig.add_trace(go.Scattermapbox(
            mode="lines+markers", 
            lat=lats, lon=lons,
            line=dict(width=1 if is_taxi else 1, color=color),
            marker=dict(
                size=4 if is_taxi else 8, # Larger hit target for invisible points
                color=color, 
                opacity=0.6 if is_taxi else 0 # Invisible if not taxi
            ),
            opacity=0.5 if selected_callsign else 0.8,
            hovertext=texts, hoverinfo='text',
            customdata=custom_data, # Crucial: This passes the ID to Streamlit
            showlegend=False, name=phase
        ))

    # 2. Draw Highlighted Flight
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
                line=dict(width=4, color=color_map.get(phase, 'white')),
                marker=dict(size=6, color=color_map.get(phase, 'white')),
                opacity=1.0, hovertext=hover_text, hoverinfo="text", showlegend=False
            ))

    # 3. Legend Hack
    for phase, color in color_map.items():
        fig.add_trace(go.Scattermapbox(
            mode='lines', lat=[None], lon=[None], name=phase,
            line=dict(color=color, width=4), showlegend=True, hoverinfo='skip'
        ))

    fig.update_layout(
        mapbox_style="carto-darkmatter", mapbox_accesstoken=get_mapbox_token(),
        margin={"r":0,"t":0,"l":0,"b":0}, uirevision='constant', height=700,
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01, bgcolor="rgba(0,0,0,0.8)", font=dict(color="white"))
    )
    return fig

def draw_hourly_traffic(df):
    df = df.copy()
    df['hour'] = df['timestamp'].dt.hour
    hourly_counts = df.groupby('hour')['callsign'].nunique().reset_index(name='flight_count')
    all_hours = pd.DataFrame({'hour': range(24)})
    hourly_counts = all_hours.merge(hourly_counts, on='hour', how='left').fillna(0)
    
    fig = px.bar(
        hourly_counts, x='hour', y='flight_count',
        title="Hourly Traffic Volume",
        labels={'hour': 'Hour of Day (UTC)', 'flight_count': 'Unique Flights'},
        color='flight_count', color_continuous_scale='Blues'
    )
    fig.update_layout(template="plotly_dark", xaxis=dict(dtick=1))
    return fig

def draw_ground_radar(df):
    taxi_data = df[df['phase'] == 'TAXI'].copy()
    if taxi_data.empty: return go.Figure().update_layout(template="plotly_dark", title="No Ground Data")

    fig = px.density_mapbox(
        taxi_data, lat='latitude', lon='longitude', z=taxi_data['groundspeed'] * 0 + 1,
        radius=5, center=dict(lat=47.458, lon=8.555), zoom=12,
        mapbox_style="carto-darkmatter", title="Airport Ground Congestion"
    )
    fig.update_layout(template="plotly_dark", margin={"r":0,"t":40,"l":0,"b":0}, height=500, uirevision='constant')
    return fig

def draw_holding_heatmap(df):
    hold_data = df[df['phase'] == 'LEVEL_OFF'].copy()
    if hold_data.empty: return go.Figure().update_layout(template="plotly_dark", title="No Level-Off Data")

    center_lat = hold_data['latitude'].mean()
    center_lon = hold_data['longitude'].mean()

    fig = px.density_mapbox(
        hold_data, lat='latitude', lon='longitude', 
        z=hold_data['altitude'] * 0 + 1,
        radius=10, center=dict(lat=center_lat, lon=center_lon), zoom=8,
        mapbox_style="carto-darkmatter", title="Airspace Congestion (Holding Patterns)"
    )
    fig.update_layout(template="plotly_dark", margin={"r":0,"t":40,"l":0,"b":0}, height=500, uirevision='constant')
    return fig

def draw_airline_efficiency(df):
    taxi_df = df[df['phase'] == 'TAXI'].copy()
    if taxi_df.empty: return go.Figure().update_layout(template="plotly_dark", title="Not enough data")

    stats = taxi_df.groupby(['airline', 'callsign'])['timestamp'].apply(
        lambda x: (x.max() - x.min()).total_seconds()/60
    ).reset_index(name='taxi_min')
    
    airline_stats = stats.groupby('airline').agg(
        avg_taxi=('taxi_min', 'mean'),
        flight_count=('callsign', 'nunique')
    ).reset_index()
    
    airline_stats = airline_stats[airline_stats['flight_count'] > 2]

    fig = px.scatter(
        airline_stats, x='flight_count', y='avg_taxi',
        size='flight_count', color='avg_taxi',
        hover_name='airline', title="Airline Efficiency (Taxi Time vs Volume)",
        labels={'flight_count': 'Number of Flights', 'avg_taxi': 'Avg Taxi Time (min)'},
        color_continuous_scale='RdYlGn_r' 
    )
    fig.update_layout(template="plotly_dark")
    return fig

def draw_stats_charts(df):
    df_no_nan = df.dropna(subset=['timestamp', 'callsign', 'phase']).copy()
    
    if not df_no_nan.empty:
        if 'segment_id' not in df_no_nan.columns:
            df_no_nan = calculate_segments(df_no_nan)

        taxi_df = df_no_nan[df_no_nan['phase'] == 'TAXI']
        if not taxi_df.empty:
            taxi_times = taxi_df.groupby('segment_id')['timestamp'].apply(
                lambda x: (x.max() - x.min()).total_seconds()/60
            ).reset_index(name='minutes')
            taxi_times = taxi_times[(taxi_times['minutes'] > 1) & (taxi_times['minutes'] < 120)]
            
            fig_taxi = px.histogram(taxi_times, x="minutes", nbins=30, title="Taxi-Out Duration (Min)", color_discrete_sequence=['#F1C40F'])
            fig_taxi.update_layout(template="plotly_dark")
        else:
            fig_taxi = go.Figure().update_layout(template="plotly_dark", title="No Taxi Data")

        lo_df = df_no_nan[df_no_nan['phase'] == 'LEVEL_OFF']
        if not lo_df.empty:
            lo_data = lo_df.groupby('callsign').size().reset_index(name='seconds')
            lo_data = lo_data[lo_data['seconds'] > 30].sort_values('seconds', ascending=False).head(15)
            fig_lo = px.bar(lo_data, x='callsign', y='seconds', title="Longest Level-Offs (Seconds)", color='seconds', color_continuous_scale='Reds')
            fig_lo.update_layout(template="plotly_dark")
        else:
            fig_lo = go.Figure().update_layout(template="plotly_dark", title="No Level-Off Data")
            
    else:
        fig_taxi, fig_lo = go.Figure(), go.Figure()
        
    return fig_taxi, fig_lo

def draw_selected_flight_profile(df):
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Scatter(x=df['timestamp'], y=df['altitude'], name="Altitude (ft)", 
                   line=dict(color="#3498DB", width=2), fill='tozeroy', fillcolor="rgba(52, 152, 219, 0.2)"),
        secondary_y=False,
    )

    fig.add_trace(
        go.Scatter(x=df['timestamp'], y=df['groundspeed'], name="Speed (kts)", 
                   line=dict(color="#E74C3C", width=2, dash='dot')),
        secondary_y=True,
    )

    fig.update_layout(
        title_text=f"Flight Profile: {df['callsign'].iloc[0]}",
        template="plotly_dark", hovermode="x unified", height=350,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    fig.update_yaxes(title_text="Altitude (ft)", secondary_y=False, gridcolor='rgba(255,255,255,0.1)')
    fig.update_yaxes(title_text="Groundspeed (kts)", secondary_y=True, showgrid=False)
    fig.update_xaxes(title_text="Time (UTC)", gridcolor='rgba(255,255,255,0.1)')

    return fig

def draw_selected_flight_profile(df):
    # Create a dual-axis chart
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Trace 1: Altitude (Blue Area)
    fig.add_trace(
        go.Scatter(
            x=df['timestamp'], 
            y=df['altitude'], 
            name="Altitude (ft)", 
            mode='lines',
            line=dict(color="#3498DB", width=2), 
            fill='tozeroy', 
            fillcolor="rgba(52, 152, 219, 0.2)"
        ),
        secondary_y=False,
    )

    # Trace 2: Groundspeed (Red Dashed Line)
    fig.add_trace(
        go.Scatter(
            x=df['timestamp'], 
            y=df['groundspeed'], 
            name="Speed (kts)", 
            mode='lines',
            line=dict(color="#E74C3C", width=2, dash='dot')
        ),
        secondary_y=True,
    )

    fig.update_layout(
        title_text=f"Flight Profile: {df['callsign'].iloc[0]}",
        template="plotly_dark", 
        hovermode="x unified", 
        height=350,
        margin=dict(l=10, r=10, t=40, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    fig.update_yaxes(title_text="Altitude (ft)", secondary_y=False, gridcolor='rgba(255,255,255,0.1)')
    fig.update_yaxes(title_text="Speed (kts)", secondary_y=True, showgrid=False)
    fig.update_xaxes(title_text="Time (UTC)", gridcolor='rgba(255,255,255,0.1)')

    return fig