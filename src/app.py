import streamlit as st
import os
import pandas as pd
from logic import load_data, detect_phases, classify_flight_direction
from viz import draw_flight_map, draw_stats_charts, draw_altitude_profile

st.set_page_config(page_title="Aircraft Ops Analytics", layout="wide", page_icon="✈️")

PROCESSED_FILE = os.path.join("data", "processed", "master_flight_data.parquet")
if not os.path.exists(PROCESSED_FILE):
    st.error("Processed data file not found.")
    st.stop()

# --- FILTERS ---
st.sidebar.header("Filter Settings")
st.sidebar.subheader("1. Airport")
st.sidebar.markdown("**LSZH - Zurich**")
zrh_bbox = (47.3, 8.3, 47.7, 8.8)

st.sidebar.subheader("2. Flight Direction")
direction_filter = st.sidebar.radio("Show:", ["Departures", "Arrivals", "Both"], index=0)

st.sidebar.subheader("3. Airlines")
@st.cache_data
def get_available_airlines():
    try:
        return sorted(pd.read_parquet(PROCESSED_FILE, columns=['airline'])['airline'].unique())
    except Exception:
        return []

available_airlines = get_available_airlines()
default_selection = ['SWR'] if 'SWR' in available_airlines else ([available_airlines[0]] if available_airlines else [])
selected_airlines = st.sidebar.multiselect("Select Airlines", available_airlines, default=default_selection)

# --- DATA LOADING ---
@st.cache_data
def load_filtered_data(airlines):
    if not airlines: return pd.DataFrame()
    parquet_filters = [('airline', 'in', airlines)]
    return load_data(PROCESSED_FILE, filters=parquet_filters)

filtered_data = load_filtered_data(selected_airlines)

if filtered_data.empty:
    st.warning("No data found.")
    st.stop()

# --- PROCESSING ---
st.sidebar.write(f"Processing {len(filtered_data['callsign'].unique())} flights...")

def flight_direction_filter(flight_group):
    direction = classify_flight_direction(flight_group, zrh_bbox)
    if direction_filter == "Both" and direction in ["DEPARTURE", "ARRIVAL"]: return True
    if direction_filter == "Departures" and direction == "DEPARTURE": return True
    if direction_filter == "Arrivals" and direction == "ARRIVAL": return True
    return False

direction_filtered_data = filtered_data.groupby('callsign').filter(flight_direction_filter)

if direction_filtered_data.empty:
    st.warning("No flights matched the direction filter.")
    st.stop()

final_df = direction_filtered_data.groupby('callsign').apply(detect_phases, include_groups=False).reset_index()
final_df['info_link'] = "https://flightaware.com/live/flight/" + final_df['callsign']

# --- DASHBOARD LAYOUT ---
st.title(f"Aircraft Analysis: {direction_filter} at LSZH")

col_map, col_controls = st.columns([3, 1])

with col_controls:
    st.markdown("### Flight Inspector")
    flight_options = sorted(final_df['callsign'].unique())
    selected_callsign = st.selectbox("Select Flight", ["None"] + flight_options)

    if selected_callsign != "None":
        f_data = final_df[final_df['callsign'] == selected_callsign]
        if not f_data.empty:
            st.markdown(f"**  [View on FlightAware]({f_data.iloc[0]['info_link']})**")
            curr_phase = f_data.iloc[-1]['phase']
            max_alt = int(f_data['altitude'].max())
            st.metric("Current Phase", curr_phase)
            st.metric("Max Altitude", f"{max_alt} ft")

with col_map:
    highlight = selected_callsign if selected_callsign != "None" else None
    fig_map = draw_flight_map(final_df, selected_callsign=highlight)
    st.plotly_chart(fig_map, use_container_width=True, config={'scrollZoom': True, 'displayModeBar': True})

st.markdown("---")

# --- CHARTS ---
st.markdown("### Flight Profiles (First 10 mins)")
fig_alt = draw_altitude_profile(final_df)
st.plotly_chart(fig_alt, use_container_width=True)

st.markdown("---")
st.markdown("### Efficiency Metrics")
fig_taxi, fig_lo = draw_stats_charts(final_df)
c1, c2 = st.columns(2)
with c1: st.plotly_chart(fig_taxi, use_container_width=True)
with c2: st.plotly_chart(fig_lo, use_container_width=True)