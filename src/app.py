import streamlit as st
import os
import pandas as pd
from logic import load_data, detect_phases, classify_flight_direction
from viz import draw_flight_map, draw_stats_charts, draw_hourly_traffic, draw_ground_radar, draw_holding_heatmap, draw_airline_efficiency, draw_selected_flight_profile

st.set_page_config(page_title="Aircraft Ops Analytics", layout="wide", page_icon="✈️")

PROCESSED_FILE = os.path.join("data", "processed", "master_flight_data.parquet")
if not os.path.exists(PROCESSED_FILE):
    st.error("Processed data file not found.")
    st.stop()

if "selected_callsign" not in st.session_state:
    st.session_state.selected_callsign = "None"

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

# 1. KPI ROW
total_flights = len(final_df['callsign'].unique())
level_off_pct = (len(final_df[final_df['phase'] == 'LEVEL_OFF']['callsign'].unique()) / total_flights * 100) if total_flights > 0 else 0

kpi1, kpi2 = st.columns(2)
kpi1.metric("Total Flights", total_flights)
kpi2.metric("Flights with Holds/Level-Offs", f"{level_off_pct:.1f}%")

st.markdown("---")

col_map, col_controls = st.columns([3, 1])

    
# --- FLIGHT INSPECTOR ---
with col_controls:
    st.markdown("### Inspector")
    
    flight_options = ["None"] + sorted(final_df['callsign'].unique())
    
    # 1. Ensure state exists
    if "selected_callsign" not in st.session_state:
        st.session_state.selected_callsign = "None"

    # 2. Callback to sync Selectbox -> State
    def update_selection():
        st.session_state.selected_callsign = st.session_state.flight_selector

    # 3. Calculate current index for the Selectbox
    # We drive the selectbox via 'index', not 'value', to allow bidirectional updates
    try:
        current_idx = flight_options.index(st.session_state.selected_callsign)
    except ValueError:
        current_idx = 0

    # 4. The Selectbox
    # Note: We use a key 'flight_selector' to avoid locking 'selected_callsign'
    selected_callsign = st.selectbox(
        "Select Flight", 
        flight_options, 
        index=current_idx,
        key="flight_selector",
        on_change=update_selection
    )

    if st.session_state.selected_callsign != "None":
        
        # 1. Filter the specific flight data
        f_data = final_df[final_df['callsign'] == st.session_state.selected_callsign]
        
        if not f_data.empty:
            # Sort by time to ensure the line chart draws correctly
            f_data = f_data.sort_values('timestamp')
            
            st.divider() # Adds a visual separator
            
            # 2. Display Metadata
            flight_link = f_data.iloc[0]['info_link']
            curr_phase = f_data.iloc[-1]['phase']
            max_alt = int(f_data['altitude'].max())
            
            st.markdown(f"** Flight: {st.session_state.selected_callsign}**")
            st.markdown(f" [View on FlightAware]({flight_link})")
            
            m1, m2 = st.columns(2)
            m1.metric("Current Phase", curr_phase)
            m2.metric("Max Altitude", f"{max_alt} ft")
            
            # 3. Draw the Altitude/Speed Chart
            st.write("###### Altitude & Speed Profile")
            fig_profile = draw_selected_flight_profile(f_data)
            
            st.plotly_chart(
                fig_profile, 
                use_container_width=True, 
                config={'displayModeBar': False}
            )

# --- MAIN MAP ---
with col_map:
    # Use session state for highlighting
    highlight = st.session_state.selected_callsign if st.session_state.selected_callsign != "None" else None
    
    fig_map = draw_flight_map(final_df, selected_callsign=highlight)
    
    # Render Map
    event = st.plotly_chart(
        fig_map, 
        use_container_width=True,
        on_select="rerun",       # Rerun script on click
        selection_mode="points", # Detects clicks on our invisible markers
        config={'scrollZoom': True, 'displayModeBar': False}
    )

    # Handle Click
    if event and event.get("selection") and event["selection"].get("points"):
        # Get customdata from the clicked point
        clicked_point = event["selection"]["points"][0]
        clicked_callsign = clicked_point.get("customdata")
        
        # Unwrap list if necessary (Plotly sometimes returns list for customdata)
        if isinstance(clicked_callsign, list):
            clicked_callsign = clicked_callsign[0]
            
        # Update State & Rerun if it's a new selection
        if clicked_callsign and clicked_callsign != st.session_state.selected_callsign:
            st.session_state.selected_callsign = clicked_callsign
            st.rerun() # Force reload so the Selectbox at the top updates!

st.markdown("---")

# 2. OPERATIONAL EFFICIENCY
st.markdown("### Operational Efficiency")
c1, c2 = st.columns(2)
with c1:
    st.markdown("**Hourly Throughput**")
    fig_hourly = draw_hourly_traffic(final_df)
    st.plotly_chart(fig_hourly, use_container_width=True)
with c2:
    st.markdown("**Airline Performance (Taxi vs Volume)**")
    fig_airline = draw_airline_efficiency(final_df)
    st.plotly_chart(fig_airline, use_container_width=True)

st.markdown("---")

# 3. SPATIAL ANALYSIS
st.markdown("### Spatial Analysis")
c3, c4 = st.columns(2)
with c3:
    st.markdown("**Ground Congestion (Taxi)**")
    fig_radar = draw_ground_radar(final_df)
    st.plotly_chart(fig_radar, use_container_width=True, config={'scrollZoom': True})
with c4:
    st.markdown("**Airspace Congestion (Holding Patterns)**")
    fig_hold = draw_holding_heatmap(final_df)
    st.plotly_chart(fig_hold, use_container_width=True, config={'scrollZoom': True})

st.markdown("---")
st.markdown("**Metrics Details**")
fig_taxi, fig_lo = draw_stats_charts(final_df)
c5, c6 = st.columns(2)
with c5: st.plotly_chart(fig_taxi, use_container_width=True)
with c6: st.plotly_chart(fig_lo, use_container_width=True)