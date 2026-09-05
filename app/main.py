"""Flight Explorer — an interactive view over the ADS-B Gold tables.

The app reads what the pipeline produced and does no analysis of its own.
Flights, phases and holds are already computed; this selects, joins and draws.
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from app import data

st.set_page_config(page_title="Flight Explorer", page_icon="✈", layout="wide")

st.markdown(
    """
    <style>
      .block-container {padding-top: 2.2rem; padding-bottom: 3rem; max-width: 1500px;}
      [data-testid="stMetricValue"] {font-size: 1.45rem;}
      [data-testid="stMetricLabel"] {font-size: .78rem; letter-spacing: .04em;
                                     text-transform: uppercase; opacity: .65;}
      h1 {font-size: 1.9rem; font-weight: 650; letter-spacing: -.01em;}
      h3 {font-size: 1.02rem; font-weight: 600; letter-spacing: .01em;
          margin-top: 1.6rem; opacity: .9;}
      .caption {font-size: .82rem; opacity: .6;}
    </style>
    """,
    unsafe_allow_html=True,
)


# --- data, loaded once -------------------------------------------------------


@st.cache_data(show_spinner="Loading flights…")
def load_flights() -> pd.DataFrame:
    return data.read_table("flights")


@st.cache_data(show_spinner=False)
def load_phases() -> pd.DataFrame:
    return data.read_table("phases")


@st.cache_data(show_spinner=False)
def load_holds() -> pd.DataFrame:
    return data.read_table("holds")


@st.cache_data(show_spinner=False)
def load_airport_metrics() -> pd.DataFrame:
    return data.read_table("airport_metrics")


@st.cache_data(show_spinner="Loading trajectory…")
def load_trajectory(flight_id: str) -> pd.DataFrame:
    return data.load_trajectory(flight_id)


def metric_row(items: list[tuple[str, str]]) -> None:
    for column, (label, value) in zip(st.columns(len(items)), items):
        column.metric(label, value)


# --- flight explorer ---------------------------------------------------------


def draw_map(points: pd.DataFrame, holds: pd.DataFrame) -> go.Figure:
    """The trajectory, coloured by detected phase, with any holds marked."""
    figure = px.scatter_map(
        points,
        lat="latitude",
        lon="longitude",
        color="phase",
        color_discrete_map=data.PHASE_COLOURS,
        category_orders={"phase": data.PHASE_ORDER},
        hover_data={
            "event_time": "|%H:%M:%S",
            "altitude_ft": ":,.0f",
            "ground_speed_kt": ":,.0f",
            "track_deg": ":.0f",
            "latitude": False,
            "longitude": False,
        },
        zoom=4,
        height=520,
    )
    figure.update_traces(marker={"size": 6})

    if not holds.empty:
        figure.add_trace(
            go.Scattermap(
                lat=holds["centroid_latitude"],
                lon=holds["centroid_longitude"],
                mode="markers",
                marker={"size": 22, "color": "#f6b93b", "opacity": 0.85},
                name="Detected hold",
                hovertext=[
                    f"Detected hold · {data.format_duration(row.duration_seconds)}"
                    f" · {row.circuits:g} circuits"
                    for row in holds.itertuples()
                ],
                hoverinfo="text",
            )
        )

    # frame the flight rather than the world
    span = max(
        points["latitude"].max() - points["latitude"].min(),
        points["longitude"].max() - points["longitude"].min(),
        0.05,
    )
    figure.update_layout(
        map={
            "style": "open-street-map",
            "center": {
                "lat": float(points["latitude"].mean()),
                "lon": float(points["longitude"].mean()),
            },
            "zoom": max(2.0, min(10.5, 7.8 - 1.45 * span ** 0.5)),
        },
        margin={"l": 0, "r": 0, "t": 0, "b": 0},
        legend={"orientation": "h", "yanchor": "bottom", "y": 0.01, "x": 0.01,
                "bgcolor": "rgba(255,255,255,.75)", "title": ""},
    )
    return figure


def draw_phase_timeline(phases: pd.DataFrame) -> go.Figure:
    figure = px.timeline(
        phases.sort_values("phase_seq"),
        x_start="start_time",
        x_end="end_time",
        y="phase",
        color="phase",
        color_discrete_map=data.PHASE_COLOURS,
        category_orders={"phase": data.PHASE_ORDER},
        hover_data={"duration_seconds": True, "n_observations": True},
        height=240,
    )
    figure.update_yaxes(title=None, categoryorder="array", categoryarray=data.PHASE_ORDER[::-1])
    figure.update_xaxes(title=None)
    figure.update_layout(
        showlegend=False, margin={"l": 0, "r": 0, "t": 10, "b": 0},
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


def draw_profile(points: pd.DataFrame) -> go.Figure:
    """Altitude and ground speed against time — the classic flight profile."""
    figure = go.Figure()
    figure.add_trace(go.Scatter(
        x=points["event_time"], y=points["altitude_ft"], name="Altitude (ft)",
        line={"color": "#2e86de", "width": 2}, fill="tozeroy",
        fillcolor="rgba(46,134,222,.12)"))
    figure.add_trace(go.Scatter(
        x=points["event_time"], y=points["ground_speed_kt"], name="Ground speed (kt)",
        line={"color": "#ee5253", "width": 1.6}, yaxis="y2"))
    figure.update_layout(
        height=260, margin={"l": 0, "r": 0, "t": 10, "b": 0},
        yaxis={"title": "ft", "rangemode": "tozero", "gridcolor": "rgba(0,0,0,.06)"},
        yaxis2={"title": "kt", "overlaying": "y", "side": "right", "showgrid": False},
        xaxis={"title": None, "gridcolor": "rgba(0,0,0,.06)"},
        legend={"orientation": "h", "y": 1.12, "x": 0},
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


def flight_explorer(flights: pd.DataFrame) -> None:
    st.sidebar.markdown("### Filters")

    selections: dict = {}
    available = flights
    for column, label in data.FILTER_COLUMNS:
        choices = data.options_for(available, column)
        picked = st.sidebar.selectbox(
            label, ["Any"] + choices, key=f"filter_{column}"
        )
        selections[column] = None if picked == "Any" else picked
        if selections[column]:
            available = available[available[column] == selections[column]]

    search = st.sidebar.text_input("Callsign or aircraft", placeholder="e.g. SWR123")
    matches = data.filter_flights(flights, selections, search)

    st.sidebar.markdown(
        f"<p class='caption'>{len(matches):,} of {len(flights):,} flights</p>",
        unsafe_allow_html=True,
    )

    if matches.empty:
        st.info("No flights match these filters. Try widening them.")
        return

    ordered = matches.sort_values("first_seen_time")
    labels = {row.flight_id: data.flight_label(row) for row in ordered.itertuples()}
    flight_id = st.selectbox(
        "Flight", ordered["flight_id"], format_func=lambda fid: labels[fid]
    )
    flight = ordered.set_index("flight_id").loc[flight_id]

    st.markdown(f"# {data.text_or(flight.callsign, flight.icao)}")
    route = " → ".join([
        data.text_or(flight.departure_airport_ident, "unknown origin"),
        data.text_or(flight.arrival_airport_ident, "unknown destination"),
    ])
    st.markdown(f"<p class='caption'>{route}</p>", unsafe_allow_html=True)

    metric_row([
        ("Airline", data.text_or(flight.airline_icao)),
        ("Aircraft", data.text_or(flight.aircraft_type)),
        ("Registration", data.text_or(flight.registration)),
        ("Duration", data.format_duration(flight.duration_seconds)),
        ("Max altitude", f"{flight.max_altitude_ft:,.0f} ft"
         if pd.notna(flight.max_altitude_ft) else "—"),
    ])
    metric_row([
        ("Departure", f"{flight.departure_time:%H:%M}"
         if pd.notna(flight.departure_time) else "not detected"),
        ("Arrival", f"{flight.arrival_time:%H:%M}"
         if pd.notna(flight.arrival_time) else "not detected"),
        ("First seen", f"{flight.first_seen_time:%H:%M}"),
        ("Observations", f"{flight.n_observations:,}"),
        ("Detected holds", f"{flight.n_detected_holds:,}"),
    ])

    points = load_trajectory(flight_id)
    flight_phases = load_phases().query("flight_id == @flight_id")
    flight_holds = load_holds().query("flight_id == @flight_id")

    if points.empty:
        st.warning("No trajectory stored for this flight.")
        return

    st.markdown("### Trajectory")
    st.plotly_chart(
        draw_map(data.label_points_with_phase(points, flight_phases), flight_holds),
        use_container_width=True,
    )

    left, right = st.columns([3, 2])
    with left:
        st.markdown("### Detected phases")
        if flight_phases.empty:
            st.caption("No phases detected for this flight.")
        else:
            st.plotly_chart(draw_phase_timeline(flight_phases), use_container_width=True)
            table = flight_phases.sort_values("phase_seq")[
                ["phase", "start_time", "end_time", "duration_seconds"]
            ].copy()
            table["duration"] = table.pop("duration_seconds").map(data.format_duration)
            table["start_time"] = table["start_time"].dt.strftime("%H:%M:%S")
            table["end_time"] = table["end_time"].dt.strftime("%H:%M:%S")
            st.dataframe(
                table.rename(columns={"phase": "Phase", "start_time": "Start",
                                      "end_time": "End", "duration": "Duration"}),
                hide_index=True, use_container_width=True,
            )
    with right:
        st.markdown("### Profile")
        st.plotly_chart(draw_profile(points), use_container_width=True)

    st.markdown("### Detected holding patterns")
    if flight_holds.empty:
        st.caption("No holding pattern detected in this trajectory.")
    else:
        st.caption(
            "Derived from the observed trajectory — sustained circling in a "
            "confined area. This is not evidence that the aircraft was "
            "instructed to hold."
        )
        holds_table = flight_holds.sort_values("hold_seq")[[
            "hold_start", "hold_end", "duration_seconds", "circuits", "span_km",
            "mean_altitude_ft", "arrival_airport_ident",
            "distance_to_arrival_airport_km",
        ]].copy()
        holds_table["duration"] = holds_table.pop("duration_seconds").map(data.format_duration)
        holds_table["hold_start"] = holds_table["hold_start"].dt.strftime("%H:%M:%S")
        holds_table["hold_end"] = holds_table["hold_end"].dt.strftime("%H:%M:%S")
        st.dataframe(
            holds_table.rename(columns={
                "hold_start": "Start", "hold_end": "End", "duration": "Duration",
                "circuits": "Circuits", "span_km": "Span (km)",
                "mean_altitude_ft": "Altitude (ft)",
                "arrival_airport_ident": "Near",
                "distance_to_arrival_airport_km": "Distance (km)"}),
            hide_index=True, use_container_width=True,
        )


# --- airport operations ------------------------------------------------------


def airport_view(flights: pd.DataFrame, metrics: pd.DataFrame) -> None:
    busiest = metrics.groupby("airport_ident")["total_operations"].sum().sort_values(
        ascending=False
    )
    labels = (
        metrics.drop_duplicates("airport_ident")
        .set_index("airport_ident")[["airport_iata", "airport_name"]]
    )

    def label(ident: str) -> str:
        row = labels.loc[ident]
        iata = data.text_or(row.airport_iata, "")
        return f"{ident} · {row.airport_name}" + (f" ({iata})" if iata else "")

    airport = st.selectbox("Airport", busiest.index, format_func=label)
    rows = metrics[metrics["airport_ident"] == airport]

    st.markdown(f"# {labels.loc[airport].airport_name}")
    st.markdown(
        f"<p class='caption'>{airport}"
        f"{' · ' + data.text_or(labels.loc[airport].airport_iata, '')}"
        f" · {len(rows)} day(s) observed</p>",
        unsafe_allow_html=True,
    )

    held = int(rows["flights_with_detected_holds"].sum())
    arrivals = int(rows["arrivals"].sum())
    metric_row([
        ("Arrivals", f"{arrivals:,}"),
        ("Departures", f"{int(rows['departures'].sum()):,}"),
        ("Total operations", f"{int(rows['total_operations'].sum()):,}"),
        ("Unique aircraft", f"{int(rows['unique_aircraft'].sum()):,}"),
        ("Flights with holds", f"{held:,}"),
    ])

    st.markdown("### Traffic by hour")
    hourly = data.hourly_traffic(flights, labels.loc[airport].airport_iata)
    figure = px.bar(
        hourly, x="hour", y=["Arrivals", "Departures"], barmode="group",
        color_discrete_map={"Arrivals": "#2e86de", "Departures": "#10ac84"},
        height=300,
    )
    figure.update_layout(
        margin={"l": 0, "r": 0, "t": 10, "b": 0}, plot_bgcolor="rgba(0,0,0,0)",
        xaxis={"title": "Hour of day (UTC)", "dtick": 2,
               "gridcolor": "rgba(0,0,0,.06)"},
        yaxis={"title": "Movements", "gridcolor": "rgba(0,0,0,.06)"},
        legend={"orientation": "h", "y": 1.15, "x": 0, "title": ""},
    )
    st.plotly_chart(figure, use_container_width=True)

    left, right = st.columns(2)
    with left:
        st.markdown("### Operations by day")
        daily = rows.sort_values("operations_date")[
            ["operations_date", "arrivals", "departures"]
        ]
        st.dataframe(
            daily.rename(columns={"operations_date": "Date", "arrivals": "Arrivals",
                                  "departures": "Departures"}),
            hide_index=True, use_container_width=True,
        )
    with right:
        st.markdown("### Detected holding")
        if held == 0:
            st.caption("No holding patterns detected at this airport.")
        else:
            rate = held / arrivals if arrivals else None
            metric_row([
                ("Flights with holds", f"{held:,}"),
                ("Share of arrivals", f"{rate:.1%}" if rate else "—"),
                ("Average hold", data.format_duration(
                    rows["avg_hold_duration_seconds"].mean())),
            ])
            st.caption(
                "Detected from observed trajectories. High rates at "
                "general-aviation fields usually reflect circuit training "
                "rather than delay."
            )


# --- app ---------------------------------------------------------------------

flights = load_flights()
explorer_tab, airport_tab = st.tabs(["Flight Explorer", "Airport Operations"])

with explorer_tab:
    flight_explorer(flights)

with airport_tab:
    airport_view(flights, load_airport_metrics())

st.sidebar.markdown(
    "<p class='caption'>Flights, phases and holding patterns are inferred from "
    "ADS-B observations. They are not official airline, airport or ATC "
    "records.</p>",
    unsafe_allow_html=True,
)
