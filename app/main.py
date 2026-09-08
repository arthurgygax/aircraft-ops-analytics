"""Flight Explorer — interactive exploration of the 7-day ZRH/DUS study period.

The app reads what the pipeline produced and does no analysis of its own.
Flights, phases and holds are already computed; this selects, joins and draws.

WHAT IT LOADS, AND WHEN
    The four small tables are read once and cached. The 3.3M-point trajectory
    table is never read whole: points are fetched only for the flights the user
    selected, and always with their dates so six of the seven day-partitions
    are pruned. Selecting nothing costs no trajectory read at all.
"""

from __future__ import annotations

import math

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from app import data

st.set_page_config(page_title="Flight Explorer", page_icon="✈", layout="wide")

# How many trajectories may share the map. Three is where colour stops being
# able to tell them apart -- see the palette note in app.data -- so beyond that
# the extra tracks go neutral and rely on their labels.
DEFAULT_MAP_FLIGHTS = 3
MAX_MAP_FLIGHTS = 8


def is_dark() -> bool:
    try:
        return st.context.theme.type == "dark"
    except Exception:
        return False


DARK = is_dark()
COLOURS = data.palette(DARK)

st.markdown(
    """
    <style>
      /* Type: system stack, tracking tightened as size grows, loosened for
         the small uppercase labels. One value for every size would be wrong
         somewhere. */
      html, body, [class*="st-"] {
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui,
                     "Helvetica Neue", Arial, sans-serif;
        font-feature-settings: "tnum" 1;   /* figures line up in tables */
      }
      .block-container {padding: 2rem 2.5rem 4rem; max-width: 1560px;}

      h1 {font-size: 1.75rem; font-weight: 620; letter-spacing: -.021em;
          line-height: 1.15; margin-bottom: .1rem;}
      h2 {font-size: 1.15rem; font-weight: 600; letter-spacing: -.012em;
          margin: 2rem 0 .35rem;}
      h3 {font-size: .93rem; font-weight: 600; letter-spacing: -.004em;
          margin: 1.35rem 0 .3rem;}

      .lede {font-size: .9rem; line-height: 1.5; opacity: .68; margin: 0 0 .2rem;}
      .note {font-size: .8rem; line-height: 1.45; opacity: .58;}
      .eyebrow {font-size: .7rem; letter-spacing: .07em; text-transform: uppercase;
                opacity: .5; font-weight: 600;}

      /* KPI cards: a hairline and a whisper of fill, no shadow. The number is
         the thing; the frame should be almost invisible. */
      [data-testid="stMetric"] {
        background: rgba(140,140,140,.055);
        border: 1px solid rgba(140,140,140,.16);
        border-radius: 12px; padding: .8rem .95rem .7rem;
      }
      [data-testid="stMetricValue"] {font-size: 1.4rem; font-weight: 600;
                                     letter-spacing: -.02em;}
      [data-testid="stMetricLabel"] {font-size: .7rem; letter-spacing: .06em;
                                     text-transform: uppercase; opacity: .62;}

      /* Sidebar filter groups read as groups */
      section[data-testid="stSidebar"] .stSelectbox label,
      section[data-testid="stSidebar"] .stTextInput label,
      section[data-testid="stSidebar"] .stSlider label {
        font-size: .78rem; font-weight: 550; opacity: .8;
      }
      section[data-testid="stSidebar"] hr {margin: 1.1rem 0 .9rem; opacity: .35;}

      div[data-testid="stDataFrame"] {border-radius: 10px;}
      hr {margin: 1.6rem 0;}

      /* Reduced motion means a gentler equivalent, not a broken UI. */
      @media (prefers-reduced-motion: reduce) {
        *, *::before, *::after {
          animation-duration: .001ms !important; animation-iteration-count: 1 !important;
          transition-duration: .001ms !important; scroll-behavior: auto !important;
        }
      }
      @media (prefers-contrast: more) {
        [data-testid="stMetric"] {background: transparent;
                                  border-color: rgba(120,120,120,.65);}
      }
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


@st.cache_data(show_spinner="Loading trajectories…", max_entries=32)
def load_trajectories(flight_ids: tuple[str, ...], dates: tuple) -> pd.DataFrame:
    """Cached per selection, not for the whole table.

    ``max_entries`` bounds it: a few dozen selections of a handful of flights
    is tens of MB, where the full point table is 3.3M rows. Arguments are
    tuples because a cache key has to be hashable.
    """
    return data.load_trajectories(list(flight_ids), list(dates))


def metric_row(items: list[tuple[str, str]]) -> None:
    for column, (label, value) in zip(st.columns(len(items)), items):
        column.metric(label, value)


def chart_layout(figure: go.Figure, height: int, legend: bool = True) -> go.Figure:
    """One place for the recessive chrome every chart shares."""
    figure.update_layout(
        height=height,
        margin={"l": 0, "r": 0, "t": 8, "b": 0},
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font={"color": COLOURS["ink"], "size": 12},
        showlegend=legend,
        legend={"orientation": "h", "y": 1.14, "x": 0, "title": "",
                "bgcolor": "rgba(0,0,0,0)"},
        hoverlabel={"font_size": 12},
    )
    figure.update_xaxes(gridcolor=COLOURS["grid"], zeroline=False,
                        linecolor=COLOURS["grid"])
    figure.update_yaxes(gridcolor=COLOURS["grid"], zeroline=False,
                        linecolor=COLOURS["grid"])
    return figure


# --- the map -----------------------------------------------------------------


def _hover(points: pd.DataFrame) -> tuple:
    custom = points[["event_time", "altitude_ft", "ground_speed_kt", "track_deg"]]
    template = (
        "%{customdata[0]|%H:%M:%S}<br>"
        "%{customdata[1]:,.0f} ft · %{customdata[2]:,.0f} kt · %{customdata[3]:.0f}°"
        "<extra>%{fullData.name}</extra>"
    )
    return custom.to_numpy(), template


def add_airports(figure: go.Figure, metrics: pd.DataFrame) -> None:
    """The two study airports, so a trajectory has something to be relative to."""
    sites = metrics.drop_duplicates("airport_ident")
    figure.add_trace(go.Scattermap(
        lat=sites["airport_latitude"], lon=sites["airport_longitude"],
        mode="markers+text",
        marker={"size": 11, "color": COLOURS["ink_muted"]},
        text=[data.STUDY_AIRPORTS.get(i, i) for i in sites["airport_ident"]],
        textposition="top right",
        textfont={"size": 11, "color": COLOURS["ink_muted"]},
        hovertext=[f"{r.airport_name} ({r.airport_iata})" for r in sites.itertuples()],
        hoverinfo="text",
        name="Airport", showlegend=False,
    ))


def draw_phase_map(points: pd.DataFrame, holds: pd.DataFrame,
                   metrics: pd.DataFrame) -> go.Figure:
    """One flight, coloured by its detected phases, with any hold highlighted."""
    figure = go.Figure()
    for phase in data.PHASE_ORDER:
        segment = points[points["phase"] == phase]
        if segment.empty:
            continue
        custom, template = _hover(segment)
        figure.add_trace(go.Scattermap(
            lat=segment["latitude"], lon=segment["longitude"],
            mode="markers", marker={"size": 5, "color": COLOURS["phases"][phase]},
            name=data.PHASE_LABELS[phase],
            customdata=custom, hovertemplate=template,
        ))

    held = data.points_in_holds(points, holds)
    if not held.empty:
        figure.add_trace(go.Scattermap(
            lat=held["latitude"], lon=held["longitude"], mode="markers",
            marker={"size": 9, "color": COLOURS["hold"], "opacity": .95},
            name="Detected holding pattern",
            hovertext=["ADS-B-derived detected holding pattern"] * len(held),
            hoverinfo="text",
        ))

    _add_endpoints(figure, points, COLOURS["ink_muted"], label=None)
    add_airports(figure, metrics)
    return _frame(figure, points)


def draw_flights_map(tracks: pd.DataFrame, legend: dict[str, str],
                     short: dict[str, str], metrics: pd.DataFrame) -> go.Figure:
    """Several flights, one colour each while the validated hues last."""
    figure = go.Figure()
    for index, (flight_id, points) in enumerate(tracks.groupby("flight_id", sort=False)):
        colour = data.series_colour(index, DARK)
        custom, template = _hover(points)
        figure.add_trace(go.Scattermap(
            lat=points["latitude"], lon=points["longitude"],
            mode="lines", line={"width": 2.4, "color": colour},
            name=legend.get(flight_id, flight_id),
            customdata=custom, hovertemplate=template,
        ))
        _add_endpoints(figure, points, colour, label=short.get(flight_id))
    add_airports(figure, metrics)
    return _frame(figure, tracks)


def _add_endpoints(figure: go.Figure, points: pd.DataFrame, colour: str,
                   label: str | None) -> None:
    """A ring where tracking started, a filled dot where it stopped.

    Direction is the one thing a line on a map cannot show by itself, and the
    two ends are read at a glance where an arrowhead mid-track would not be.
    """
    first, last = points.iloc[0], points.iloc[-1]
    figure.add_trace(go.Scattermap(
        lat=[first["latitude"]], lon=[first["longitude"]], mode="markers",
        marker={"size": 13, "color": "rgba(0,0,0,0)"},
        hovertext=[f"First seen {pd.Timestamp(first['event_time']):%H:%M}"],
        hoverinfo="text", showlegend=False, name="start",
    ))
    figure.add_trace(go.Scattermap(
        lat=[first["latitude"]], lon=[first["longitude"]], mode="markers",
        marker={"size": 7, "color": colour, "opacity": .55},
        hoverinfo="skip", showlegend=False,
    ))
    figure.add_trace(go.Scattermap(
        lat=[last["latitude"]], lon=[last["longitude"]],
        mode="markers+text" if label else "markers",
        marker={"size": 12, "color": colour},
        text=[f" {label}"] if label else None,
        textposition="middle right",
        textfont={"size": 11, "color": COLOURS["ink"]},
        hovertext=[f"Last seen {pd.Timestamp(last['event_time']):%H:%M}"],
        hoverinfo="text", showlegend=False,
    ))


def _frame(figure: go.Figure, points: pd.DataFrame) -> go.Figure:
    """Frame the selection rather than the world.

    Web-mercator zoom is logarithmic -- each level halves the visible span --
    so the fit is log2(360 / span), less a little for margin. A linear guess
    put three European flights at zoom 2.5, which is most of the Atlantic.
    """
    lat_span = float(points["latitude"].max() - points["latitude"].min())
    lon_span = float(points["longitude"].max() - points["longitude"].min())
    # a degree of longitude is shorter than one of latitude away from the
    # equator, so compare them in comparable units before taking the wider
    mid_lat = float(points["latitude"].mean())
    span = max(lat_span, lon_span * math.cos(math.radians(mid_lat)), 0.02)

    figure.update_layout(
        map={
            "style": "carto-darkmatter" if DARK else "carto-positron",
            "center": {"lat": mid_lat,
                       "lon": float(points["longitude"].mean())},
            "zoom": max(1.5, min(11.0, math.log2(360.0 / span) - 0.75)),
        },
        height=560,
        margin={"l": 0, "r": 0, "t": 0, "b": 0},
        paper_bgcolor="rgba(0,0,0,0)",
        font={"color": COLOURS["ink"]},
        legend={"orientation": "h", "yanchor": "bottom", "y": 0.008, "x": 0.008,
                "bgcolor": "rgba(128,128,128,.14)", "title": "",
                "font": {"size": 11}},
    )
    return figure


# --- per-flight detail -------------------------------------------------------


def draw_phase_timeline(phases: pd.DataFrame) -> go.Figure:
    """Only the phases actually detected, in the order a flight flies them."""
    frame = phases.sort_values("phase_seq").copy()
    frame["label"] = frame["phase"].map(data.PHASE_LABELS)
    present = [data.PHASE_LABELS[p] for p in data.PHASE_ORDER
               if p in set(frame["phase"])]
    figure = px.timeline(
        frame, x_start="start_time", x_end="end_time", y="label", color="phase",
        color_discrete_map=COLOURS["phases"],
        category_orders={"label": present},
        custom_data=["label", "duration_seconds", "n_observations"],
    )
    figure.update_traces(hovertemplate=(
        "%{customdata[0]}<br>%{x|%H:%M:%S}<br>"
        "%{customdata[2]:,} observations<extra></extra>"
    ))
    figure.update_yaxes(title=None, categoryorder="array",
                        categoryarray=present[::-1], showgrid=False)
    figure.update_xaxes(title=None)
    return chart_layout(figure, height=52 + 34 * len(present), legend=False)


def draw_profile(points: pd.DataFrame) -> go.Figure:
    """Altitude and speed as two stacked panels sharing one time axis.

    Two panels rather than two y-scales on one: overlaid axes with different
    units invite crossings that mean nothing, and the reader cannot tell which
    line belongs to which scale.
    """
    figure = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.07)
    figure.add_trace(go.Scatter(
        x=points["event_time"], y=points["altitude_ft"], name="Altitude",
        line={"color": COLOURS["series"][0], "width": 2}, fill="tozeroy",
        fillcolor="rgba(42,120,214,.10)",
        hovertemplate="%{x|%H:%M} · %{y:,.0f} ft<extra></extra>",
    ), row=1, col=1)
    figure.add_trace(go.Scatter(
        x=points["event_time"], y=points["ground_speed_kt"], name="Ground speed",
        line={"color": COLOURS["series"][1], "width": 2},
        hovertemplate="%{x|%H:%M} · %{y:,.0f} kt<extra></extra>",
    ), row=2, col=1)
    figure.update_yaxes(title_text="ft", rangemode="tozero", row=1, col=1)
    figure.update_yaxes(title_text="kt", rangemode="tozero", row=2, col=1)
    return chart_layout(figure, height=310, legend=False)


def flight_details(flight, phases: pd.DataFrame, holds: pd.DataFrame,
                   points: pd.DataFrame) -> None:
    st.markdown(f"## {data.text_or(flight.callsign, flight.icao)}")
    st.markdown(
        f"<p class='lede'>{data.text_or(flight.departure_airport_name, 'Origin not detected')}"
        f" → {data.text_or(flight.arrival_airport_name, 'Destination not detected')}"
        f" · {pd.Timestamp(flight.flight_date):%A %d %B %Y}</p>",
        unsafe_allow_html=True,
    )

    metric_row([
        ("Airline", data.text_or(flight.airline_icao)),
        ("Aircraft type", data.text_or(flight.aircraft_type)),
        ("Registration", data.text_or(flight.registration)),
        ("Departure", data.format_clock(flight.departure_time)),
        ("Arrival", data.format_clock(flight.arrival_time)),
    ])
    metric_row([
        ("Duration", data.format_duration(flight.duration_seconds)),
        ("Max altitude", f"{flight.max_altitude_ft:,.0f} ft"
         if pd.notna(flight.max_altitude_ft) else "—"),
        ("Max speed", f"{flight.max_ground_speed_kt:,.0f} kt"
         if pd.notna(flight.max_ground_speed_kt) else "—"),
        ("Observations", f"{flight.n_observations:,}"),
        ("Detected holds", f"{int(flight.n_detected_holds):,}"),
    ])
    st.markdown(
        "<p class='note'>Registration, type and owner come from an aircraft "
        "database, not from the transmission. Departure and arrival are "
        "inferred by matching the trajectory's endpoints to an airport.</p>",
        unsafe_allow_html=True,
    )

    left, right = st.columns([1, 1], gap="large")
    with left:
        st.markdown("### Detected phases")
        st.markdown("<p class='note'>ADS-B-derived classification, from the "
                    "observed vertical rate and ground state — not an ATC or "
                    "airline record.</p>", unsafe_allow_html=True)
        if phases.empty:
            st.caption("No phases detected for this flight.")
        else:
            st.plotly_chart(draw_phase_timeline(phases), use_container_width=True)
    with right:
        st.markdown("### Altitude and speed")
        st.markdown("<p class='note'>Every stored observation, unsmoothed.</p>",
                    unsafe_allow_html=True)
        st.plotly_chart(draw_profile(points), use_container_width=True)

    st.markdown("### Detected holding patterns")
    if holds.empty:
        st.caption("No holding pattern detected in this trajectory.")
        return

    st.markdown(
        "<p class='note'><b>ADS-B-derived detected holding pattern.</b> "
        "Sustained circling in a confined area, measured from the observed "
        "trajectory. It is not evidence that the aircraft was instructed to "
        "hold, and circuit training produces the same geometry.</p>",
        unsafe_allow_html=True,
    )
    for hold in holds.sort_values("hold_seq").itertuples():
        metric_row([
            ("Duration", data.format_duration(hold.duration_seconds)),
            ("Circuits", f"{hold.circuits:g}"),
            ("Altitude", f"{hold.mean_altitude_ft:,.0f} ft"
             if pd.notna(hold.mean_altitude_ft) else "—"),
            ("Near", data.text_or(hold.arrival_airport_iata,
                                  data.text_or(hold.arrival_airport_ident))),
            ("Distance", f"{hold.distance_to_arrival_airport_km:,.0f} km"
             if pd.notna(hold.distance_to_arrival_airport_km) else "—"),
        ])
        st.markdown(
            f"<p class='note'>{pd.Timestamp(hold.hold_start):%H:%M:%S}–"
            f"{pd.Timestamp(hold.hold_end):%H:%M:%S} · centred "
            f"{hold.centroid_latitude:.3f}, {hold.centroid_longitude:.3f} · "
            f"{hold.span_km:.1f} km across · "
            f"{hold.min_altitude_ft:,.0f}–{hold.max_altitude_ft:,.0f} ft</p>",
            unsafe_allow_html=True,
        )


# --- the explorer ------------------------------------------------------------


def sidebar_filters(flights: pd.DataFrame) -> dict:
    st.sidebar.markdown("<p class='eyebrow'>Filters</p>", unsafe_allow_html=True)

    dates = sorted(pd.to_datetime(flights["flight_date"]).dt.date.unique())
    date = st.sidebar.selectbox(
        "Date", [None] + dates, format_func=lambda d: "All 7 days" if d is None
        else f"{d:%a %d %b}",
    )

    st.sidebar.markdown("---")
    st.sidebar.markdown("<p class='eyebrow'>Route</p>", unsafe_allow_html=True)
    departure = st.sidebar.selectbox(
        "Departure airport", [None] + data.airport_options(flights, "departure_airport_ident"),
        format_func=lambda a: "Any" if a is None else data.airport_label(a),
    )
    arrival = st.sidebar.selectbox(
        "Arrival airport", [None] + data.airport_options(flights, "arrival_airport_ident"),
        format_func=lambda a: "Any" if a is None else data.airport_label(a),
    )

    st.sidebar.markdown("---")
    st.sidebar.markdown("<p class='eyebrow'>Aircraft</p>", unsafe_allow_html=True)
    airline = st.sidebar.selectbox(
        "Airline", [None] + data.options_for(flights, "airline_icao"),
        format_func=lambda a: "Any" if a is None else a,
    )
    aircraft_type = st.sidebar.selectbox(
        "Aircraft type", [None] + data.options_for(flights, "aircraft_type"),
        format_func=lambda a: "Any" if a is None else a,
    )
    callsign = st.sidebar.text_input("Callsign or address", placeholder="e.g. SWR or 4b18")

    st.sidebar.markdown("---")
    limit = st.sidebar.slider(
        "Trajectories on the map", 1, MAX_MAP_FLIGHTS, DEFAULT_MAP_FLIGHTS,
        help="A display limit for legibility. The full seven days of "
             "trajectories stay queryable — this only caps how many are drawn "
             "at once.",
    )
    return {"date": date, "departure": departure, "arrival": arrival,
            "airline": airline, "aircraft_type": aircraft_type,
            "callsign": callsign, "limit": limit}


def flight_explorer(flights: pd.DataFrame, metrics: pd.DataFrame) -> None:
    choices = sidebar_filters(flights)
    limit = choices.pop("limit")
    matches = data.select_flights(flights, **choices)

    st.markdown("# Flight Explorer")
    st.markdown(
        f"<p class='lede'>{len(flights):,} reconstructed flights over seven days, "
        f"24–30 December 2025, at Zürich and Düsseldorf. Every flight's full "
        f"trajectory is available.</p>",
        unsafe_allow_html=True,
    )

    if matches.empty:
        st.info("No flights match these filters. Widen one of them to continue.")
        return

    metric_row([
        ("Matching flights", f"{len(matches):,}"),
        ("Aircraft", f"{matches['icao'].nunique():,}"),
        ("Airlines", f"{matches['airline_icao'].nunique():,}"),
        ("Aircraft types", f"{matches['aircraft_type'].nunique():,}"),
        ("With detected holds", f"{int(matches['has_detected_hold'].sum()):,}"),
    ])

    st.markdown("## Flights")
    st.markdown(
        f"<p class='note'>Select up to {limit} rows to draw their trajectories. "
        f"Sorted by first observation.</p>", unsafe_allow_html=True,
    )
    table = data.flight_table(matches)
    event = st.dataframe(
        table, use_container_width=True, height=310,
        on_select="rerun", selection_mode="multi-row", hide_index=True,
    )

    picked = [table.index[i] for i in event.selection.rows]
    if not picked:
        st.info(
            "Select one or more flights above to draw their trajectories. "
            "Nothing is loaded until you do — the point table holds 3.3 million "
            "observations."
        )
        return

    shown, dropped = picked[:limit], picked[limit:]
    if dropped:
        st.warning(
            f"Drawing the first {limit} of {len(picked)} selected flights. "
            f"Raise the map limit in the sidebar, or deselect some rows.",
            icon="⚠",
        )

    selected = matches.set_index("flight_id").loc[shown]
    tracks = load_trajectories(
        tuple(shown), tuple(sorted(set(pd.to_datetime(selected["flight_date"]).dt.date)))
    )
    if tracks.empty:
        st.warning("No trajectory stored for the selected flights.")
        return

    all_phases, all_holds = load_phases(), load_holds()
    phases = all_phases[all_phases["flight_id"].isin(shown)]
    holds = all_holds[all_holds["flight_id"].isin(shown)]

    st.markdown("## Trajectories")
    if len(shown) == 1:
        flight_id = shown[0]
        points = data.label_points_with_phase(tracks, phases)
        st.plotly_chart(draw_phase_map(points, holds, metrics),
                        use_container_width=True)
        st.markdown(
            "<p class='note'>Coloured by ADS-B-derived phase. Hollow ring = "
            "first observation, filled dot = last. Airports marked in grey.</p>",
            unsafe_allow_html=True,
        )
        st.divider()
        flight_details(selected.loc[flight_id], phases, holds, tracks)
    else:
        legend = {row.Index: data.flight_label(row) for row in selected.itertuples()}
        short = {row.Index: data.text_or(row.callsign, row.icao)
                 for row in selected.itertuples()}
        st.plotly_chart(draw_flights_map(tracks, legend, short, metrics),
                        use_container_width=True)
        coloured = min(len(shown), data.MAX_COLOURED_FLIGHTS)
        extra = ("; further tracks share a neutral colour and are told apart by "
                 "their labels" if len(shown) > coloured else "")
        st.markdown(
            f"<p class='note'>Hollow ring = first observation, filled dot and "
            f"label = last. {coloured} tracks carry a distinct colour{extra}. "
            f"Select a single flight to see its phases, profile and holds.</p>",
            unsafe_allow_html=True,
        )
        summary = selected.reset_index()[[
            "callsign", "aircraft_type", "airline_icao", "duration_seconds",
            "n_observations", "n_detected_holds"]].copy()
        summary["duration_seconds"] = summary["duration_seconds"].map(data.format_duration)
        st.dataframe(
            summary.rename(columns={
                "callsign": "Callsign", "aircraft_type": "Aircraft",
                "airline_icao": "Airline", "duration_seconds": "Duration",
                "n_observations": "Points", "n_detected_holds": "Holds"}),
            hide_index=True, use_container_width=True,
        )


# --- airport operations ------------------------------------------------------


def draw_daily(rows: pd.DataFrame) -> go.Figure:
    frame = rows.sort_values("operations_date").copy()
    frame["day"] = pd.to_datetime(frame["operations_date"]).dt.strftime("%a %d %b")
    figure = go.Figure()
    for name, colour in (("arrivals", COLOURS["series"][0]),
                         ("departures", COLOURS["series"][1])):
        figure.add_trace(go.Bar(
            x=frame["day"], y=frame[name], name=name.capitalize(),
            marker={"color": colour, "line": {"width": 0}},
            hovertemplate="%{x} · %{y:,} " + name + "<extra></extra>",
        ))
    figure.update_layout(barmode="group", bargap=0.28, bargroupgap=0.06)
    figure.update_yaxes(title_text="Movements")
    return chart_layout(figure, height=290)


def draw_hourly(hourly: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    for name, colour in (("Arrivals", COLOURS["series"][0]),
                         ("Departures", COLOURS["series"][1])):
        figure.add_trace(go.Bar(
            x=hourly["hour"], y=hourly[name], name=name,
            marker={"color": colour, "line": {"width": 0}},
            hovertemplate="%{x}:00 · %{y:,} " + name.lower() + "<extra></extra>",
        ))
    figure.update_layout(barmode="group", bargap=0.22, bargroupgap=0.05)
    figure.update_xaxes(title_text="Hour of day (UTC)", dtick=2)
    figure.update_yaxes(title_text="Movements")
    return chart_layout(figure, height=290)


def draw_distribution(frame: pd.DataFrame, title: str) -> go.Figure:
    figure = go.Figure(go.Bar(
        x=frame["flights"], y=frame["value"], orientation="h",
        marker={"color": COLOURS["series"][0], "line": {"width": 0}},
        hovertemplate="%{y} · %{x:,} flights<extra></extra>",
    ))
    figure.update_layout(bargap=0.3)
    figure.update_yaxes(autorange="reversed", title=None)
    figure.update_xaxes(title_text=title)
    return chart_layout(figure, height=40 + 30 * len(frame), legend=False)


def airport_view(flights: pd.DataFrame, metrics: pd.DataFrame) -> None:
    st.markdown("# Airport operations")
    st.markdown(
        "<p class='lede'>Movements inferred from ADS-B observations and matched "
        "to an airport by proximity. Not official airport statistics.</p>",
        unsafe_allow_html=True,
    )

    idents = [i for i in data.STUDY_AIRPORTS if i in set(metrics["airport_ident"])]
    ident = st.segmented_control(
        "Airport", idents, default=idents[0],
        format_func=lambda i: f"{data.STUDY_AIRPORTS[i]} · {i}",
    ) or idents[0]

    rows = metrics[metrics["airport_ident"] == ident]
    iata = data.STUDY_AIRPORTS[ident]
    here = data.airport_flights(flights, iata)
    held = int(rows["flights_with_detected_holds"].sum())
    arrivals = int(rows["arrivals"].sum())

    st.markdown(f"## {rows.iloc[0]['airport_name']}")
    st.markdown(
        f"<p class='lede'>{ident} · {iata} · {len(rows)} days observed</p>",
        unsafe_allow_html=True,
    )
    metric_row([
        ("Arrivals", f"{arrivals:,}"),
        ("Departures", f"{int(rows['departures'].sum()):,}"),
        ("Total movements", f"{int(rows['total_operations'].sum()):,}"),
        ("Unique aircraft", f"{int(rows['unique_aircraft'].sum()):,}"),
        ("Flights with holds", f"{held:,}"),
    ])

    st.markdown("### Movements by day")
    st.plotly_chart(draw_daily(rows), use_container_width=True)

    st.markdown("### Movements by hour of day")
    st.plotly_chart(draw_hourly(data.hourly_traffic(flights, iata)),
                    use_container_width=True)

    left, right = st.columns(2, gap="large")
    with left:
        st.markdown("### Airlines")
        airlines = data.distribution(here, "airline_icao")
        if airlines.empty:
            st.caption("No airline could be inferred for these flights.")
        else:
            st.plotly_chart(draw_distribution(airlines, "Flights"),
                            use_container_width=True)
            st.markdown("<p class='note'>From an airline-style callsign; "
                        "about a third of flights carry none.</p>",
                        unsafe_allow_html=True)
    with right:
        st.markdown("### Aircraft types")
        types = data.distribution(here, "aircraft_type")
        if types.empty:
            st.caption("No aircraft type recorded for these flights.")
        else:
            st.plotly_chart(draw_distribution(types, "Flights"),
                            use_container_width=True)
            st.markdown("<p class='note'>From an aircraft database, not from "
                        "the transmission.</p>", unsafe_allow_html=True)

    st.markdown("### Detected holding patterns")
    if held == 0:
        st.caption(
            "No holding pattern detected at this airport in the study period."
        )
    else:
        rate = held / arrivals if arrivals else None
        metric_row([
            ("Flights with holds", f"{held:,}"),
            ("Share of arrivals", f"{rate:.1%}" if rate else "—"),
            ("Average hold", data.format_duration(
                rows["avg_hold_duration_seconds"].mean())),
        ])
        st.markdown(
            "<p class='note'><b>ADS-B-derived detected holding patterns.</b> "
            "Sustained circling near the inferred arrival airport. Not a delay "
            "metric and not evidence of an ATC instruction.</p>",
            unsafe_allow_html=True,
        )
        daily_holds = rows.sort_values("operations_date")[
            ["operations_date", "arrivals", "flights_with_detected_holds",
             "avg_hold_duration_seconds"]].copy()
        daily_holds["avg_hold_duration_seconds"] = daily_holds[
            "avg_hold_duration_seconds"].map(data.format_duration)
        st.dataframe(
            daily_holds.rename(columns={
                "operations_date": "Date", "arrivals": "Arrivals",
                "flights_with_detected_holds": "Flights with holds",
                "avg_hold_duration_seconds": "Average hold"}),
            hide_index=True, use_container_width=True,
        )


# --- app ---------------------------------------------------------------------

flights = load_flights()
metrics = load_airport_metrics()

explorer_tab, airport_tab = st.tabs(["Flights", "Airports"])
with explorer_tab:
    flight_explorer(flights, metrics)
with airport_tab:
    airport_view(flights, metrics)

st.sidebar.markdown(
    "<p class='note'>Flights, phases and holding patterns are inferred from "
    "volunteer ADS-B observations. They are not airline, airport or ATC "
    "records, and will not reconcile with published statistics.</p>",
    unsafe_allow_html=True,
)
