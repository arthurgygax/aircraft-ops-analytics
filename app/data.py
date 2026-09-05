"""Reading and filtering the Gold tables.

Deliberately free of Streamlit so it can be tested directly, and deliberately
free of Spark: the app reads Delta with delta-rs, which needs no JVM and starts
in a second. Every expensive thing -- reconstruction, phase detection, hold
detection -- already happened in the pipeline. Nothing here computes analytics;
it selects and reshapes rows that are already analytical.
"""

from __future__ import annotations

import os

import pandas as pd
from deltalake import DeltaTable

BUCKET = os.environ.get("S3_BUCKET", "adsb")

TABLES = {
    "flights": "gold/flights",
    "phases": "gold/flight_phases",
    "holds": "gold/flight_holds",
    "airport_metrics": "gold/airport_daily_operations",
    # trajectories stay in Silver: a Gold copy would duplicate 1.3 GB and
    # exclude nothing (see the Gold model notes in the README)
    "tracks": "silver/flight_observations",
}

# Ordered the way a flight actually progresses, which is also the legend order.
PHASE_ORDER = ["taxi_out", "climb", "cruise", "descent", "taxi_in", "taxi", "unknown"]
PHASE_COLOURS = {
    "taxi_out": "#8c7ae6",
    "climb": "#2e86de",
    "cruise": "#10ac84",
    "descent": "#ee5253",
    "taxi_in": "#576574",
    "taxi": "#8395a7",
    "unknown": "#c8d6e5",
}


def storage_options() -> dict[str, str]:
    """Credentials for MinIO or AWS, from the same variables the pipeline uses."""
    options = {
        "AWS_ACCESS_KEY_ID": os.environ.get("S3_ACCESS_KEY", ""),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("S3_SECRET_KEY", ""),
        "AWS_REGION": os.environ.get("S3_REGION", "us-east-1"),
    }
    endpoint = os.environ.get("S3_ENDPOINT")
    if endpoint:
        # MinIO: plain HTTP against a path-style endpoint
        options["AWS_ENDPOINT_URL"] = endpoint
        options["AWS_ALLOW_HTTP"] = "true"
    return options


def table_uri(name: str) -> str:
    return f"s3://{BUCKET}/{TABLES[name]}"


def read_table(name: str, filters: list | None = None) -> pd.DataFrame:
    """Read a Gold table, pushing ``filters`` down to the parquet files."""
    table = DeltaTable(table_uri(name), storage_options=storage_options())
    return table.to_pandas(filters=filters)


def load_trajectory(flight_id: str) -> pd.DataFrame:
    """One flight's ADS-B positions, in order."""
    points = read_table("tracks", filters=[("flight_id", "=", flight_id)])
    return points.sort_values("observation_seq").reset_index(drop=True)


# --- filtering ---------------------------------------------------------------

# (column, label) for each filter, applied in this order so that each dropdown
# only offers values still reachable given the choices above it.
FILTER_COLUMNS = [
    ("flight_date", "Date"),
    ("departure_airport_iata", "From"),
    ("arrival_airport_iata", "To"),
    ("airline_icao", "Airline"),
    ("aircraft_type", "Aircraft"),
]


def options_for(flights: pd.DataFrame, column: str) -> list:
    """Values still available for a filter, blanks dropped and sorted."""
    if flights.empty or column not in flights:
        return []
    return sorted(flights[column].dropna().unique().tolist())


def filter_flights(flights: pd.DataFrame, selections: dict, search: str = "") -> pd.DataFrame:
    """Apply the sidebar selections. A missing or empty selection means 'any'."""
    result = flights
    for column, _ in FILTER_COLUMNS:
        value = selections.get(column)
        if value:
            result = result[result[column] == value]

    text = (search or "").strip().upper()
    if text:
        # one box for callsign or aircraft address: people search for both
        callsign = result["callsign"].fillna("").str.upper().str.contains(text, regex=False)
        icao = result["icao"].fillna("").str.upper().str.contains(text, regex=False)
        result = result[callsign | icao]
    return result


def flight_label(row: pd.Series) -> str:
    """How a flight reads in the picker: callsign, route, time."""
    callsign = text_or(row.callsign, row.icao)
    origin = text_or(row.departure_airport_iata, "???")
    destination = text_or(row.arrival_airport_iata, "???")
    return f"{callsign}  ·  {origin} → {destination}  ·  {row.first_seen_time:%H:%M}"


# --- reshaping for display ---------------------------------------------------


def label_points_with_phase(points: pd.DataFrame, phases: pd.DataFrame) -> pd.DataFrame:
    """Attach each position to the phase interval it falls in.

    A lookup against intervals the pipeline already detected, not detection.
    """
    if points.empty or phases.empty:
        return points.assign(phase="unknown")

    intervals = phases.sort_values("start_time")[["start_time", "end_time", "phase"]]
    labelled = pd.merge_asof(
        points.sort_values("event_time"),
        intervals,
        left_on="event_time",
        right_on="start_time",
        direction="backward",
    )
    # a point after the last interval's end belongs to no phase
    outside = labelled["end_time"].isna() | (labelled["event_time"] > labelled["end_time"])
    labelled.loc[outside, "phase"] = "unknown"
    return labelled.drop(columns=["start_time", "end_time"]).fillna({"phase": "unknown"})


def hourly_traffic(flights: pd.DataFrame, airport_iata: str) -> pd.DataFrame:
    """Movements per hour of day at one airport, split by direction."""
    # to_datetime first: a column where every value is missing arrives as
    # object dtype, and .dt would raise on it
    departures = pd.to_datetime(
        flights.loc[flights["departure_airport_iata"] == airport_iata, "departure_time"],
        errors="coerce",
    ).dropna()
    arrivals = pd.to_datetime(
        flights.loc[flights["arrival_airport_iata"] == airport_iata, "arrival_time"],
        errors="coerce",
    ).dropna()

    frame = pd.DataFrame({"hour": range(24)})
    frame["Departures"] = (
        departures.dt.hour.value_counts().reindex(frame["hour"], fill_value=0).values
    )
    frame["Arrivals"] = (
        arrivals.dt.hour.value_counts().reindex(frame["hour"], fill_value=0).values
    )
    return frame


def text_or(value, fallback: str = "—") -> str:
    """A displayable string, treating NaN as missing.

    Needed because pandas uses NaN rather than None for missing strings, and
    NaN is truthy -- ``value or fallback`` would render the word "nan".
    """
    if value is None or (isinstance(value, float) and pd.isna(value)) or pd.isna(value):
        return fallback
    return str(value)


def format_duration(seconds: float | None) -> str:
    """Durations read as 1h 24m, not as 5040."""
    if seconds is None or pd.isna(seconds):
        return "—"
    seconds = int(seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes = remainder // 60
    if hours:
        return f"{hours}h {minutes:02d}m"
    if minutes:
        return f"{minutes}m {seconds % 60:02d}s"
    return f"{seconds}s"
