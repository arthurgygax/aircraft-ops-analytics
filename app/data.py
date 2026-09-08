"""Reading, filtering and reshaping the published tables.

Deliberately free of Streamlit so it can be tested directly, and deliberately
free of Spark: the app reads Delta with delta-rs, which needs no JVM and starts
in a second. Every expensive thing -- reconstruction, phase detection, hold
detection -- already happened in the pipeline. Nothing here computes analytics;
it selects and reshapes rows that are already analytical.

THE ACCESS PATTERN
    filters -> flight metadata -> selected flight_ids -> track query -> map

    The four small tables (flights, phases, holds, airport operations) are a
    few thousand rows each and are read whole. ``observations`` is 3.3M points
    and is **never** read whole: only the selected flights are fetched, and
    always with their dates, which prunes six of the seven partitions.
    Measured: 1,538 ms without the date against 320 ms with it.
"""

from __future__ import annotations

import datetime as dt
import os
from collections.abc import Iterable, Sequence

import pandas as pd
from deltalake import DeltaTable

BUCKET = os.environ.get("S3_BUCKET", "adsb")

# The same root the pipeline writes to, so a development root is one variable
# for both. delta-rs speaks s3://, the pipeline's S3A speaks s3a://.
ROOT = os.environ.get("ADSB_ROOT", f"s3a://{BUCKET}").rstrip("/").replace("s3a://", "s3://", 1)

TABLES = {
    "flights": "flights",
    "phases": "flight_phases",
    "holds": "flight_holds",
    "airport_metrics": "airport_daily_operations",
    # trajectories come from the canonical observation table; there is no
    # second copy of the point grain anywhere in the pipeline
    "tracks": "observations",
}

# The two airports the study period covers. Ordered, because they lead every
# airport dropdown: the common path first.
STUDY_AIRPORTS: dict[str, str] = {"LSZH": "ZRH", "EDDL": "DUS"}

TRACK_COLUMNS = [
    "flight_id", "icao", "registration", "aircraft_type", "callsign",
    "event_time", "observation_seq", "latitude", "longitude", "on_ground",
    "altitude_ft", "ground_speed_kt", "track_deg", "vertical_rate_fpm",
    "release_date",
]


# --- colour ------------------------------------------------------------------
#
# Three categorical hues, not eight. They are the only three of the reference
# palette that clear the all-pairs colour-vision and normal-vision floors in
# BOTH light and dark -- which is the test that applies here, because any two
# trajectories can end up adjacent on a map. Checked with the palette
# validator rather than by eye: adding a fourth (violet) measures ΔE 1.9
# against blue for protanopes on the dark surface, which is no separation at
# all.
#
# Identity therefore never rests on colour alone: every track is also labelled
# with its callsign, and the flight table repeats the swatch.

SERIES_LIGHT = ["#2a78d6", "#eb6834", "#1baf7a"]
SERIES_DARK = ["#3987e5", "#d95926", "#199e70"]

# Beyond three selected flights, extra tracks are drawn in neutral ink and
# identified by their label. A fourth hue is never invented.
NEUTRAL_LIGHT = "#8a8a85"
NEUTRAL_DARK = "#9a9a94"

MAX_COLOURED_FLIGHTS = 3

# Phases share the same three validated hues. Ground phases are deliberately
# neutral: they are two short stubs at the ends of a flight, they are always
# labelled on the timeline, and giving them saturated hues of their own is what
# pushed this palette past the separation floor.
PHASE_COLOURS_LIGHT = {
    "climb": "#2a78d6",
    "cruise": "#1baf7a",
    "descent": "#eb6834",
    "taxi_out": "#6f6f6a",
    "taxi_in": "#6f6f6a",
    "taxi": "#6f6f6a",
    "unknown": "#c2c2bb",
}
PHASE_COLOURS_DARK = {
    "climb": "#3987e5",
    "cruise": "#199e70",
    "descent": "#d95926",
    "taxi_out": "#a6a69f",
    "taxi_in": "#a6a69f",
    "taxi": "#a6a69f",
    "unknown": "#5c5c57",
}

# Ordered the way a flight actually progresses, which is also the legend order.
PHASE_ORDER = ["taxi_out", "climb", "cruise", "descent", "taxi_in", "taxi", "unknown"]

PHASE_LABELS = {
    "taxi_out": "Taxi out",
    "climb": "Climb",
    "cruise": "Cruise",
    "descent": "Descent",
    "taxi_in": "Taxi in",
    "taxi": "Taxi",
    "unknown": "Unclassified",
}

# The hold highlight is a status colour, not a fourth series: it marks a state
# rather than an identity, and it is always accompanied by a label.
HOLD_COLOUR = "#eda100"


def palette(dark: bool = False) -> dict:
    """Every colour the UI needs, for one theme.

    Dark is a selected set of steps for the dark surface, not an automatic
    inversion of the light one.
    """
    return {
        "series": SERIES_DARK if dark else SERIES_LIGHT,
        "neutral": NEUTRAL_DARK if dark else NEUTRAL_LIGHT,
        "phases": PHASE_COLOURS_DARK if dark else PHASE_COLOURS_LIGHT,
        "hold": HOLD_COLOUR,
        "grid": "rgba(140,140,140,.18)",
        "ink": "#e9e9e4" if dark else "#1b1b19",
        "ink_muted": "#9a9a94" if dark else "#6b6b66",
    }


def series_colour(index: int, dark: bool = False) -> str:
    """Colour for the n-th selected flight; neutral once the hues run out."""
    colours = SERIES_DARK if dark else SERIES_LIGHT
    if index < len(colours):
        return colours[index]
    return NEUTRAL_DARK if dark else NEUTRAL_LIGHT


# --- reading -----------------------------------------------------------------


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
    return f"{ROOT}/{TABLES[name]}"


def read_table(name: str, filters: list | None = None) -> pd.DataFrame:
    """Read a published table, pushing ``filters`` down to the parquet files."""
    table = DeltaTable(table_uri(name), storage_options=storage_options())
    return table.to_pandas(filters=filters)


def _as_dates(values: Iterable) -> list[dt.date]:
    """Partition values must be real dates: pyarrow will not compare a string."""
    out = []
    for value in values:
        if value is None or (not isinstance(value, (dt.date, str)) and pd.isna(value)):
            continue
        if isinstance(value, dt.datetime):
            out.append(value.date())
        elif isinstance(value, dt.date):
            out.append(value)
        else:
            out.append(pd.Timestamp(value).date())
    return sorted(set(out))


def load_trajectories(
    flight_ids: Sequence[str], dates: Iterable | None = None
) -> pd.DataFrame:
    """Trajectory points for the given flights, ordered within each flight.

    ``dates`` is not an optimisation to be skipped: without it delta-rs opens
    all seven day-partitions instead of the one or two that can hold these
    flights. Callers have the dates already -- they came from the flight rows
    that produced ``flight_ids``.
    """
    ids = [fid for fid in dict.fromkeys(flight_ids) if fid]
    if not ids:
        return pd.DataFrame(columns=TRACK_COLUMNS)

    filters: list = []
    days = _as_dates(dates or [])
    if days:
        filters.append(("release_date", "in", days))
    filters.append(("flight_id", "in", ids))

    points = read_table("tracks", filters=filters)
    if points.empty:
        return points
    return points.sort_values(["flight_id", "observation_seq"]).reset_index(drop=True)


def load_trajectory(flight_id: str, flight_date=None) -> pd.DataFrame:
    """One flight's ADS-B positions, in order."""
    return load_trajectories([flight_id], [flight_date] if flight_date else None)


# --- filtering ---------------------------------------------------------------
#
# Departure and arrival are separate filters on purpose. A single "airport"
# box cannot express "departing ZRH" as distinct from "arriving at ZRH", and
# those are different questions about the same airport.

def select_flights(
    flights: pd.DataFrame,
    date=None,
    departure: str | None = None,
    arrival: str | None = None,
    airline: str | None = None,
    aircraft_type: str | None = None,
    callsign: str = "",
) -> pd.DataFrame:
    """The flight list for one set of filter choices. ``None`` means 'any'.

    Every argument is an independent AND, so "SWISS flights departing ZRH" is
    ``departure='LSZH', airline='SWR'`` and "Lufthansa between ZRH and DUS" is
    ``departure='LSZH', arrival='EDDL', airline='DLH'``.
    """
    result = flights
    equals = {
        "departure_airport_ident": departure,
        "arrival_airport_ident": arrival,
        "airline_icao": airline,
        "aircraft_type": aircraft_type,
    }
    for column, value in equals.items():
        if value:
            result = result[result[column] == value]

    if date is not None:
        wanted = pd.Timestamp(date).date()
        result = result[
            pd.to_datetime(result["flight_date"], errors="coerce").dt.date == wanted
        ]

    text = (callsign or "").strip().upper()
    if text:
        # one box for callsign or aircraft address: people search for both
        matches = result["callsign"].fillna("").str.upper().str.contains(text, regex=False)
        address = result["icao"].fillna("").str.upper().str.contains(text, regex=False)
        result = result[matches | address]
    return result


def options_for(flights: pd.DataFrame, column: str) -> list:
    """Values still available for a filter, blanks dropped and sorted."""
    if flights.empty or column not in flights:
        return []
    return sorted(flights[column].dropna().unique().tolist())


def airport_options(flights: pd.DataFrame, column: str) -> list[str]:
    """Airport idents for a filter, with the two study airports first.

    They account for four fifths of both columns, so putting them at the top
    is the difference between a dropdown you scroll and one you don't.
    """
    available = options_for(flights, column)
    leading = [a for a in STUDY_AIRPORTS if a in available]
    return leading + [a for a in available if a not in STUDY_AIRPORTS]


def airport_label(ident: str) -> str:
    """``LSZH`` -> ``LSZH · ZRH`` for the two airports people know by IATA."""
    iata = STUDY_AIRPORTS.get(ident)
    return f"{ident} · {iata}" if iata else ident


# --- reshaping for display ---------------------------------------------------

FLIGHT_TABLE_COLUMNS = {
    "callsign": "Callsign",
    "flight_date": "Date",
    "departure_airport_iata": "From",
    "arrival_airport_iata": "To",
    "airline_icao": "Airline",
    "aircraft_type": "Aircraft",
    "departure_time": "Dep",
    "arrival_time": "Arr",
    "duration": "Duration",
    "n_detected_holds": "Holds",
}


def flight_table(flights: pd.DataFrame) -> pd.DataFrame:
    """The compact list the user picks from, in the order it is displayed."""
    if flights.empty:
        return pd.DataFrame(columns=list(FLIGHT_TABLE_COLUMNS.values()))

    frame = flights.sort_values("first_seen_time").copy()
    frame["duration"] = frame["duration_seconds"].map(format_duration)
    for column in ("departure_time", "arrival_time"):
        times = pd.to_datetime(frame[column], errors="coerce")
        frame[column] = times.dt.strftime("%H:%M").fillna("—")
    frame["callsign"] = [
        text_or(row.callsign, row.icao) for row in frame.itertuples()
    ]
    frame["flight_date"] = pd.to_datetime(
        frame["flight_date"], errors="coerce"
    ).dt.strftime("%a %d %b")
    for column in ("departure_airport_iata", "arrival_airport_iata",
                   "airline_icao", "aircraft_type"):
        frame[column] = frame[column].map(lambda v: text_or(v, "—"))

    display = frame[list(FLIGHT_TABLE_COLUMNS)].rename(columns=FLIGHT_TABLE_COLUMNS)
    display.index = frame["flight_id"].to_numpy()
    return display


def flight_label(row) -> str:
    """How a flight reads in a legend or a label: callsign and route."""
    callsign = text_or(row.callsign, row.icao)
    origin = text_or(row.departure_airport_iata, "???")
    destination = text_or(row.arrival_airport_iata, "???")
    return f"{callsign}  ·  {origin} → {destination}"


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


def points_in_holds(points: pd.DataFrame, holds: pd.DataFrame) -> pd.DataFrame:
    """The subset of a trajectory that falls inside a detected hold.

    Used to draw the circling itself rather than only a marker where it
    happened. A lookup against intervals the pipeline detected, not detection.
    """
    if points.empty or holds.empty:
        return points.iloc[0:0]

    times = pd.to_datetime(points["event_time"], errors="coerce")
    inside = pd.Series(False, index=points.index)
    for hold in holds.itertuples():
        inside |= times.between(
            pd.Timestamp(hold.hold_start), pd.Timestamp(hold.hold_end)
        )
    return points[inside]


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


def distribution(flights: pd.DataFrame, column: str, top: int = 8) -> pd.DataFrame:
    """The ``top`` most common values of a column, with the rest folded in.

    Folded rather than truncated, so the bars still add up to the flights the
    user filtered to and a long tail cannot masquerade as absence.
    """
    if flights.empty or column not in flights:
        return pd.DataFrame(columns=["value", "flights"])

    counts = flights[column].dropna().value_counts()
    head = counts.head(top)
    frame = head.rename_axis("value").reset_index(name="flights")
    remainder = int(counts.iloc[top:].sum())
    if remainder:
        frame.loc[len(frame)] = {"value": f"Other ({len(counts) - top})",
                                 "flights": remainder}
    return frame


def airport_flights(flights: pd.DataFrame, iata: str) -> pd.DataFrame:
    """Flights that touched one airport, at either end."""
    return flights[
        (flights["departure_airport_iata"] == iata)
        | (flights["arrival_airport_iata"] == iata)
    ]


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


def format_clock(value) -> str:
    """A timestamp as HH:MM, or a dash when the pipeline detected none."""
    if value is None or pd.isna(value):
        return "—"
    return pd.Timestamp(value).strftime("%H:%M")
