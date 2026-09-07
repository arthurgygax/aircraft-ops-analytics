"""The same pipeline, in pandas, for the Spark-vs-pandas benchmark.

    raw traces -> observations -> flights + movements + phases + holds + airport ops

Not a second implementation of the product: it exists so the two engines can be
measured on identical work. Every transformation mirrors ``src/adsb`` exactly --
the same dedup winner rule, the same 15-minute gap, the same ±300 fpm level
band, the same 5 km / 5,000 ft airport test, the same 6-minute centred turn
window -- and the outputs are compared row for row by ``run_benchmark.py``.

Written the way someone reaching for pandas would write it: vectorised
groupby/rolling, no Python row loops beyond parsing the JSON, and no tuning
aimed at the benchmark. It is deliberately not optimised, and neither is the
Spark side.
"""

from __future__ import annotations

import glob
import gzip
import json
import math
import os
from pathlib import Path

import numpy as np
import pandas as pd

GAP_SECONDS = 15 * 60
MAX_GROUND_SPEED_KT = 700
MAX_VERTICAL_RATE_FPM = 20000

MATCH_RADIUS_KM = 5.0
MAX_HEIGHT_ABOVE_AIRPORT_FT = 5000.0
EARTH_RADIUS_KM = 6371.0

LEVEL_BAND_FPM = 300.0
SMOOTHING_WINDOW_SECONDS = 60

TURN_WINDOW_SECONDS = 360
MIN_TURN_DEGREES = 360.0
MIN_DURATION_SECONDS = 240
MAX_SPAN_KM = 25.0
MAX_ALTITUDE_RANGE_FT = 4000.0
MAX_STEP_GAP_SECONDS = 60
KM_PER_DEGREE_LATITUDE = 111.32

AIRLINE_CALLSIGN = r"^[A-Z]{3}[0-9][0-9A-Z]*$"


# --- observations ------------------------------------------------------------


def epoch_seconds(times: pd.Series) -> pd.Series:
    """Whole seconds since the epoch, truncated -- Spark's UNIX_TIMESTAMP.

    Every duration and every gap in the Spark pipeline is a difference of two
    UNIX_TIMESTAMP values, so both operands lose their sub-second part before
    they are subtracted. Using exact timedeltas instead is not a rounding
    nicety: a 900.4 s coverage gap splits a flight in pandas and does not in
    Spark, and a 0.9 s step spanning a second boundary counts toward a turn in
    Spark and not in pandas. Measured on 16,000 aircraft, the naive version
    produced 4 extra flights and 547 phantom holds.
    """
    # cast to second resolution first: pandas 2.x keeps whatever unit the
    # column was built with (here datetime64[us]), so dividing raw ints by a
    # fixed factor silently produces nonsense on a different unit
    return times.astype("datetime64[s]").astype("int64")


def _callsign(details) -> str | None:
    if isinstance(details, dict):
        flight = details.get("flight")
        if isinstance(flight, str):
            return flight.strip()
    return None


def _nullable_ground(position3: pd.Series) -> pd.Series:
    """``p[3] = 'ground'`` -- and NULL when the source had no value there.

    Spark's equality propagates NULL; a plain pandas comparison would turn a
    missing altitude into False, which the phase labeller reads as airborne.
    0.06% of real observations have no value at position 3.
    """
    flag = pd.Series(position3 == "ground", dtype="boolean")
    flag[position3.isna()] = pd.NA
    return flag


def _first_last(frame: pd.DataFrame, key: str, columns: list[str]):
    """Values on each group's first and last row -- nulls included.

    ``agg("first")`` skips nulls; Spark's ``MIN_BY(col, event_time)`` does not.
    On altitude that is the difference between "on the ground" and "the last
    altitude we happened to see", which airport attribution depends on.
    """
    grouped = frame.groupby(key, sort=False)
    first = grouped.head(1).set_index(key)[columns]
    last = grouped.tail(1).set_index(key)[columns]
    return first, last


def _one_file(path: str) -> pd.DataFrame | None:
    with gzip.open(path, "rb") as handle:
        doc = json.load(handle)
    trace = doc.get("trace") or []
    if not trace:
        return None

    frame = pd.DataFrame(
        [row[:14] + [None] * (14 - len(row)) for row in trace],
        columns=[f"p{i}" for i in range(14)],
    )
    altitude = pd.to_numeric(frame["p3"].where(frame["p3"] != "ground"), errors="coerce")
    out = pd.DataFrame({
        "icao": doc.get("icao"),
        "registration": doc.get("r"),
        "aircraft_type": doc.get("t"),
        "operator": doc.get("ownOp"),
        # microseconds, truncated -- not rounded. Spark casts double to
        # timestamp with (value * 1e6).toLong, which truncates toward zero.
        # Rounding instead moves almost every timestamp by 1 us, which is
        # enough to flip a flight_id across a second boundary and to change
        # which observations fall inside a centred time window.
        "event_time": pd.to_datetime(
            ((doc["timestamp"] + pd.to_numeric(frame["p0"])) * 1_000_000)
            .astype("int64"),
            unit="us",
        ),
        "latitude": pd.to_numeric(frame["p1"], errors="coerce"),
        "longitude": pd.to_numeric(frame["p2"], errors="coerce"),
        "on_ground": _nullable_ground(frame["p3"]),
        "altitude_ft": altitude,
        "ground_speed_kt": pd.to_numeric(frame["p4"], errors="coerce"),
        "track_deg": pd.to_numeric(frame["p5"], errors="coerce"),
        "vertical_rate_fpm": pd.to_numeric(frame["p7"], errors="coerce"),
        "callsign": frame["p8"].map(_callsign),
        "idx": np.arange(len(frame), dtype=np.int32),
    })
    return out


def decode(raw_dir: str, release_tag: str) -> pd.DataFrame:
    """Read every trace file, type it, and resolve same-timestamp collisions.

    The dedup is the same winner rule the Spark decoder applies: the more
    complete reception, then the lower (latitude, longitude), then the earlier
    point in the source array. It runs here on the whole frame rather than per
    array, because pandas has no cheaper place to put it -- there is only one
    table.
    """
    files = sorted(glob.glob(os.path.join(raw_dir, "**", "*.json.gz"), recursive=True))
    frames = [f for f in (_one_file(p) for p in files) if f is not None]
    df = pd.concat(frames, ignore_index=True)
    del frames

    # completeness, on the raw values, exactly as the ranking window saw them
    df["rank_score"] = -(
        df["altitude_ft"].notna().astype("int8")
        + df["ground_speed_kt"].notna().astype("int8")
        + df["track_deg"].notna().astype("int8")
        + df["vertical_rate_fpm"].notna().astype("int8")
        + df["callsign"].notna().astype("int8")
    )
    df = df.sort_values(
        ["icao", "event_time", "rank_score", "latitude", "longitude", "idx"],
        na_position="first",  # Spark orders ASC NULLS FIRST
        kind="stable",
    )
    df = df.drop_duplicates(subset=["icao", "event_time"], keep="first")
    df = df.drop(columns=["rank_score", "idx"])

    # value cleaning, after the ranking
    df.loc[df["ground_speed_kt"] > MAX_GROUND_SPEED_KT, "ground_speed_kt"] = np.nan
    df.loc[
        df["vertical_rate_fpm"].abs() > MAX_VERTICAL_RATE_FPM, "vertical_rate_fpm"
    ] = np.nan
    df["callsign"] = df["callsign"].replace("", None)

    df["is_icao_address"] = ~df["icao"].str.startswith("~")
    df["release_tag"] = release_tag
    # datetime64, not date objects: an object column of dates cannot be
    # aggregated with max(), which Spark's DateType has no trouble with
    df["release_date"] = pd.Timestamp(
        pd.to_datetime(release_tag[1:11], format="%Y.%m.%d")
    )
    # string dtype so NA-aware max() works the way Spark's MAX ignores NULL
    for column in ("icao", "registration", "aircraft_type", "operator",
                   "callsign", "release_tag"):
        df[column] = df[column].astype("string")
    return df.reset_index(drop=True)


def assign_flights(df: pd.DataFrame, gap_seconds: int = GAP_SECONDS) -> pd.DataFrame:
    """Segment each aircraft's observations on tracking gaps."""
    df = df.sort_values(
        ["icao", "event_time", "latitude", "longitude"], na_position="first",
        kind="stable",
    ).reset_index(drop=True)

    df["_epoch"] = epoch_seconds(df["event_time"])
    gap = df.groupby("icao", sort=False)["_epoch"].diff()
    is_break = (gap.isna() | (gap > gap_seconds)).astype("int32")
    df["segment_no"] = is_break.groupby(df["icao"], sort=False).cumsum()

    grouped = df.groupby(["icao", "segment_no"], sort=False)
    first_seen = grouped["event_time"].transform("min")
    df["flight_id"] = df["icao"] + "_" + first_seen.dt.strftime("%Y%m%d%H%M%S")
    df["observation_seq"] = grouped.cumcount().astype("int32") + 1
    return df.drop(columns=["segment_no", "_epoch"])


OBSERVATION_COLUMNS = [
    "flight_id", "icao", "is_icao_address", "registration", "aircraft_type",
    "operator", "event_time", "observation_seq", "latitude", "longitude",
    "on_ground", "altitude_ft", "ground_speed_kt", "track_deg",
    "vertical_rate_fpm", "callsign", "release_tag", "release_date",
]


def to_observations(raw_dir: str, release_tag: str) -> pd.DataFrame:
    return assign_flights(decode(raw_dir, release_tag))[OBSERVATION_COLUMNS]


# --- flights -----------------------------------------------------------------


def _first_non_null(series: pd.Series):
    valid = series.dropna()
    return valid.iloc[0] if len(valid) else None


def aggregate_flights(obs: pd.DataFrame) -> pd.DataFrame:
    """One row per flight. The frame is already in (icao, event_time) order."""
    obs = obs.copy()
    obs["ground_flag"] = obs["on_ground"].fillna(False)  # Spark's CASE WHEN ... ELSE 0
    grouped = obs.groupby("flight_id", sort=False)
    base = grouped.agg(
        icao=("icao", "max"),
        is_icao_address=("is_icao_address", "max"),
        registration=("registration", "max"),
        aircraft_type=("aircraft_type", "max"),
        registered_owner=("operator", "max"),
        callsign=("callsign", _first_non_null),
        n_callsigns=("callsign", "nunique"),
        first_seen_time=("event_time", "min"),
        last_seen_time=("event_time", "max"),
        n_observations=("event_time", "size"),
        saw_ground=("ground_flag", "max"),
        max_altitude_ft=("altitude_ft", "max"),
        max_ground_speed_kt=("ground_speed_kt", "max"),
        release_tag=("release_tag", "max"),
        release_date=("release_date", "max"),
    ).reset_index()

    endpoint_columns = ["latitude", "longitude", "on_ground", "altitude_ft"]
    first, last = _first_last(obs, "flight_id", endpoint_columns)
    first.columns = ["first_latitude", "first_longitude", "started_on_ground",
                     "first_altitude_ft"]
    last.columns = ["last_latitude", "last_longitude", "ended_on_ground",
                    "last_altitude_ft"]
    base = base.merge(first, on="flight_id").merge(last, on="flight_id")
    base["duration_seconds"] = (
        epoch_seconds(base["last_seen_time"]) - epoch_seconds(base["first_seen_time"])
    )
    return base


def _haversine_km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    return EARTH_RADIUS_KM * 2 * np.arcsin(
        np.sqrt(
            np.sin((lat2 - lat1) / 2) ** 2
            + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
        )
    )


def airport_movements(
    flights: pd.DataFrame, airports: pd.DataFrame, radius_km: float = MATCH_RADIUS_KM
) -> pd.DataFrame:
    """Flight endpoints matched to the nearest qualifying airport."""
    moving = flights[
        ~(
            (flights["first_latitude"] == flights["last_latitude"])
            & (flights["first_longitude"] == flights["last_longitude"])
        )
    ]
    carried = ["flight_id", "icao", "aircraft_type", "registered_owner", "callsign",
               "release_tag", "release_date"]
    ends = []
    for kind, prefix, time_col in (
        ("departure", "first", "first_seen_time"),
        ("arrival", "last", "last_seen_time"),
    ):
        part = moving[carried + [time_col, f"{prefix}_latitude",
                                 f"{prefix}_longitude", f"{prefix}_altitude_ft"]].copy()
        part.columns = carried + ["event_time", "latitude", "longitude", "altitude_ft"]
        part["movement_type"] = kind
        ends.append(part)
    endpoints = pd.concat(ends, ignore_index=True)

    # the same 3x3 one-degree cell index the Spark join uses, so neither side
    # pays for a cross product against all 5,281 airports
    cells = airports.loc[airports.index.repeat(9)].copy()
    offsets = np.tile(np.array([-1, 0, 1]), 3), np.repeat(np.array([-1, 0, 1]), 3)
    cells["cell_lat"] = np.floor(cells["latitude_deg"]) + np.tile(
        offsets[0], len(airports)
    )[: len(cells)]
    cells["cell_lon"] = np.floor(cells["longitude_deg"]) + np.tile(
        offsets[1], len(airports)
    )[: len(cells)]

    endpoints["cell_lat"] = np.floor(endpoints["latitude"])
    endpoints["cell_lon"] = np.floor(endpoints["longitude"])
    candidates = endpoints.merge(cells, on=["cell_lat", "cell_lon"], how="inner")

    candidates["distance_km"] = _haversine_km(
        candidates["latitude"], candidates["longitude"],
        candidates["latitude_deg"], candidates["longitude_deg"],
    )
    height = candidates["altitude_ft"] - candidates["elevation_ft"].fillna(0)
    qualifying = candidates[
        (candidates["distance_km"] <= radius_km)
        & (candidates["altitude_ft"].isna() | (height <= MAX_HEIGHT_ABOVE_AIRPORT_FT))
    ]
    nearest = (
        qualifying.sort_values(["distance_km", "ident"], kind="stable")
        .drop_duplicates(subset=["flight_id", "movement_type"], keep="first")
    )
    return nearest.rename(columns={"name": "airport_name", "type": "airport_type"})[[
        "flight_id", "icao", "aircraft_type", "registered_owner", "callsign",
        "movement_type", "event_time", "latitude", "longitude", "altitude_ft",
        "ident", "iata_code", "airport_name", "airport_type", "iso_country",
        "latitude_deg", "longitude_deg", "elevation_ft", "distance_km",
        "release_tag", "release_date",
    ]].reset_index(drop=True)


def movement_pivot(movements: pd.DataFrame) -> pd.DataFrame:
    parts = []
    for kind in ("departure", "arrival"):
        side = movements[movements["movement_type"] == kind]
        side = side.set_index("flight_id")[
            ["ident", "iata_code", "airport_name", "event_time", "distance_km"]
        ]
        side.columns = [f"{kind}_airport_ident", f"{kind}_airport_iata",
                        f"{kind}_airport_name", f"{kind}_time",
                        f"{kind}_distance_km"]
        parts.append(side)
    return parts[0].join(parts[1], how="outer").reset_index()


def to_flights(base: pd.DataFrame, pivot: pd.DataFrame, holds: pd.DataFrame):
    flights = base.merge(pivot, on="flight_id", how="left")
    rollup = (
        holds.groupby("flight_id")
        .agg(n_detected_holds=("flight_id", "size"),
             total_hold_seconds=("duration_seconds", "sum"))
        .reset_index()
        if len(holds)
        else pd.DataFrame(columns=["flight_id", "n_detected_holds", "total_hold_seconds"])
    )
    flights = flights.merge(rollup, on="flight_id", how="left")
    flights["n_detected_holds"] = flights["n_detected_holds"].fillna(0).astype("int64")
    flights["total_hold_seconds"] = (
        flights["total_hold_seconds"].fillna(0).astype("int64")
    )
    flights["has_detected_hold"] = flights["n_detected_holds"] > 0
    flights["flight_date"] = flights["first_seen_time"].dt.date
    airline = flights["callsign"].fillna("").str.extract(f"({AIRLINE_CALLSIGN})")[0]
    flights["airline_icao"] = airline.str[:3]
    return flights


# --- phases ------------------------------------------------------------------


def _centred(frame: pd.DataFrame, column: str, seconds: int, how: str) -> pd.Series:
    """A time-centred window per flight, matching Spark's RANGE BETWEEN.

    Spark's window is over UNIX_TIMESTAMP(event_time) -- whole seconds -- and
    inclusive at both ends, so the index is floored to the second and the
    window is (2*half + 1) seconds wide with both edges closed.
    """
    half = seconds // 2
    out = []
    for _, group in frame.groupby("flight_id", sort=False):
        indexed = group.set_index("event_second")
        rolled = getattr(
            indexed[column].rolling(f"{2 * half + 1}s", center=True, closed="both"), how
        )()
        out.append(pd.Series(rolled.to_numpy(), index=group.index))
    return pd.concat(out) if out else pd.Series(dtype="float64")


def label_observations(obs: pd.DataFrame) -> pd.DataFrame:
    frame = obs[[
        "flight_id", "event_time", "observation_seq", "release_date",
        "altitude_ft", "on_ground", "ground_speed_kt", "vertical_rate_fpm",
    ]].copy()
    frame["event_second"] = frame["event_time"].dt.floor("s")
    frame["smoothed_vertical_rate_fpm"] = _centred(
        frame, "vertical_rate_fpm", SMOOTHING_WINDOW_SECONDS, "mean"
    )

    airborne_seq = frame["observation_seq"].where(
        ~frame["on_ground"].fillna(True)
    )
    first_air = airborne_seq.groupby(frame["flight_id"], sort=False).transform("min")
    last_air = airborne_seq.groupby(frame["flight_id"], sort=False).transform("max")

    rate = frame["smoothed_vertical_rate_fpm"]
    ground = frame["on_ground"].fillna(False).to_numpy(dtype=bool)
    phase = np.select(
        [
            frame["on_ground"].isna().to_numpy(),
            ground & first_air.isna().to_numpy(),
            ground & (frame["observation_seq"] < first_air).fillna(False).to_numpy(),
            ground & (frame["observation_seq"] > last_air).fillna(False).to_numpy(),
            ground,
            rate.isna().to_numpy(),
            (rate > LEVEL_BAND_FPM).to_numpy(),
            (rate < -LEVEL_BAND_FPM).to_numpy(),
        ],
        ["unknown", "taxi", "taxi_out", "taxi_in", "taxi", "unknown", "climb",
         "descent"],
        default="cruise",
    )
    frame["phase"] = phase
    return frame


def _runs(frame: pd.DataFrame, column: str) -> pd.Series:
    """Run number within each flight, collapsing consecutive equal values."""
    changed = frame[column] != frame.groupby("flight_id", sort=False)[column].shift()
    return changed.astype("int32").groupby(frame["flight_id"], sort=False).cumsum()


def to_flight_phases(obs: pd.DataFrame) -> pd.DataFrame:
    labelled = label_observations(obs).sort_values(
        ["flight_id", "observation_seq"], kind="stable"
    )
    labelled["run_no"] = _runs(labelled, "phase")
    labelled["run_key"] = (
        labelled["flight_id"] + "#" + labelled["run_no"].astype(str)
    )
    intervals = labelled.groupby("run_key", sort=False).agg(
        flight_id=("flight_id", "max"),
        phase=("phase", "max"),
        start_time=("event_time", "min"),
        end_time=("event_time", "max"),
        n_observations=("event_time", "size"),
        min_altitude_ft=("altitude_ft", "min"),
        max_altitude_ft=("altitude_ft", "max"),
        avg_ground_speed_kt=("ground_speed_kt", "mean"),
        avg_vertical_rate_fpm=("smoothed_vertical_rate_fpm", "mean"),
        release_date=("release_date", "max"),
    ).reset_index()
    first, last = _first_last(labelled, "run_key", ["altitude_ft"])
    first.columns = ["start_altitude_ft"]
    last.columns = ["end_altitude_ft"]
    intervals = intervals.merge(first, on="run_key").merge(last, on="run_key")
    intervals["duration_seconds"] = (
        epoch_seconds(intervals["end_time"]) - epoch_seconds(intervals["start_time"])
    )
    intervals["avg_ground_speed_kt"] = intervals["avg_ground_speed_kt"].round(1)
    intervals["avg_vertical_rate_fpm"] = intervals["avg_vertical_rate_fpm"].round(1)
    intervals = intervals.sort_values(["flight_id", "start_time"], kind="stable")
    intervals["phase_seq"] = (
        intervals.groupby("flight_id", sort=False).cumcount().astype("int32") + 1
    )
    return intervals.drop(columns=["run_key"])


# --- holds -------------------------------------------------------------------


def to_flight_holds(
    obs: pd.DataFrame, pivot: pd.DataFrame, airports: pd.DataFrame
) -> pd.DataFrame:
    frame = obs[
        (~obs["on_ground"].fillna(True)) & obs["track_deg"].notna()
    ][[
        "flight_id", "event_time", "observation_seq", "release_date",
        "latitude", "longitude", "altitude_ft", "track_deg",
    ]].copy()
    if frame.empty:
        return pd.DataFrame(columns=["flight_id", "hold_seq"])

    frame = frame.sort_values(["flight_id", "observation_seq"], kind="stable")
    frame["_epoch"] = epoch_seconds(frame["event_time"])
    grouped = frame.groupby("flight_id", sort=False)
    step_seconds = grouped["_epoch"].diff()
    step_turn = ((frame["track_deg"] - grouped["track_deg"].shift()) + 180) % 360 - 180
    frame["step_seconds"] = step_seconds
    frame["counted_turn"] = np.where(
        step_seconds.between(1, MAX_STEP_GAP_SECONDS), step_turn, 0.0
    )

    frame["event_second"] = frame["event_time"].dt.floor("s")
    frame["turn_in_window_degrees"] = _centred(
        frame, "counted_turn", TURN_WINDOW_SECONDS, "sum"
    )
    frame["circling"] = (
        frame["turn_in_window_degrees"].abs() >= MIN_TURN_DEGREES
    ).astype("int32")
    frame["run_no"] = _runs(frame, "circling")

    circling = frame[frame["circling"] == 1]
    if circling.empty:
        return pd.DataFrame(columns=["flight_id", "hold_seq"])

    def _strongest(series: pd.Series) -> float:
        return series.iloc[series.abs().to_numpy().argmax()]

    candidates = circling.groupby(["flight_id", "run_no"], sort=False).agg(
        hold_start=("event_time", "min"),
        hold_end=("event_time", "max"),
        n_observations=("event_time", "size"),
        centroid_latitude=("latitude", "mean"),
        centroid_longitude=("longitude", "mean"),
        min_lat=("latitude", "min"), max_lat=("latitude", "max"),
        min_lon=("longitude", "min"), max_lon=("longitude", "max"),
        min_altitude_ft=("altitude_ft", "min"),
        max_altitude_ft=("altitude_ft", "max"),
        mean_altitude_ft=("altitude_ft", "mean"),
        turn_degrees=("turn_in_window_degrees", _strongest),
        peak_turn=("turn_in_window_degrees", lambda s: s.abs().max()),
        max_sample_gap_seconds=("step_seconds", "max"),
        release_date=("release_date", "max"),
    ).reset_index()

    candidates["duration_seconds"] = (
        epoch_seconds(candidates["hold_end"]) - epoch_seconds(candidates["hold_start"])
    )
    candidates["span_km"] = np.round(np.hypot(
        (candidates["max_lat"] - candidates["min_lat"]) * KM_PER_DEGREE_LATITUDE,
        (candidates["max_lon"] - candidates["min_lon"]) * KM_PER_DEGREE_LATITUDE
        * np.cos(np.radians(candidates["centroid_latitude"])),
    ), 2)
    candidates["centroid_latitude"] = candidates["centroid_latitude"].round(5)
    candidates["centroid_longitude"] = candidates["centroid_longitude"].round(5)
    candidates["mean_altitude_ft"] = candidates["mean_altitude_ft"].round()
    candidates["turn_degrees"] = candidates["turn_degrees"].round(1)
    candidates["circuits"] = (candidates["peak_turn"] / 360.0).round(1)

    kept = candidates[
        (candidates["duration_seconds"] >= MIN_DURATION_SECONDS)
        & (candidates["span_km"] <= MAX_SPAN_KM)
        & (candidates["max_altitude_ft"] - candidates["min_altitude_ft"]
           <= MAX_ALTITUDE_RANGE_FT)
    ].copy()
    if kept.empty:
        return pd.DataFrame(columns=["flight_id", "hold_seq"])

    kept = kept.merge(
        pivot[["flight_id", "arrival_airport_ident", "arrival_airport_iata"]],
        on="flight_id", how="left",
    ).merge(
        airports[["ident", "latitude_deg", "longitude_deg"]],
        left_on="arrival_airport_ident", right_on="ident", how="left",
    )
    kept["distance_to_arrival_airport_km"] = np.where(
        kept["latitude_deg"].isna(),
        np.nan,
        np.round(_haversine_km(
            kept["centroid_latitude"], kept["centroid_longitude"],
            kept["latitude_deg"], kept["longitude_deg"],
        ), 1),
    )
    kept = kept.sort_values(["flight_id", "hold_start"], kind="stable")
    kept["hold_seq"] = kept.groupby("flight_id", sort=False).cumcount().astype("int32") + 1
    return kept[[
        "flight_id", "hold_seq", "hold_start", "hold_end", "duration_seconds",
        "n_observations", "centroid_latitude", "centroid_longitude", "span_km",
        "min_altitude_ft", "max_altitude_ft", "mean_altitude_ft", "turn_degrees",
        "circuits", "max_sample_gap_seconds", "arrival_airport_ident",
        "arrival_airport_iata", "distance_to_arrival_airport_km", "release_date",
    ]].reset_index(drop=True)


# --- airport operations ------------------------------------------------------


def to_airport_operations(movements: pd.DataFrame, holds: pd.DataFrame) -> pd.DataFrame:
    frame = movements.copy()
    frame["operations_date"] = frame["event_time"].dt.date
    metrics = frame.groupby([
        "operations_date", "ident", "iata_code", "airport_name", "airport_type",
        "iso_country", "latitude_deg", "longitude_deg",
    ], dropna=False).agg(
        arrivals=("movement_type", lambda s: int((s == "arrival").sum())),
        departures=("movement_type", lambda s: int((s == "departure").sum())),
        total_operations=("movement_type", "size"),
        unique_aircraft=("icao", "nunique"),
        first_operation_time=("event_time", "min"),
        last_operation_time=("event_time", "max"),
        release_tag=("release_tag", "max"),
        release_date=("release_date", "max"),
    ).reset_index().rename(columns={
        "ident": "airport_ident", "iata_code": "airport_iata",
        "latitude_deg": "airport_latitude", "longitude_deg": "airport_longitude",
    })
    metrics["metric_source"] = "adsb_inferred"

    if len(holds):
        held = holds[holds["arrival_airport_ident"].notna()].copy()
        held["operations_date"] = held["hold_start"].dt.date
        rollup = held.groupby(["operations_date", "arrival_airport_ident"]).agg(
            flights_with_detected_holds=("flight_id", "nunique"),
            avg_hold_duration_seconds=("duration_seconds", "mean"),
        ).reset_index().rename(columns={"arrival_airport_ident": "airport_ident"})
        rollup["avg_hold_duration_seconds"] = rollup["avg_hold_duration_seconds"].round()
        metrics = metrics.merge(rollup, on=["operations_date", "airport_ident"],
                                how="left")
    else:
        metrics["flights_with_detected_holds"] = 0
        metrics["avg_hold_duration_seconds"] = np.nan
    metrics["flights_with_detected_holds"] = (
        metrics["flights_with_detected_holds"].fillna(0).astype("int64")
    )
    metrics["hold_rate"] = np.where(
        metrics["arrivals"] > 0,
        (metrics["flights_with_detected_holds"] / metrics["arrivals"]).round(4),
        np.nan,
    )
    return metrics


# --- the run -----------------------------------------------------------------


def run(raw_dir: str, release_tag: str, airports_csv: str, out_dir: str) -> dict:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    airports = pd.read_csv(airports_csv)

    observations = to_observations(raw_dir, release_tag)
    base = aggregate_flights(observations)
    movements = airport_movements(base, airports)
    pivot = movement_pivot(movements)
    phases = to_flight_phases(observations)
    holds = to_flight_holds(observations, pivot, airports)
    flights = to_flights(base, pivot, holds)
    operations = to_airport_operations(movements, holds)

    tables = {
        "observations": observations,
        "flights": flights,
        "movements": movements,
        "flight_phases": phases,
        "flight_holds": holds,
        "airport_daily_operations": operations,
    }
    for name, frame in tables.items():
        frame = frame.copy()
        if "release_date" in frame.columns:
            frame["release_date"] = pd.to_datetime(frame["release_date"]).dt.date
        frame.to_parquet(out / f"{name}.parquet", index=False)
    return {name: len(frame) for name, frame in tables.items()}


def main(argv: list[str] | None = None) -> None:
    import argparse
    import json as _json

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--raw", required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--airports", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)

    counts = run(args.raw, args.tag, args.airports, args.out)
    print("ROW_COUNTS " + _json.dumps(counts))


if __name__ == "__main__":
    main()
