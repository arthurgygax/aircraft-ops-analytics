"""Tests for the app's data access and filtering.

Rendering is not tested: these cover the logic that decides *what* is shown.
Skipped in the pipeline container, which has no delta-rs; run them with
``docker compose run --rm explorer pytest tests/test_app_data.py``.
"""

from datetime import datetime

import pytest

pytest.importorskip("deltalake", reason="app tests run in the explorer container")

import pandas as pd  # noqa: E402

from app import data  # noqa: E402


def flights_frame() -> pd.DataFrame:
    return pd.DataFrame([
        {"flight_id": "a_1", "flight_date": "2025-12-30", "callsign": "SWR123",
         "icao": "aaa111", "departure_airport_iata": "ZRH",
         "arrival_airport_iata": "LHR", "airline_icao": "SWR",
         "aircraft_type": "A320", "first_seen_time": datetime(2025, 12, 30, 8, 0)},
        {"flight_id": "b_2", "flight_date": "2025-12-30", "callsign": "BAW456",
         "icao": "bbb222", "departure_airport_iata": "LHR",
         "arrival_airport_iata": "ZRH", "airline_icao": "BAW",
         "aircraft_type": "A320", "first_seen_time": datetime(2025, 12, 30, 9, 0)},
        {"flight_id": "c_3", "flight_date": "2025-12-31", "callsign": None,
         "icao": "ccc333", "departure_airport_iata": None,
         "arrival_airport_iata": None, "airline_icao": None,
         "aircraft_type": None, "first_seen_time": datetime(2025, 12, 31, 7, 0)},
    ])


# --- filtering ---------------------------------------------------------------


def test_no_selection_returns_everything():
    flights = flights_frame()

    assert len(data.filter_flights(flights, {})) == 3


def test_each_filter_narrows_the_result():
    flights = flights_frame()

    assert len(data.filter_flights(flights, {"airline_icao": "SWR"})) == 1
    assert len(data.filter_flights(flights, {"aircraft_type": "A320"})) == 2
    assert len(data.filter_flights(flights, {"flight_date": "2025-12-31"})) == 1


def test_filters_combine():
    flights = flights_frame()

    result = data.filter_flights(
        flights, {"aircraft_type": "A320", "arrival_airport_iata": "ZRH"}
    )

    assert list(result["flight_id"]) == ["b_2"]


def test_search_matches_callsign_or_aircraft_address():
    flights = flights_frame()

    assert list(data.filter_flights(flights, {}, "swr")["flight_id"]) == ["a_1"]
    assert list(data.filter_flights(flights, {}, "ccc333")["flight_id"]) == ["c_3"]
    assert data.filter_flights(flights, {}, "nothing").empty


def test_options_exclude_missing_values():
    """A flight with no airline must not create a blank dropdown entry."""
    options = data.options_for(flights_frame(), "airline_icao")

    assert options == ["BAW", "SWR"]


def test_options_narrow_with_the_frame():
    flights = flights_frame()
    narrowed = data.filter_flights(flights, {"airline_icao": "SWR"})

    assert data.options_for(narrowed, "arrival_airport_iata") == ["LHR"]


# --- display helpers ---------------------------------------------------------


def test_a_flight_without_a_callsign_is_labelled_by_its_address():
    row = flights_frame().iloc[2]

    assert data.flight_label(row).startswith("ccc333")
    assert "??? → ???" in data.flight_label(row)


def test_missing_text_renders_as_a_dash_not_as_nan():
    """pandas NaN is truthy, so `value or fallback` would print 'nan'."""
    assert data.text_or(float("nan")) == "—"
    assert data.text_or(None) == "—"
    assert data.text_or("A320") == "A320"
    assert data.text_or(float("nan"), "unknown") == "unknown"


@pytest.mark.parametrize("seconds,expected", [
    (0, "0s"), (45, "45s"), (95, "1m 35s"), (3600, "1h 00m"), (5040, "1h 24m"),
])
def test_durations_read_as_time_not_as_seconds(seconds, expected):
    assert data.format_duration(seconds) == expected


def test_a_missing_duration_renders_as_a_dash():
    assert data.format_duration(None) == "—"
    assert data.format_duration(float("nan")) == "—"


# --- joining points to phases ------------------------------------------------


def points_frame(times):
    return pd.DataFrame({
        "event_time": pd.to_datetime(times, format="ISO8601"),
        "latitude": [47.0] * len(times),
        "longitude": [8.0] * len(times),
    })


def phases_frame():
    return pd.DataFrame({
        "start_time": pd.to_datetime(["2025-12-30 08:00", "2025-12-30 08:10"],
                                     format="ISO8601"),
        "end_time": pd.to_datetime(["2025-12-30 08:09", "2025-12-30 08:20"],
                                   format="ISO8601"),
        "phase": ["climb", "cruise"],
    })


def test_each_point_takes_the_phase_of_its_interval():
    points = points_frame(["2025-12-30 08:05", "2025-12-30 08:15"])

    labelled = data.label_points_with_phase(points, phases_frame())

    assert list(labelled["phase"]) == ["climb", "cruise"]


def test_a_point_outside_every_interval_is_unknown():
    """Between or after intervals there is no detected phase to claim."""
    points = points_frame(["2025-12-30 07:00", "2025-12-30 08:09:30",
                           "2025-12-30 09:00"])

    labelled = data.label_points_with_phase(points, phases_frame())

    assert list(labelled["phase"]) == ["unknown", "unknown", "unknown"]


def test_points_survive_when_a_flight_has_no_phases():
    points = points_frame(["2025-12-30 08:05"])

    labelled = data.label_points_with_phase(points, pd.DataFrame())

    assert len(labelled) == 1
    assert labelled["phase"].iloc[0] == "unknown"


# --- airport hourly traffic --------------------------------------------------


def test_hourly_traffic_counts_both_directions_across_all_24_hours():
    flights = pd.DataFrame([
        {"departure_airport_iata": "ZRH", "arrival_airport_iata": "LHR",
         "departure_time": datetime(2025, 12, 30, 8, 30),
         "arrival_time": datetime(2025, 12, 30, 10, 5)},
        {"departure_airport_iata": "LHR", "arrival_airport_iata": "ZRH",
         "departure_time": datetime(2025, 12, 30, 9, 0),
         "arrival_time": datetime(2025, 12, 30, 8, 45)},
    ])

    hourly = data.hourly_traffic(flights, "ZRH")

    assert len(hourly) == 24, "every hour present, including quiet ones"
    assert hourly.loc[hourly["hour"] == 8, "Departures"].item() == 1
    assert hourly.loc[hourly["hour"] == 8, "Arrivals"].item() == 1
    assert hourly.loc[hourly["hour"] == 10, "Arrivals"].item() == 0


def test_hourly_traffic_ignores_flights_with_no_detected_time():
    flights = pd.DataFrame([
        {"departure_airport_iata": "ZRH", "arrival_airport_iata": None,
         "departure_time": None, "arrival_time": None},
    ])

    hourly = data.hourly_traffic(flights, "ZRH")

    assert hourly["Departures"].sum() == 0
