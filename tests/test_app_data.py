"""Tests for the app's data access and filtering.

Rendering is not tested: these cover the logic that decides *what* is shown.
Skipped in the pipeline container, which has no delta-rs; run them with
``docker compose run --rm explorer pytest tests/test_app_data.py``.
"""

from datetime import date, datetime

import pytest

pytest.importorskip("deltalake", reason="app tests run in the explorer container")

import pandas as pd  # noqa: E402

from app import data  # noqa: E402


def flight(flight_id, day, callsign, icao, dep, arr, airline, kind,
           dep_hour=8, arr_hour=10, holds=0):
    iata = {"LSZH": "ZRH", "EDDL": "DUS", "EGLL": "LHR", "EDDM": "MUC"}
    return {
        "flight_id": flight_id,
        "flight_date": day,
        "callsign": callsign,
        "icao": icao,
        "registration": "HB-ABC",
        "departure_airport_ident": dep,
        "departure_airport_iata": iata.get(dep),
        "departure_airport_name": dep,
        "arrival_airport_ident": arr,
        "arrival_airport_iata": iata.get(arr),
        "arrival_airport_name": arr,
        "airline_icao": airline,
        "aircraft_type": kind,
        "first_seen_time": datetime(2025, 12, int(day[-2:]), dep_hour, 0),
        "departure_time": datetime(2025, 12, int(day[-2:]), dep_hour, 5),
        "arrival_time": datetime(2025, 12, int(day[-2:]), arr_hour, 0),
        "duration_seconds": 5040,
        "n_observations": 700,
        "n_detected_holds": holds,
        "has_detected_hold": holds > 0,
        "max_altitude_ft": 37000.0,
        "max_ground_speed_kt": 450.0,
    }


def flights_frame() -> pd.DataFrame:
    """A small board covering both study airports and both directions."""
    return pd.DataFrame([
        # Saturday
        flight("f1", "2025-12-27", "SWR123", "aaa111", "LSZH", "EGLL", "SWR", "A320"),
        flight("f2", "2025-12-27", "DLH456", "bbb222", "LSZH", "EDDL", "DLH", "A320"),
        flight("f3", "2025-12-27", "EWG789", "ccc333", "EDDL", "LSZH", "EWG", "BCS3"),
        # Sunday
        flight("f4", "2025-12-28", "SWR777", "ddd444", "EGLL", "LSZH", "SWR", "A321"),
        flight("f5", "2025-12-28", "DLH999", "eee555", "EDDM", "EDDL", "DLH", "A320",
               holds=2),
        # a flight the pipeline could not attribute at either end
        flight("f6", "2025-12-28", None, "fff666", None, None, None, None),
    ])


# --- filter semantics: the questions the app has to answer --------------------


def test_no_filters_returns_everything():
    assert len(data.select_flights(flights_frame())) == 6


def test_all_flights_departing_zrh():
    result = data.select_flights(flights_frame(), departure="LSZH")

    assert set(result["flight_id"]) == {"f1", "f2"}


def test_all_flights_arriving_at_dus():
    result = data.select_flights(flights_frame(), arrival="EDDL")

    assert set(result["flight_id"]) == {"f2", "f5"}


def test_departure_and_arrival_are_not_the_same_filter():
    """The reason there is no single 'airport' box: these differ."""
    departing = data.select_flights(flights_frame(), departure="LSZH")
    arriving = data.select_flights(flights_frame(), arrival="LSZH")

    assert set(departing["flight_id"]) == {"f1", "f2"}
    assert set(arriving["flight_id"]) == {"f3", "f4"}


def test_swiss_flights_departing_zrh():
    result = data.select_flights(flights_frame(), departure="LSZH", airline="SWR")

    assert list(result["flight_id"]) == ["f1"]


def test_a320_flights_arriving_at_dus():
    result = data.select_flights(flights_frame(), arrival="EDDL", aircraft_type="A320")

    assert set(result["flight_id"]) == {"f2", "f5"}


def test_lufthansa_flights_between_zrh_and_dus():
    result = data.select_flights(
        flights_frame(), departure="LSZH", arrival="EDDL", airline="DLH"
    )

    assert list(result["flight_id"]) == ["f2"]


def test_all_flights_on_one_day():
    result = data.select_flights(flights_frame(), date=date(2025, 12, 27))

    assert set(result["flight_id"]) == {"f1", "f2", "f3"}


def test_the_date_filter_accepts_a_string_or_a_date():
    frame = flights_frame()

    assert len(data.select_flights(frame, date="2025-12-28")) == 3
    assert len(data.select_flights(frame, date=date(2025, 12, 28))) == 3


def test_callsign_search_matches_callsign_or_address():
    frame = flights_frame()

    assert list(data.select_flights(frame, callsign="swr")["flight_id"]) == ["f1", "f4"]
    assert list(data.select_flights(frame, callsign="fff666")["flight_id"]) == ["f6"]


def test_filters_that_match_nothing_return_an_empty_frame():
    result = data.select_flights(flights_frame(), departure="LSZH", airline="EWG")

    assert result.empty
    assert list(result.columns) == list(flights_frame().columns)


def test_a_flight_with_no_airports_survives_an_unfiltered_query():
    """Unattributed flights are real observations, not rows to hide by default."""
    assert "f6" in set(data.select_flights(flights_frame())["flight_id"])


# --- filter options ----------------------------------------------------------


def test_options_exclude_missing_values():
    assert data.options_for(flights_frame(), "airline_icao") == ["DLH", "EWG", "SWR"]


def test_airport_options_lead_with_the_study_airports():
    """ZRH and DUS are four fifths of both columns; they go first."""
    options = data.airport_options(flights_frame(), "departure_airport_ident")

    assert options[:2] == ["LSZH", "EDDL"]
    assert set(options) == {"LSZH", "EDDL", "EGLL", "EDDM"}


def test_airport_labels_carry_the_iata_code_people_know():
    assert data.airport_label("LSZH") == "LSZH · ZRH"
    assert data.airport_label("EDDL") == "EDDL · DUS"
    assert data.airport_label("EGLL") == "EGLL"


# --- flight selection --------------------------------------------------------


def test_the_flight_table_is_indexed_by_flight_id_so_a_row_maps_back():
    """Row selection returns positions; the id is how they become flights."""
    table = data.flight_table(flights_frame())

    assert list(table.index) == ["f1", "f2", "f3", "f4", "f5", "f6"]
    assert table.index[0] == "f1"


def test_the_flight_table_is_ordered_by_first_observation():
    frame = flights_frame()
    # f1 starts the day; push it to the end of its own day
    frame.loc[frame["flight_id"] == "f1", "first_seen_time"] = datetime(2025, 12, 27, 23)

    saturday = data.flight_table(
        data.select_flights(frame, date=date(2025, 12, 27))
    )

    assert list(saturday.index) == ["f2", "f3", "f1"]


def test_the_flight_table_carries_the_columns_the_brief_asks_for():
    table = data.flight_table(flights_frame())

    for column in ("Callsign", "Date", "From", "To", "Airline", "Aircraft",
                   "Dep", "Arr", "Duration"):
        assert column in table.columns


def test_the_flight_table_renders_missing_values_as_dashes():
    table = data.flight_table(flights_frame())
    unattributed = table.loc["f6"]

    assert unattributed["From"] == "—" and unattributed["To"] == "—"
    assert unattributed["Callsign"] == "fff666", "falls back to the address"


def test_an_empty_flight_table_still_has_its_columns():
    table = data.flight_table(flights_frame().iloc[0:0])

    assert table.empty
    assert "Callsign" in table.columns


# --- trajectory retrieval ----------------------------------------------------


class Recorder:
    """Stands in for the Delta read so the pushed-down filters are inspectable."""

    def __init__(self, frame=None):
        self.filters = None
        self.frame = frame if frame is not None else pd.DataFrame(
            columns=data.TRACK_COLUMNS)

    def __call__(self, name, filters=None):
        self.name, self.filters = name, filters
        return self.frame


def test_a_trajectory_query_prunes_by_date_as_well_as_flight(monkeypatch):
    """Without the date it opens all seven partitions: 1,538 ms against 320."""
    recorder = Recorder()
    monkeypatch.setattr(data, "read_table", recorder)

    data.load_trajectories(["f1", "f2"], [date(2025, 12, 27)])

    assert recorder.name == "tracks"
    assert ("release_date", "in", [date(2025, 12, 27)]) in recorder.filters
    assert ("flight_id", "in", ["f1", "f2"]) in recorder.filters


def test_dates_are_normalised_to_dates(monkeypatch):
    """pyarrow will not compare a date32 partition against a string."""
    recorder = Recorder()
    monkeypatch.setattr(data, "read_table", recorder)

    data.load_trajectories(["f1"], ["2025-12-27", datetime(2025, 12, 27, 9, 0)])

    partition = [f for f in recorder.filters if f[0] == "release_date"][0]
    assert partition[2] == [date(2025, 12, 27)], "deduplicated and typed"


def test_selecting_no_flights_reads_nothing_at_all(monkeypatch):
    """The empty selection must not turn into a scan of 3.3M points."""
    recorder = Recorder()
    monkeypatch.setattr(data, "read_table", recorder)

    result = data.load_trajectories([], [date(2025, 12, 27)])

    assert result.empty
    assert list(result.columns) == data.TRACK_COLUMNS
    assert recorder.filters is None, "no query was issued"


def test_duplicate_flight_ids_are_asked_for_once(monkeypatch):
    recorder = Recorder()
    monkeypatch.setattr(data, "read_table", recorder)

    data.load_trajectories(["f1", "f1", "f2"], None)

    assert ("flight_id", "in", ["f1", "f2"]) in recorder.filters


def test_a_trajectory_comes_back_ordered_within_each_flight(monkeypatch):
    points = pd.DataFrame({
        "flight_id": ["f2", "f1", "f1"],
        "observation_seq": [1, 2, 1],
        "latitude": [51.0, 47.1, 47.0],
        "longitude": [6.0, 8.1, 8.0],
    })
    monkeypatch.setattr(data, "read_table", Recorder(points))

    result = data.load_trajectories(["f1", "f2"], None)

    assert list(result["flight_id"]) == ["f1", "f1", "f2"]
    assert list(result["observation_seq"]) == [1, 2, 1]


def test_one_flight_is_the_same_call_as_many(monkeypatch):
    recorder = Recorder()
    monkeypatch.setattr(data, "read_table", recorder)

    data.load_trajectory("f1", date(2025, 12, 27))

    assert ("flight_id", "in", ["f1"]) in recorder.filters


# --- phase retrieval ---------------------------------------------------------


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
    labelled = data.label_points_with_phase(points_frame(["2025-12-30 08:05"]),
                                            pd.DataFrame())

    assert len(labelled) == 1
    assert labelled["phase"].iloc[0] == "unknown"


def test_every_detected_phase_has_a_colour_and_a_label():
    """A phase the pipeline emits but the app cannot draw would vanish."""
    emitted = {"climb", "cruise", "descent", "taxi_out", "taxi_in", "taxi", "unknown"}

    assert emitted <= set(data.PHASE_ORDER)
    assert emitted <= set(data.PHASE_LABELS)
    assert emitted <= set(data.PHASE_COLOURS_LIGHT)
    assert emitted <= set(data.PHASE_COLOURS_DARK)


# --- hold retrieval ----------------------------------------------------------


def holds_frame():
    return pd.DataFrame({
        "hold_seq": [1],
        "hold_start": pd.to_datetime(["2025-12-30 08:05"], format="ISO8601"),
        "hold_end": pd.to_datetime(["2025-12-30 08:12"], format="ISO8601"),
    })


def test_the_points_inside_a_hold_are_the_ones_drawn_as_the_hold():
    points = points_frame(["2025-12-30 08:00", "2025-12-30 08:06",
                           "2025-12-30 08:10", "2025-12-30 08:30"])

    held = data.points_in_holds(points, holds_frame())

    assert len(held) == 2
    assert list(held["event_time"].dt.strftime("%H:%M")) == ["08:06", "08:10"]


def test_a_flight_with_no_hold_highlights_nothing():
    points = points_frame(["2025-12-30 08:00"])

    assert data.points_in_holds(points, pd.DataFrame()).empty


def test_a_hold_with_no_points_in_range_highlights_nothing():
    points = points_frame(["2025-12-30 09:00"])

    assert data.points_in_holds(points, holds_frame()).empty


# --- airport view ------------------------------------------------------------


def test_airport_flights_include_both_directions():
    both = data.airport_flights(flights_frame(), "ZRH")

    assert set(both["flight_id"]) == {"f1", "f2", "f3", "f4"}


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

    assert data.hourly_traffic(flights, "ZRH")["Departures"].sum() == 0


def test_a_distribution_folds_its_tail_instead_of_truncating_it():
    """The bars must still add up, or a long tail reads as absence."""
    frame = pd.DataFrame({"airline_icao": list("aabbccdde") + ["f"]})

    top = data.distribution(frame, "airline_icao", top=2)

    assert list(top["value"])[:2] == ["a", "b"]
    assert top["value"].iloc[-1].startswith("Other")
    assert top["flights"].sum() == 10


def test_a_distribution_of_nothing_is_empty_not_an_error():
    assert data.distribution(pd.DataFrame(), "airline_icao").empty


# --- display helpers ---------------------------------------------------------


def test_a_flight_without_a_callsign_is_labelled_by_its_address():
    row = flights_frame().set_index("flight_id").loc["f6"]

    assert data.flight_label(row).startswith("fff666")
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


def test_a_missing_clock_time_renders_as_a_dash():
    assert data.format_clock(None) == "—"
    assert data.format_clock(datetime(2025, 12, 30, 8, 5)) == "08:05"


# --- colour ------------------------------------------------------------------


def test_only_validated_hues_are_used_and_never_cycled():
    """A fourth flight gets neutral ink, not a re-used or invented hue."""
    hues = [data.series_colour(i) for i in range(5)]

    assert hues[:3] == data.SERIES_LIGHT
    assert hues[3] == hues[4] == data.NEUTRAL_LIGHT
    assert len(set(data.SERIES_LIGHT)) == 3


def test_the_dark_palette_is_a_selected_set_not_an_inversion():
    assert len(data.SERIES_DARK) == len(data.SERIES_LIGHT)
    assert data.SERIES_DARK != data.SERIES_LIGHT
    assert data.palette(dark=True)["series"] == data.SERIES_DARK
    assert data.palette(dark=False)["series"] == data.SERIES_LIGHT
