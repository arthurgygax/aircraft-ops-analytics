import math
from datetime import date

import pytest

from adsb.scope import (
    PREFILTER_RADIUS_KM,
    STUDY_AIRPORTS,
    STUDY_END,
    STUDY_START,
    Scope,
    bbox_predicate,
    boxes_for,
    default_scope,
    near_scope_predicate,
    parse_airports,
)

# Real coordinates: the box arithmetic is checked against known distances.
ZRH = ("LSZH", 47.458056, 8.548056)
DUS = ("EDDL", 51.289501, 6.76678)


# --- the period ------------------------------------------------------------


def test_the_default_scope_is_seven_consecutive_days_at_zrh_and_dus():
    scope = default_scope()
    assert scope.days[0] == STUDY_START
    assert scope.days[-1] == STUDY_END
    assert len(scope.days) == 7
    assert [(b - a).days for a, b in zip(scope.days, scope.days[1:])] == [1] * 6
    assert scope.airports == ("LSZH", "EDDL") == STUDY_AIRPORTS


def test_each_day_maps_to_the_release_that_covers_it():
    scope = Scope(start_date=date(2025, 12, 24), end_date=date(2025, 12, 26))
    assert scope.release_tags == (
        "v2025.12.24-planes-readsb-prod-0",
        "v2025.12.25-planes-readsb-prod-0",
        "v2025.12.26-planes-readsb-prod-0",
    )
    assert scope.releases[0] == ("v2025.12.24-planes-readsb-prod-0", "2025-12-24")


def test_a_single_day_scope_is_one_release():
    day = date(2025, 12, 30)
    assert len(Scope(start_date=day, end_date=day).releases) == 1


def test_the_environment_can_move_the_study_period(monkeypatch):
    monkeypatch.setenv("ADSB_SCOPE_START", "2025-12-01")
    monkeypatch.setenv("ADSB_SCOPE_END", "2025-12-02")
    monkeypatch.setenv("ADSB_SCOPE_AIRPORTS", "eggw, lfpg")
    scope = default_scope()
    assert scope.days == (date(2025, 12, 1), date(2025, 12, 2))
    assert scope.airports == ("EGGW", "LFPG")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"start_date": date(2025, 12, 30), "end_date": date(2025, 12, 24)},
        {"airports": ()},
        {"airports": ("LSZH", "Zurich Airport")},
        {"airports": ("LSZH", "'; DROP TABLE flights --")},
        {"prefilter_radius_km": 0},
    ],
)
def test_a_scope_that_cannot_mean_anything_is_refused(kwargs):
    with pytest.raises(ValueError):
        Scope(**kwargs)


def test_parse_airports_normalises_a_command_line_value():
    assert parse_airports(" lszh , eddl ,") == ("LSZH", "EDDL")


# --- the pre-filter box ----------------------------------------------------


def _km(lat1, lon1, lat2, lon2):
    """Haversine, so the box is checked against distance rather than degrees."""
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return 6371.0 * 2 * math.asin(math.sqrt(a))


def test_the_box_encloses_everything_within_the_radius():
    (box,) = boxes_for([ZRH], radius_km=25.0)
    ident, lat, lon = ZRH
    # the corners are the furthest points still in the box, so anything at or
    # inside the radius is inside the box in both directions
    assert _km(lat, lon, box.max_lat, lon) >= 25.0
    assert _km(lat, lon, lat, box.max_lon) >= 25.0


def test_the_box_is_wider_in_longitude_than_in_latitude():
    """A degree of longitude is shorter than a degree of latitude at 47N."""
    (box,) = boxes_for([ZRH], radius_km=25.0)
    assert (box.max_lon - box.min_lon) > (box.max_lat - box.min_lat)


def test_the_further_north_the_airport_the_wider_its_box_in_degrees():
    zurich, dusseldorf = boxes_for([ZRH, DUS], radius_km=25.0)
    assert (dusseldorf.max_lon - dusseldorf.min_lon) > (zurich.max_lon - zurich.min_lon)


def test_a_box_that_would_wrap_the_antimeridian_is_refused():
    with pytest.raises(ValueError, match="antimeridian"):
        boxes_for([("NZSP", -20.0, 179.99)], radius_km=25.0)


def test_the_predicate_covers_every_airport_in_the_scope():
    boxes = boxes_for([ZRH, DUS], radius_km=25.0)
    predicate = bbox_predicate(boxes, "lat", "lon")
    assert predicate.count(" OR ") == 1
    for box in boxes:
        assert str(box.min_lat) in predicate and str(box.max_lon) in predicate


def test_an_empty_scope_never_becomes_a_predicate_that_drops_everything():
    with pytest.raises(ValueError):
        bbox_predicate([], "lat", "lon")


def test_the_trace_predicate_reads_the_position_out_of_the_raw_array():
    predicate = near_scope_predicate(boxes_for([ZRH], radius_km=25.0))
    # positions 1 and 2 of a trace point are latitude and longitude
    assert predicate.startswith("exists(trace, p -> ")
    assert "CAST(p[1] AS DOUBLE)" in predicate
    assert "CAST(p[2] AS DOUBLE)" in predicate


def test_the_prefilter_radius_cannot_be_narrower_than_the_match_radius():
    """The pre-filter must not be able to drop a flight the matcher would keep."""
    pytest.importorskip("pyspark", reason="adsb.airports needs Spark to import")
    from adsb.airports import MATCH_RADIUS_KM

    assert PREFILTER_RADIUS_KM > MATCH_RADIUS_KM


# --- the two filters, on real DataFrames -----------------------------------

BOXES = boxes_for([ZRH, DUS], radius_km=PREFILTER_RADIUS_KM)
SCOPE = Scope()


def _aircraft(spark, rows):
    """Raw aircraft rows: ``trace`` is array<array<string>>, as the reader gives."""
    from pyspark.sql.types import (
        ArrayType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType([
        StructField("icao", StringType()),
        StructField("trace", ArrayType(ArrayType(StringType()))),
    ])
    return spark.createDataFrame(rows, schema)


def _point(latitude, longitude):
    return ["0.0", str(latitude), str(longitude)]


def test_near_scope_keeps_an_aircraft_that_only_touches_the_box_once(spark):
    """A Zurich departure is mostly nowhere near Zurich. It is still in scope."""
    from adsb.scope import near_scope

    aircraft = _aircraft(spark, [
        # departs Zurich, then crosses the Atlantic
        ("zrh1", [_point(47.46, 8.55), _point(48.9, 2.4), _point(40.6, -73.8)]),
        # never comes near either airport
        ("far1", [_point(-33.9, 151.2), _point(1.4, 103.9)]),
        # passes near Dusseldorf at the end
        ("dus1", [_point(41.8, 12.3), _point(51.29, 6.77)]),
    ])
    kept = {row["icao"] for row in near_scope(aircraft, BOXES).collect()}
    assert kept == {"zrh1", "dus1"}


def test_near_scope_drops_an_aircraft_that_passes_just_outside_the_box(spark):
    from adsb.scope import near_scope

    aircraft = _aircraft(spark, [
        ("near", [_point(47.60, 8.55)]),   # ~16 km north of Zurich: inside
        ("wide", [_point(48.10, 8.55)]),   # ~71 km north of Zurich: outside
    ])
    kept = {row["icao"] for row in near_scope(aircraft, BOXES).collect()}
    assert kept == {"near"}


def _movements(spark, rows):
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType([
        StructField("flight_id", StringType()),
        StructField("ident", StringType()),
        StructField("movement_type", StringType()),
    ])
    return spark.createDataFrame(rows, schema)


def test_a_flight_is_in_scope_at_either_end(spark):
    from adsb.scope import scope_flight_ids

    movements = _movements(spark, [
        ("out", "LSZH", "departure"),   # departs Zurich
        ("out", "KJFK", "arrival"),
        ("in", "EGLL", "departure"),
        ("in", "EDDL", "arrival"),      # arrives Dusseldorf
        ("other", "EGLL", "departure"),  # touches neither
        ("other", "LFPG", "arrival"),
    ])
    ids = {row["flight_id"] for row in scope_flight_ids(movements, SCOPE).collect()}
    assert ids == {"out", "in"}


def test_the_movements_table_keeps_only_the_study_airport_end(spark):
    from adsb.scope import at_study_airports

    movements = _movements(spark, [
        ("out", "LSZH", "departure"),
        ("out", "KJFK", "arrival"),
    ])
    kept = at_study_airports(movements, SCOPE).collect()
    assert [(r["ident"], r["movement_type"]) for r in kept] == [("LSZH", "departure")]


def test_restricting_keeps_the_whole_trajectory_of_an_in_scope_flight(spark):
    """The point of the scope: filter on the flight, never on the position."""
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    from adsb.scope import restrict_to_flights

    schema = StructType([
        StructField("flight_id", StringType()),
        StructField("latitude", DoubleType()),
    ])
    observations = spark.createDataFrame(
        [("out", 47.46), ("out", 48.9), ("out", 40.6),  # Zurich to New York
         ("other", 51.5), ("other", 48.9)],
        schema,
    )
    ids = spark.createDataFrame([("out",)], "flight_id string")

    kept = restrict_to_flights(observations, ids).collect()
    assert len(kept) == 3
    assert {row["flight_id"] for row in kept} == {"out"}
    # including the point 6,000 km from the airport that put it in scope
    assert min(row["latitude"] for row in kept) == 40.6
    # and it adds no columns
    assert restrict_to_flights(observations, ids).columns == observations.columns
