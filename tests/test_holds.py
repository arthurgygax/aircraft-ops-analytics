import math
from datetime import date, datetime, timedelta

import pytest

pyspark = pytest.importorskip("pyspark", reason="Spark tests run in the spark container")

from pyspark.sql.types import (  # noqa: E402
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from adsb.holds import detect_hold_candidates, to_flight_holds  # noqa: E402

POINT_SCHEMA = StructType([
    StructField("flight_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("observation_seq", IntegerType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("altitude_ft", DoubleType()),
    StructField("on_ground", BooleanType()),
    StructField("track_deg", DoubleType()),
    StructField("release_date", DateType()),
])

FLIGHT_SCHEMA = StructType([
    StructField("flight_id", StringType()),
    StructField("arrival_airport_ident", StringType()),
    StructField("arrival_airport_iata", StringType()),
    StructField("release_date", DateType()),
])

AIRPORT_SCHEMA = StructType([
    StructField("ident", StringType()),
    StructField("latitude_deg", DoubleType()),
    StructField("longitude_deg", DoubleType()),
])

START = datetime(2025, 12, 30, 8, 0, 0)
RELEASE_DATE = date(2025, 12, 30)
FLIGHT = "a1b2c3_20251230080000"

CENTRE_LAT, CENTRE_LON = 47.4, 8.5
KM_PER_DEG = 111.32


def _rows(samples, flight_id=FLIGHT, step_s=10, altitude=12000.0):
    """samples: (lat, lon, track_deg) triples -> flight_observations rows."""
    return [
        (flight_id, START + timedelta(seconds=i * step_s), i + 1,
         lat, lon, altitude, False, track, RELEASE_DATE)
        for i, (lat, lon, track) in enumerate(samples)
    ]


def circling(turn_total_deg, radius_km=6.0, step_s=10, turn_rate=3.0):
    """A geometrically consistent circle: position and heading agree.

    ``turn_rate`` degrees per second is the standard rate turn, so a full
    circle takes two minutes.
    """
    samples = []
    steps = int(abs(turn_total_deg) / (turn_rate * step_s))
    for i in range(steps + 1):
        bearing = math.radians(i * turn_rate * step_s)
        lat = CENTRE_LAT + (radius_km / KM_PER_DEG) * math.cos(bearing)
        lon = CENTRE_LON + (radius_km / (KM_PER_DEG * math.cos(math.radians(CENTRE_LAT)))) * math.sin(bearing)
        # heading is tangent to the circle
        samples.append((lat, lon, (math.degrees(bearing) + 90.0) % 360.0))
    return samples


def straight(n, step_s=10, ground_speed_kt=250.0, track=90.0):
    """Level flight due east at constant heading."""
    km_per_step = ground_speed_kt * 1.852 * step_s / 3600.0
    return [
        (CENTRE_LAT,
         CENTRE_LON + i * km_per_step / (KM_PER_DEG * math.cos(math.radians(CENTRE_LAT))),
         track)
        for i in range(n)
    ]


@pytest.fixture
def points(spark):
    return lambda samples: spark.createDataFrame(samples, POINT_SCHEMA)


@pytest.fixture
def context(spark):
    flights = spark.createDataFrame(
        [(FLIGHT, "LSZH", "ZRH", RELEASE_DATE)], FLIGHT_SCHEMA
    )
    airports = spark.createDataFrame([("LSZH", 47.458056, 8.548056)], AIRPORT_SCHEMA)
    return flights, airports


# 1. a holding-like racetrack -------------------------------------------------


def test_sustained_circling_is_detected_as_a_hold(points, context):
    """Two full circles in a small area over eight minutes."""
    holds = to_flight_holds(points(_rows(circling(720))), *context).collect()

    assert len(holds) == 1
    hold = holds[0]
    assert hold.circuits >= 1.9
    assert hold.duration_seconds >= 240
    assert hold.span_km <= 25


def test_a_detected_hold_reports_where_and_how_high(points, context):
    hold = to_flight_holds(points(_rows(circling(720))), *context).first()

    assert abs(hold.centroid_latitude - CENTRE_LAT) < 0.05
    assert abs(hold.centroid_longitude - CENTRE_LON) < 0.05
    assert hold.mean_altitude_ft == 12000.0
    assert hold.min_altitude_ft == hold.max_altitude_ft == 12000.0


def test_a_hold_is_associated_with_the_flights_own_arrival_airport(points, context):
    hold = to_flight_holds(points(_rows(circling(720))), *context).first()

    assert hold.arrival_airport_ident == "LSZH"
    assert hold.arrival_airport_iata == "ZRH"
    # LSZH is ~7 km from the circle centre used in these fixtures
    assert 0 < hold.distance_to_arrival_airport_km < 20


def test_a_hold_without_an_inferred_arrival_airport_still_stands(points, spark):
    """Airport inference fails for most flights; the hold is still real."""
    flights = spark.createDataFrame([(FLIGHT, None, None, RELEASE_DATE)], FLIGHT_SCHEMA)
    airports = spark.createDataFrame([("LSZH", 47.458056, 8.548056)], AIRPORT_SCHEMA)

    hold = to_flight_holds(points(_rows(circling(720))), flights, airports).first()

    assert hold.arrival_airport_ident is None
    assert hold.distance_to_arrival_airport_km is None
    assert hold.circuits >= 1.9


# 2. a normal turn ------------------------------------------------------------


def test_a_base_to_final_turn_is_not_a_hold(points, context):
    """Ninety degrees of turn then straight ahead: ordinary approach."""
    samples = circling(90) + straight(30)

    assert to_flight_holds(points(_rows(samples)), *context).count() == 0


def test_a_procedure_turn_of_270_degrees_is_not_a_hold(points, context):
    """Below a full circle, so it never reaches the threshold."""
    assert to_flight_holds(points(_rows(circling(270))), *context).count() == 0


def test_an_s_bend_does_not_accumulate_into_a_circle(points, context):
    """Left then right cancels: the sum is signed, not absolute."""
    left = circling(180)
    right = [(lat, lon, (360.0 - track) % 360.0) for lat, lon, track in circling(180)]

    assert to_flight_holds(points(_rows(left + right)), *context).count() == 0


# 3. straight flight ----------------------------------------------------------


def test_straight_and_level_flight_is_not_a_hold(points, context):
    assert to_flight_holds(points(_rows(straight(60))), *context).count() == 0


# 4. sparse and noisy data ----------------------------------------------------


def test_a_heading_change_across_a_coverage_gap_is_not_counted(points, context):
    """Two observations ten minutes apart say nothing about the path between."""
    samples = [(CENTRE_LAT, CENTRE_LON, float(h)) for h in (0, 90, 180, 270, 0, 90)]
    rows = _rows(samples, step_s=600)  # far beyond MAX_STEP_GAP_SECONDS

    assert to_flight_holds(points(rows), *context).count() == 0


def test_sparse_sampling_is_reported_so_a_row_can_be_judged(points, context):
    dense = _rows(circling(720))
    hold = to_flight_holds(points(dense), *context).first()

    assert hold.max_sample_gap_seconds <= 10
    assert hold.n_observations > 20


# conservative filters --------------------------------------------------------


def test_a_wide_sweeping_circle_is_rejected_as_too_large(points, context):
    """A 360 degree turn spread over 80 km is not a holding pattern."""
    candidates = detect_hold_candidates(points(_rows(circling(720, radius_km=40.0))))

    assert candidates.count() >= 1, "the turn itself is still circling"
    assert candidates.first().span_km > 25
    assert to_flight_holds(points(_rows(circling(720, radius_km=40.0))), *context).count() == 0


def test_a_spiral_descent_is_rejected_as_not_level(points, spark, context):
    samples = circling(720)
    rows = [
        (FLIGHT, START + timedelta(seconds=i * 10), i + 1, lat, lon,
         20000.0 - i * 200.0, False, track, RELEASE_DATE)
        for i, (lat, lon, track) in enumerate(samples)
    ]

    assert to_flight_holds(points(rows), *context).count() == 0


def test_flights_are_detected_independently(points, spark):
    holding = _rows(circling(720), flight_id="aaa111_20251230080000")
    cruising = _rows(straight(60), flight_id="bbb222_20251230080000")
    flights = spark.createDataFrame(
        [("aaa111_20251230080000", None, None, RELEASE_DATE),
         ("bbb222_20251230080000", None, None, RELEASE_DATE)], FLIGHT_SCHEMA)
    airports = spark.createDataFrame([("LSZH", 47.458056, 8.548056)], AIRPORT_SCHEMA)

    holds = to_flight_holds(points(holding + cruising), flights, airports).collect()

    assert [h.flight_id for h in holds] == ["aaa111_20251230080000"]


def test_detection_is_deterministic(points, context):
    rows = _rows(circling(720))

    def snapshot():
        return [(h.hold_seq, h.duration_seconds, h.circuits, h.span_km)
                for h in to_flight_holds(points(rows), *context).collect()]

    assert snapshot() == snapshot()
