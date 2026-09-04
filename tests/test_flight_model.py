from datetime import date, datetime, timedelta

import pytest

pyspark = pytest.importorskip("pyspark", reason="Spark tests run in the spark container")

from pyspark.sql.types import (  # noqa: E402
    BooleanType,
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from adsb.flights import GAP_SECONDS, to_flight_segments  # noqa: E402
from adsb.flight_model import to_flight_observations, to_flights  # noqa: E402

# explicit: a fixture where every callsign or altitude is None defeats
# Spark's type inference
SILVER_SCHEMA = StructType([
    StructField("icao", StringType()),
    StructField("is_icao_address", BooleanType()),
    StructField("registration", StringType()),
    StructField("aircraft_type", StringType()),
    StructField("operator", StringType()),
    StructField("event_time", TimestampType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("on_ground", BooleanType()),
    StructField("altitude_ft", DoubleType()),
    StructField("ground_speed_kt", DoubleType()),
    StructField("track_deg", DoubleType()),
    StructField("vertical_rate_fpm", DoubleType()),
    StructField("callsign", StringType()),
    StructField("release_tag", StringType()),
    StructField("release_date", DateType()),
    StructField("ingested_at", TimestampType()),
])

AIRPORT_COLUMNS = (
    "ident iata_code name type iso_country latitude_deg longitude_deg elevation_ft"
).split()

START = datetime(2025, 12, 30, 8, 0, 0)
RELEASE_DATE = date(2025, 12, 30)
INGESTED = datetime(2026, 1, 1)

ZRH = ("LSZH", "ZRH", "Zurich Airport", "large_airport", "CH",
       47.458056, 8.548056, 1417.0)


def obs(icao="a1b2c3", offset_s=0, lat=47.458, lon=8.548, callsign="SWR123",
        on_ground=False, alt=10000.0, gs=300.0, track=90.0, vrate=0.0):
    return (icao, True, "HB-ABC", "A320", "SWISS AIR", START + timedelta(seconds=offset_s),
            lat, lon, on_ground, alt, gs, track, vrate, callsign, "v2025.12.30",
            RELEASE_DATE, INGESTED)


@pytest.fixture
def silver(spark):
    return lambda rows: spark.createDataFrame(rows, SILVER_SCHEMA)


@pytest.fixture
def airports(spark):
    return spark.createDataFrame([ZRH], AIRPORT_COLUMNS)


# --- segmentation and flight_id ---------------------------------------------


def test_observations_within_the_gap_share_one_flight_id(silver):
    points = to_flight_observations(
        silver([obs(offset_s=0), obs(offset_s=60), obs(offset_s=120)])
    ).collect()

    assert len({p.flight_id for p in points}) == 1


def test_a_gap_starts_a_new_flight_id(silver):
    points = to_flight_observations(
        silver([obs(offset_s=0), obs(offset_s=GAP_SECONDS + 1)])
    ).collect()

    assert len({p.flight_id for p in points}) == 2


def test_flight_id_is_the_aircraft_address_and_first_seen_instant(silver):
    """Deterministic and explainable, not a random surrogate key."""
    points = to_flight_observations(silver([obs(offset_s=0), obs(offset_s=60)]))

    assert points.first().flight_id == "a1b2c3_20251230080000"


def test_flight_id_is_stable_across_recomputation(silver):
    rows = [obs(offset_s=0), obs(offset_s=60), obs(offset_s=GAP_SECONDS + 5)]

    first = {p.flight_id for p in to_flight_observations(silver(rows)).collect()}
    second = {p.flight_id for p in to_flight_observations(silver(rows)).collect()}

    assert first == second


def test_flight_ids_agree_between_the_two_tables(silver, airports):
    """The trajectory and the summary must never disagree about the flight."""
    rows = [obs(offset_s=0), obs(offset_s=60), obs(offset_s=GAP_SECONDS + 5)]
    df = silver(rows)

    point_ids = {p.flight_id for p in to_flight_observations(df).collect()}
    flight_ids = {
        f.flight_id for f in to_flights(to_flight_segments(df), airports).collect()
    }

    assert point_ids == flight_ids


# --- track preservation ------------------------------------------------------


def test_every_observation_is_preserved_as_its_own_row(silver):
    """The map needs points; reducing to one row per flight would break it."""
    rows = [obs(offset_s=s) for s in (0, 30, 60, 90, GAP_SECONDS + 30)]

    points = to_flight_observations(silver(rows))

    assert points.count() == len(rows)


def test_trajectory_columns_survive_to_the_point_table(silver):
    points = to_flight_observations(
        silver([obs(lat=47.1, lon=8.1, alt=12000.0, gs=310.0, track=275.0, vrate=-640.0)])
    ).first()

    assert (points.latitude, points.longitude) == (47.1, 8.1)
    assert points.altitude_ft == 12000.0
    assert points.ground_speed_kt == 310.0
    assert points.track_deg == 275.0
    assert points.vertical_rate_fpm == -640.0


def test_observations_are_numbered_in_time_order_within_a_flight(silver):
    rows = [obs(offset_s=120), obs(offset_s=0), obs(offset_s=60)]

    points = to_flight_observations(silver(rows)).collect()
    ordered = sorted(points, key=lambda p: p.observation_seq)

    assert [p.observation_seq for p in ordered] == [1, 2, 3]
    assert [p.event_time for p in ordered] == sorted(p.event_time for p in points)


# --- flight-level aggregation ------------------------------------------------


def test_flight_row_summarizes_its_observations(silver, airports):
    rows = [obs(offset_s=0, alt=1000.0, gs=200.0), obs(offset_s=600, alt=30000.0, gs=450.0)]

    flight = to_flights(to_flight_segments(silver(rows)), airports).first()

    assert flight.n_observations == 2
    assert flight.duration_seconds == 600
    assert flight.max_altitude_ft == 30000.0
    assert flight.max_ground_speed_kt == 450.0
    assert flight.first_seen_time == START
    assert flight.last_seen_time == START + timedelta(seconds=600)


def test_airline_code_is_taken_from_an_airline_style_callsign(silver, airports):
    flight = to_flights(
        to_flight_segments(silver([obs(callsign="SWR123"), obs(offset_s=60)])), airports
    ).first()

    assert flight.airline_icao == "SWR"


@pytest.mark.parametrize("callsign", ["N884GA", "D-EABC", "12345"])
def test_a_registration_style_callsign_yields_no_airline(silver, airports, callsign):
    """A private registration must not be mistaken for an airline code."""
    flight = to_flights(
        to_flight_segments(
            silver([obs(callsign=callsign), obs(offset_s=60, callsign=callsign)])
        ),
        airports,
    ).first()

    assert flight.airline_icao is None


def test_the_registry_owner_is_kept_but_not_called_the_airline(silver, airports):
    """It is frequently a leasing trust, so the name must not imply operator."""
    flight = to_flights(to_flight_segments(silver([obs()])), airports).first()

    assert flight.registered_owner == "SWISS AIR"
    assert "operator" not in flight.asDict()


# --- airports ----------------------------------------------------------------


def test_a_flight_leaving_an_airport_gets_a_departure(silver, airports):
    rows = [
        obs(offset_s=0, lat=47.458, lon=8.548, alt=None, on_ground=True),
        obs(offset_s=600, lat=46.0, lon=7.0, alt=30000.0),
    ]

    flight = to_flights(to_flight_segments(silver(rows)), airports).first()

    assert flight.departure_airport_ident == "LSZH"
    assert flight.departure_airport_iata == "ZRH"
    assert flight.departure_time == START
    assert flight.arrival_airport_ident is None, "nowhere near an airport at the end"


def test_a_flight_matching_no_airport_keeps_null_airport_columns(silver, airports):
    """60% of real flights match at most one end; nulls are the normal case."""
    rows = [obs(offset_s=0, lat=10.0, lon=100.0, alt=35000.0),
            obs(offset_s=600, lat=11.0, lon=101.0, alt=35000.0)]

    flight = to_flights(to_flight_segments(silver(rows)), airports).first()

    assert flight.departure_airport_ident is None
    assert flight.arrival_airport_ident is None
    assert flight.flight_id is not None, "the flight itself must survive"


# --- sparse and missing values ----------------------------------------------


def test_a_flight_with_no_callsign_at_all_still_produces_rows(silver, airports):
    rows = [obs(callsign=None), obs(offset_s=60, callsign=None)]

    flight = to_flights(to_flight_segments(silver(rows)), airports).first()
    points = to_flight_observations(silver(rows))

    assert flight.callsign is None
    assert flight.airline_icao is None
    assert points.count() == 2


def test_a_sparse_callsign_is_filled_at_flight_level_but_not_at_point_level(
    silver, airports
):
    """Only 24% of observations carry a callsign; the flight still gets one."""
    rows = [obs(offset_s=0, callsign=None), obs(offset_s=60, callsign="SWR123")]

    flight = to_flights(to_flight_segments(silver(rows)), airports).first()
    points = {p.observation_seq: p.callsign
              for p in to_flight_observations(silver(rows)).collect()}

    assert flight.callsign == "SWR123"
    assert points == {1: None, 2: "SWR123"}, "point-level sparsity is preserved"


def test_missing_measurements_stay_null_rather_than_becoming_zero(silver):
    point = to_flight_observations(
        silver([obs(alt=None, gs=None, track=None, vrate=None)])
    ).first()

    assert point.altitude_ft is None
    assert point.ground_speed_kt is None
    assert point.track_deg is None
    assert point.vertical_rate_fpm is None


def test_a_single_observation_flight_is_still_a_flight(silver, airports):
    flight = to_flights(to_flight_segments(silver([obs()])), airports).first()

    assert flight.n_observations == 1
    assert flight.duration_seconds == 0
    assert flight.first_seen_time == flight.last_seen_time


# --- trajectory readiness (Phase 11) -----------------------------------------


def test_observation_order_is_deterministic_when_timestamps_tie(silver):
    """Silver makes ties impossible, but the order must not depend on read order."""
    tied = [
        obs(offset_s=0, lat=47.500, lon=8.500),
        obs(offset_s=0, lat=47.400, lon=8.400),  # same instant
        obs(offset_s=60, lat=47.600, lon=8.600),
    ]

    forward = to_flight_observations(silver(tied)).collect()
    reversed_ = to_flight_observations(silver(list(reversed(tied)))).collect()

    def order(rows):
        return [(r.observation_seq, r.latitude) for r in sorted(rows, key=lambda x: x.observation_seq)]

    assert order(forward) == order(reversed_)
    assert order(forward)[0][1] == 47.400, "tie broken by position, not by read order"


def test_observation_seq_is_a_total_order_within_each_flight(silver):
    rows = [obs(offset_s=s) for s in (0, 30, 60, 90)] + [obs(offset_s=GAP_SECONDS + 10)]

    points = to_flight_observations(silver(rows)).collect()

    by_flight = {}
    for p in points:
        by_flight.setdefault(p.flight_id, []).append(p.observation_seq)
    for seqs in by_flight.values():
        assert sorted(seqs) == list(range(1, len(seqs) + 1))


def test_the_track_carries_every_field_a_trajectory_needs(silver):
    """flight_id, time, position, altitude, speed, track -- and nothing bulky."""
    point = to_flight_observations(silver([obs()])).first()

    assert set(point.asDict()) == {
        "flight_id", "icao", "event_time", "observation_seq",
        "latitude", "longitude", "altitude_ft", "on_ground",
        "ground_speed_kt", "track_deg", "vertical_rate_fpm",
        "callsign", "release_date",
    }


def test_a_position_without_altitude_or_speed_is_still_a_track_point(silver):
    """0.7% of real points carry position only; they remain plottable."""
    points = to_flight_observations(
        silver([obs(alt=None, gs=None, track=None, vrate=None), obs(offset_s=60)])
    )

    assert points.count() == 2, "position-only points must not be dropped"


def test_recomputation_produces_identical_track_rows(silver):
    rows = [obs(offset_s=s) for s in (0, 30, 60)]

    def snapshot():
        return sorted(
            (r.flight_id, r.observation_seq, r.latitude, r.longitude)
            for r in to_flight_observations(silver(rows)).collect()
        )

    assert snapshot() == snapshot()
