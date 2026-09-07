"""The single flight table: the aggregate, its airports, its hold rollups."""

from datetime import date, datetime, timedelta

import pytest

pyspark = pytest.importorskip("pyspark", reason="Spark tests run in the spark container")

from pyspark.sql.types import (  # noqa: E402
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from adsb.airports import airport_movements  # noqa: E402
from adsb.flights import (  # noqa: E402
    aggregate_flights,
    movement_pivot,
    read_flights,
    to_flights,
    write_flights,
)
from adsb.observations import GAP_SECONDS, assign_flights  # noqa: E402

START = datetime(2025, 12, 30, 8, 0, 0)
RELEASE_DATE = date(2025, 12, 30)

AIRPORT_COLUMNS = (
    "ident iata_code name type iso_country latitude_deg longitude_deg elevation_ft"
).split()
ZRH = ("LSZH", "ZRH", "Zurich Airport", "large_airport", "CH",
       47.458056, 8.548056, 1417.0)

HOLD_SCHEMA = StructType([
    StructField("flight_id", StringType()),
    StructField("duration_seconds", LongType()),
    StructField("arrival_airport_ident", StringType()),
    StructField("release_date", DateType()),
])


def obs(icao="a1b2c3", offset_s=0, lat=47.458, lon=8.548, callsign="SWR123",
        on_ground=False, alt=10000.0, gs=300.0):
    return (icao, True, "HB-ABC", "A320", "SWISS AIR",
            START + timedelta(seconds=offset_s), lat, lon, on_ground, alt, gs,
            90.0, 0.0, callsign, "v2025.12.30", RELEASE_DATE)


@pytest.fixture
def base(cleaned):
    """Rows -> the flight aggregate everything downstream is built from."""
    return lambda rows: aggregate_flights(assign_flights(cleaned(rows)))


@pytest.fixture
def airports(spark):
    return spark.createDataFrame([ZRH], AIRPORT_COLUMNS)


@pytest.fixture
def no_holds(spark):
    return spark.createDataFrame([], HOLD_SCHEMA)


@pytest.fixture
def flights(spark, base, airports, no_holds):
    """The finished flight table for a set of observations."""
    def _make(rows, holds=None):
        aggregate = base(rows)
        movements = airport_movements(aggregate, airports)
        return to_flights(
            aggregate, movement_pivot(movements), holds if holds is not None else no_holds
        )

    return _make


# --- the aggregate -----------------------------------------------------------


def test_observations_within_the_gap_become_one_flight(base):
    rows = base([obs(offset_s=0), obs(offset_s=60), obs(offset_s=120)]).collect()

    assert len(rows) == 1
    assert rows[0].n_observations == 3
    assert rows[0].duration_seconds == 120


def test_a_gap_longer_than_the_threshold_makes_two_flights(base):
    assert base([obs(offset_s=0), obs(offset_s=GAP_SECONDS + 1)]).count() == 2


def test_aircraft_are_aggregated_independently(base):
    """One aircraft's gap must not break another's flight."""
    rows = base([
        obs(icao="aaa111", offset_s=0),
        obs(icao="bbb222", offset_s=10),
        obs(icao="aaa111", offset_s=GAP_SECONDS + 30),
        obs(icao="bbb222", offset_s=60),
    ]).collect()

    counts = {r.icao: r.n_observations for r in rows if r.icao == "bbb222"}
    assert len(rows) == 3
    assert counts["bbb222"] == 2


def test_every_observation_lands_in_exactly_one_flight(base, cleaned):
    rows = [obs(offset_s=o) for o in (0, 30, GAP_SECONDS + 60, GAP_SECONDS + 90)]

    total = sum(r.n_observations for r in base(rows).collect())

    assert total == len(rows)


def test_endpoints_come_from_the_first_and_last_observation(base):
    flight = base([
        obs(offset_s=0, lat=47.0, lon=8.0, on_ground=True),
        obs(offset_s=60, lat=48.0, lon=9.0),
        obs(offset_s=120, lat=49.0, lon=10.0, on_ground=False),
    ]).first()

    assert (flight.first_latitude, flight.first_longitude) == (47.0, 8.0)
    assert (flight.last_latitude, flight.last_longitude) == (49.0, 10.0)
    assert flight.started_on_ground is True
    assert flight.ended_on_ground is False
    assert flight.saw_ground is True
    # endpoint altitudes, which airport attribution needs
    assert flight.first_altitude_ft == 10000.0
    assert flight.last_altitude_ft == 10000.0


def test_callsign_is_the_first_reported_one_and_changes_are_counted(base):
    """A flight spanning a turnaround keeps the first callsign but flags itself."""
    flight = base([
        obs(offset_s=0, callsign=None),
        obs(offset_s=60, callsign="SWR100"),
        obs(offset_s=120, callsign="SWR200"),
    ]).first()

    assert flight.callsign == "SWR100", "nulls skipped, earliest real callsign wins"
    assert flight.n_callsigns == 2


def test_a_lone_observation_is_still_a_flight(base):
    flight = base([obs()]).first()

    assert flight.n_observations == 1
    assert flight.duration_seconds == 0


def test_flight_id_is_derived_from_aircraft_and_first_instant(base):
    assert base([obs(offset_s=0), obs(offset_s=60)]).first().flight_id == (
        "a1b2c3_20251230080000"
    )


# --- the published table -----------------------------------------------------


def test_airline_comes_from_an_airline_style_callsign(flights):
    assert flights([obs(callsign="SWR123")]).first().airline_icao == "SWR"


@pytest.mark.parametrize("callsign", ["N884GA", "D-EABC", None, "SW1"])
def test_a_registration_style_callsign_is_not_an_airline(flights, callsign):
    assert flights([obs(callsign=callsign)]).first().airline_icao is None


def test_airports_are_matched_onto_the_flight(flights):
    flight = flights([
        obs(offset_s=0, lat=47.458, lon=8.548, on_ground=True, alt=None),
        obs(offset_s=600, lat=46.0, lon=7.0, alt=30000.0),
    ]).first()

    assert flight.departure_airport_ident == "LSZH"
    assert flight.departure_airport_iata == "ZRH"
    assert flight.departure_time is not None
    assert flight.arrival_airport_ident is None, "nullable by design"


def test_flight_date_is_the_day_of_the_first_observation(flights):
    assert flights([obs()]).first().flight_date == RELEASE_DATE


def test_registered_owner_is_the_registry_entry_not_the_airline(flights):
    assert flights([obs()]).first().registered_owner == "SWISS AIR"


def test_hold_rollups_default_to_zero_not_null(flights):
    flight = flights([obs()]).first()

    assert flight.n_detected_holds == 0
    assert flight.has_detected_hold is False
    assert flight.total_hold_seconds == 0


def test_hold_rollups_are_joined_on(flights, spark):
    holds = spark.createDataFrame(
        [("a1b2c3_20251230080000", 300, "LSZH", RELEASE_DATE),
         ("a1b2c3_20251230080000", 420, "LSZH", RELEASE_DATE)],
        HOLD_SCHEMA,
    )

    flight = flights([obs(offset_s=0), obs(offset_s=60)], holds=holds).first()

    assert flight.n_detected_holds == 2
    assert flight.has_detected_hold is True
    assert flight.total_hold_seconds == 720


def test_flights_round_trip_through_delta(flights, spark, tmp_path):
    path = str(tmp_path / "flights")
    table = flights([obs(offset_s=0), obs(offset_s=GAP_SECONDS + 1)])

    write_flights(table, path, release_date="2025-12-30")

    assert read_flights(spark, path).count() == 2
