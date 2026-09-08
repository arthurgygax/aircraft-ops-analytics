"""Every quality rule is deliberately made to fire.

A check that never fires is worse than no check at all, so each case here
corrupts exactly one field and names the rule that must catch it.
"""

from datetime import date, datetime

import pytest

pyspark = pytest.importorskip("pyspark", reason="Spark tests run in the spark container")

from pyspark.sql.types import (  # noqa: E402
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from adsb.quality import (  # noqa: E402
    DataQualityError,
    assert_valid,
    check_not_empty,
    check_observations_conserved,
    check_references,
    check_rows,
    check_unique,
    collisions_resolved,
    validate_airport_operations,
    validate_flight_holds,
    validate_flights,
    validate_movements,
    validate_observations,
)

T0 = datetime(2025, 12, 30, 8, 0, 0)
T1 = datetime(2025, 12, 30, 9, 0, 0)
DAY = date(2025, 12, 30)

# One valid row per table; tests corrupt a single field to prove each rule bites.
TABLES = {
    "observations": (
        validate_observations,
        StructType([
            StructField("flight_id", StringType()),
            StructField("icao", StringType()),
            StructField("is_icao_address", BooleanType()),
            StructField("event_time", TimestampType()),
            StructField("observation_seq", IntegerType()),
            StructField("latitude", DoubleType()),
            StructField("longitude", DoubleType()),
            StructField("ground_speed_kt", DoubleType()),
            StructField("vertical_rate_fpm", DoubleType()),
            StructField("callsign", StringType()),
            StructField("release_tag", StringType()),
            StructField("release_date", DateType()),
        ]),
        {"flight_id": "a1b2c3_20251230080000", "icao": "a1b2c3",
         "is_icao_address": True, "event_time": T0, "observation_seq": 1,
         "latitude": 47.4, "longitude": 8.5, "ground_speed_kt": 300.0,
         "vertical_rate_fpm": 500.0, "callsign": "SWR1",
         "release_tag": "v2025.12.30", "release_date": DAY},
    ),
    "movements": (
        validate_movements,
        StructType([
            StructField("flight_id", StringType()),
            StructField("movement_type", StringType()),
            StructField("event_time", TimestampType()),
            StructField("ident", StringType()),
            StructField("latitude", DoubleType()),
            StructField("longitude", DoubleType()),
            StructField("distance_km", DoubleType()),
        ]),
        {"flight_id": "a1b2c3_20251230080000", "movement_type": "departure",
         "event_time": T0, "ident": "LSZH", "latitude": 47.4, "longitude": 8.5,
         "distance_km": 1.2},
    ),
    "flights": (
        validate_flights,
        StructType([
            StructField("flight_id", StringType()),
            StructField("flight_date", DateType()),
            StructField("icao", StringType()),
            StructField("first_seen_time", TimestampType()),
            StructField("last_seen_time", TimestampType()),
            StructField("duration_seconds", LongType()),
            StructField("n_observations", LongType()),
            StructField("first_latitude", DoubleType()),
            StructField("first_longitude", DoubleType()),
            StructField("last_latitude", DoubleType()),
            StructField("last_longitude", DoubleType()),
            StructField("airline_icao", StringType()),
            StructField("departure_airport_ident", StringType()),
            StructField("departure_time", TimestampType()),
            StructField("departure_distance_km", DoubleType()),
            StructField("arrival_airport_ident", StringType()),
            StructField("arrival_time", TimestampType()),
            StructField("arrival_distance_km", DoubleType()),
            StructField("n_detected_holds", LongType()),
            StructField("has_detected_hold", BooleanType()),
            StructField("total_hold_seconds", LongType()),
        ]),
        {"flight_id": "a1b2c3_20251230080000", "flight_date": DAY,
         "icao": "a1b2c3", "first_seen_time": T0, "last_seen_time": T1,
         "duration_seconds": 3600, "n_observations": 10,
         "first_latitude": 47.4, "first_longitude": 8.5,
         "last_latitude": 46.0, "last_longitude": 7.0, "airline_icao": "SWR",
         "departure_airport_ident": "LSZH", "departure_time": T0,
         "departure_distance_km": 1.2, "arrival_airport_ident": "EGLL",
         "arrival_time": T1, "arrival_distance_km": 2.5,
         "n_detected_holds": 0, "has_detected_hold": False,
         "total_hold_seconds": 0},
    ),
    "airport_daily_operations": (
        validate_airport_operations,
        StructType([
            StructField("operations_date", DateType()),
            StructField("airport_ident", StringType()),
            StructField("arrivals", LongType()),
            StructField("departures", LongType()),
            StructField("total_operations", LongType()),
            StructField("unique_aircraft", LongType()),
            StructField("airport_latitude", DoubleType()),
            StructField("airport_longitude", DoubleType()),
            StructField("first_operation_time", TimestampType()),
            StructField("last_operation_time", TimestampType()),
            StructField("metric_source", StringType()),
            StructField("flights_with_detected_holds", LongType()),
            StructField("hold_rate", DoubleType()),
        ]),
        {"operations_date": DAY, "airport_ident": "LSZH",
         "arrivals": 3, "departures": 2, "total_operations": 5,
         "unique_aircraft": 4, "airport_latitude": 47.458,
         "airport_longitude": 8.548, "first_operation_time": T0,
         "last_operation_time": T1, "metric_source": "adsb_inferred",
         "flights_with_detected_holds": 1, "hold_rate": 0.3333},
    ),
}


@pytest.fixture
def table(spark):
    def _make(name, **overrides):
        _, schema, valid = TABLES[name]
        row = {**valid, **overrides}
        return spark.createDataFrame([tuple(row[f.name] for f in schema)], schema)

    return _make


@pytest.mark.parametrize("name", list(TABLES))
def test_a_valid_row_passes_every_check(table, name):
    validator = TABLES[name][0]

    failures = [r for r in validator(table(name)) if not r.passed]

    assert failures == []


# Each case corrupts one field and names the check that must catch it.
CORRUPTIONS = [
    ("observations", {"icao": None}, "icao is present"),
    ("observations", {"latitude": 91.0}, "latitude within [-90, 90]"),
    ("observations", {"longitude": -181.0}, "longitude within [-180, 180]"),
    ("observations", {"event_time": None}, "event_time is present"),
    ("observations", {"latitude": 0.0, "longitude": 0.0},
     "position is not null island"),
    ("observations", {"latitude": None}, "position is present"),
    ("observations", {"observation_seq": 0}, "observation_seq starts at one"),
    ("observations", {"flight_id": "zzz_1"},
     "flight_id starts with the aircraft address"),
    ("observations", {"ground_speed_kt": 1800.0},
     "implausible ground speed removed"),
    ("observations", {"vertical_rate_fpm": -64000.0},
     "implausible vertical rate removed"),
    ("observations", {"callsign": ""}, "blank callsign normalized to NULL"),
    ("observations", {"is_icao_address": None}, "is_icao_address is set"),
    # the regex regression that actually happened
    ("observations", {"release_tag": ""}, "release_tag is recorded"),
    # a point filed under the wrong day is invisible to a date-filtered read,
    # which is how the app reaches a trajectory
    ("observations", {"release_date": date(2025, 12, 29)},
     "observation falls in its partition day"),
    ("movements", {"movement_type": "diversion"},
     "movement_type is arrival or departure"),
    ("movements", {"ident": None}, "an airport is matched"),
    ("movements", {"distance_km": 120.0}, "the match is within the search radius"),
    ("flights", {"last_seen_time": datetime(2025, 12, 30, 7, 0, 0)},
     "flight does not end before it starts"),
    ("flights", {"duration_seconds": -1}, "duration is not negative"),
    ("flights", {"n_observations": 0}, "flight has at least one observation"),
    ("flights", {"duration_seconds": 12345}, "duration agrees with the timestamps"),
    ("flights", {"flight_date": date(2025, 12, 31)},
     "flight_date matches the first observation"),
    ("flights", {"airline_icao": "N884GA"}, "airline_icao is a three letter code"),
    ("flights", {"arrival_distance_km": 120.0},
     "matched airports are within the search radius"),
    ("flights", {"departure_time": None}, "an airport match carries a time"),
    ("flights", {"has_detected_hold": True}, "hold rollups agree with each other"),
    ("flights", {"total_hold_seconds": 300},
     "a flight with no holds has no hold time"),
    ("airport_daily_operations", {"total_operations": 99},
     "arrivals and departures sum to total_operations"),
    ("airport_daily_operations", {"unique_aircraft": 99},
     "unique_aircraft does not exceed operations"),
    ("airport_daily_operations", {"operations_date": None},
     "operations_date is present"),
    ("airport_daily_operations", {"airport_ident": None}, "airport_ident is present"),
    ("airport_daily_operations", {"metric_source": "official"},
     "every row is labelled as inferred"),
    ("airport_daily_operations", {"flights_with_detected_holds": 99},
     "flights with holds do not exceed arrivals"),
    ("airport_daily_operations", {"hold_rate": 1.5}, "hold_rate is a proportion"),
    ("airport_daily_operations", {"last_operation_time": datetime(2025, 12, 30, 7, 0)},
     "last operation is not before the first"),
    ("airport_daily_operations", {"first_operation_time": datetime(2025, 12, 31, 8, 0)},
     "operations fall on the reported date"),
]


@pytest.mark.parametrize("name,corruption,expected_check", CORRUPTIONS)
def test_a_corrupt_row_is_caught_by_the_right_check(
    table, name, corruption, expected_check
):
    validator = TABLES[name][0]

    failed = [r.check for r in validator(table(name, **corruption)) if not r.passed]

    assert expected_check in failed


def test_range_checks_ignore_missing_values(table):
    """A NULL coordinate is missing, not out of range."""
    failed = [
        r.check for r in validate_observations(table("observations", latitude=None))
        if not r.passed
    ]

    assert "latitude within [-90, 90]" not in failed
    assert "position is present" in failed, "the IS NULL rule is what polices it"


def test_an_empty_table_is_a_failure(table):
    """The silent-empty failure mode: everything downstream builds, emptily."""
    assert check_not_empty(table("flights").limit(0)).failures == 1
    assert check_not_empty(table("flights")).passed


def test_an_empty_flights_table_still_fails_validation(table):
    failed = [r.check for r in validate_flights(table("flights").limit(0)) if not r.passed]
    assert "table is not empty" in failed


def test_an_empty_holds_table_is_a_measurement_not_a_failure(spark):
    """No aircraft circled at these airports today is a result, not a bug.

    Real: 2025-12-25 detected no holds at ZRH or DUS. Every other table has a
    row per flight or per observation, so emptiness there stays a failure --
    the test above holds that line.
    """
    empty = spark.createDataFrame([], StructType([
        StructField("flight_id", StringType()),
        StructField("hold_seq", IntegerType()),
        StructField("hold_start", TimestampType()),
        StructField("hold_end", TimestampType()),
        StructField("duration_seconds", LongType()),
        StructField("span_km", DoubleType()),
        StructField("circuits", DoubleType()),
        StructField("min_altitude_ft", DoubleType()),
        StructField("max_altitude_ft", DoubleType()),
        StructField("centroid_latitude", DoubleType()),
        StructField("centroid_longitude", DoubleType()),
    ]))

    results = validate_flight_holds(empty)

    assert [r.check for r in results if not r.passed] == []
    assert "table is not empty" not in [r.check for r in results]


def test_duplicate_keys_are_counted(table):
    doubled = table("observations").union(table("observations"))

    assert check_unique(doubled, ("icao", "event_time")).failures == 1
    assert check_unique(table("observations"), ("icao", "event_time")).passed


def test_lost_observations_are_detected(table):
    """Flights must account for every observation, exactly once."""
    observations = table("observations").union(table("observations").limit(1))

    matching = table("flights", n_observations=2)
    assert check_observations_conserved(observations, matching).passed

    dropping = table("flights", n_observations=1)
    assert check_observations_conserved(observations, dropping).failures == 1


def test_orphan_children_are_detected(table):
    flights = table("flights")

    assert check_references(table("observations"), flights).passed
    orphan = table("observations", flight_id="zzz999_20251230080000")
    assert check_references(orphan, flights).failures == 1


def test_collisions_resolved_reports_what_the_decoder_collapsed(spark):
    """The number used to be visible only as a difference between row counts."""
    aircraft = spark.createDataFrame(
        [("a1b2c3", [["1"], ["2"], ["3"]]), ("d4e5f6", [["1"], ["2"]])],
        "icao string, trace array<array<string>>",
    )
    kept = spark.createDataFrame([(i,) for i in range(4)], "n int")

    points, observations = collisions_resolved(aircraft, kept)

    assert (points, observations) == (5, 4)


def test_check_rows_reports_the_number_of_offending_rows(table):
    two_bad = table("observations", latitude=91.0).union(
        table("observations", latitude=91.0)
    )

    results = check_rows(two_bad, {"latitude in range": "latitude > 90"})

    assert results[0].failures == 2


def test_assert_valid_names_every_failing_check(table):
    results = validate_airport_operations(
        table("airport_daily_operations", total_operations=99, unique_aircraft=100)
    )

    with pytest.raises(DataQualityError) as raised:
        assert_valid("airport_daily_operations", results)

    message = str(raised.value)
    assert "arrivals and departures sum to total_operations" in message
    assert "unique_aircraft does not exceed operations" in message
    assert "airport_daily_operations failed 2 of" in message


def test_assert_valid_is_silent_when_everything_passes(table):
    assert assert_valid(
        "airport_daily_operations",
        validate_airport_operations(table("airport_daily_operations")),
    ) is None
