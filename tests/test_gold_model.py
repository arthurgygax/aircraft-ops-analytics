from datetime import date, datetime, timedelta

import pytest

pyspark = pytest.importorskip("pyspark", reason="Spark tests run in the spark container")

from pyspark.sql.types import (  # noqa: E402
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from adsb.gold import to_gold_flights  # noqa: E402
from adsb.quality import check_references, validate_gold_flights  # noqa: E402

T0 = datetime(2025, 12, 30, 8, 0, 0)
RELEASE_DATE = date(2025, 12, 30)

# the silver.flights columns gold.flights reads
SILVER_FLIGHT_SCHEMA = StructType([
    StructField("flight_id", StringType()),
    StructField("icao", StringType()),
    StructField("registration", StringType()),
    StructField("aircraft_type", StringType()),
    StructField("callsign", StringType()),
    StructField("airline_icao", StringType()),
    StructField("registered_owner", StringType()),
    StructField("departure_airport_ident", StringType()),
    StructField("departure_airport_iata", StringType()),
    StructField("departure_airport_name", StringType()),
    StructField("departure_time", TimestampType()),
    StructField("arrival_airport_ident", StringType()),
    StructField("arrival_airport_iata", StringType()),
    StructField("arrival_airport_name", StringType()),
    StructField("arrival_time", TimestampType()),
    StructField("first_seen_time", TimestampType()),
    StructField("last_seen_time", TimestampType()),
    StructField("duration_seconds", LongType()),
    StructField("n_observations", LongType()),
    StructField("max_altitude_ft", DoubleType()),
    StructField("max_ground_speed_kt", DoubleType()),
    StructField("saw_ground", BooleanType()),
    StructField("release_tag", StringType()),
    StructField("release_date", DateType()),
])

HOLD_SCHEMA = StructType([
    StructField("flight_id", StringType()),
    StructField("duration_seconds", LongType()),
    StructField("arrival_airport_ident", StringType()),
    StructField("release_date", DateType()),
])


def silver_flight(flight_id="a1b2c3_20251230080000", airline="SWR", dep="LSZH",
                  arr="EGLL", duration=3600):
    return (flight_id, "a1b2c3", "HB-ABC", "A320", "SWR123", airline, "SWISS",
            dep, "ZRH", "Zurich", T0,
            arr, "LHR", "Heathrow", T0 + timedelta(seconds=duration),
            T0, T0 + timedelta(seconds=duration), duration, 300,
            35000.0, 450.0, True, "v2025.12.30", RELEASE_DATE)


@pytest.fixture
def flights(spark):
    return lambda rows: spark.createDataFrame(rows, SILVER_FLIGHT_SCHEMA)


@pytest.fixture
def holds(spark):
    return lambda rows: spark.createDataFrame(rows, HOLD_SCHEMA)


# --- the spine ---------------------------------------------------------------


def test_gold_flights_has_one_row_per_flight(flights, holds):
    rows = [silver_flight("aaa_1"), silver_flight("bbb_2")]

    gold = to_gold_flights(flights(rows), holds([]))

    assert gold.count() == 2
    assert gold.select("flight_id").distinct().count() == 2


def test_the_filter_attributes_survive_into_gold(flights, holds):
    """Date, airline, type and both airports are what the dashboard filters on."""
    gold = to_gold_flights(flights([silver_flight()]), holds([])).first()

    assert str(gold.flight_date) == "2025-12-30"
    assert gold.airline_icao == "SWR"
    assert gold.aircraft_type == "A320"
    assert gold.departure_airport_ident == "LSZH"
    assert gold.arrival_airport_ident == "EGLL"
    assert gold.callsign == "SWR123"


def test_flight_date_comes_from_the_observation_not_the_processing_day(flights, holds):
    """release_date is when data was processed; flight_date is when it flew."""
    gold = to_gold_flights(flights([silver_flight()]), holds([])).first()

    assert gold.flight_date == gold.first_seen_time.date()
    assert "release_date" in gold.asDict(), "processing day is kept alongside"


# --- hold rollups ------------------------------------------------------------


def test_a_flight_without_holds_reports_zero_not_null(flights, holds):
    """Power BI should not have to coalesce nulls to count flights without holds."""
    gold = to_gold_flights(flights([silver_flight()]), holds([])).first()

    assert gold.n_detected_holds == 0
    assert gold.has_detected_hold is False
    assert gold.total_hold_seconds == 0


def test_hold_rollups_summarize_the_holds_of_that_flight(flights, holds):
    hold_rows = [
        ("aaa_1", 300, "EGLL", RELEASE_DATE),
        ("aaa_1", 480, "EGLL", RELEASE_DATE),
    ]

    gold = to_gold_flights(flights([silver_flight("aaa_1")]), holds(hold_rows)).first()

    assert gold.n_detected_holds == 2
    assert gold.has_detected_hold is True
    assert gold.total_hold_seconds == 780


def test_holds_are_attributed_only_to_their_own_flight(flights, holds):
    rows = [silver_flight("aaa_1"), silver_flight("bbb_2")]
    hold_rows = [("aaa_1", 300, "EGLL", RELEASE_DATE)]

    gold = {r.flight_id: r for r in to_gold_flights(flights(rows), holds(hold_rows)).collect()}

    assert gold["aaa_1"].n_detected_holds == 1
    assert gold["bbb_2"].n_detected_holds == 0


# --- integrity ---------------------------------------------------------------


def test_a_valid_gold_flights_row_passes_every_check(flights, holds):
    gold = to_gold_flights(flights([silver_flight()]), holds([]))

    assert [r.check for r in validate_gold_flights(gold) if not r.passed] == []


def test_duplicate_flight_ids_are_caught(flights, holds):
    duplicated = [silver_flight("aaa_1"), silver_flight("aaa_1")]

    failed = [r.check for r in validate_gold_flights(
        to_gold_flights(flights(duplicated), holds([]))) if not r.passed]

    assert "flight_id is unique" in failed


def test_an_orphan_child_row_is_detected(spark, flights, holds):
    """A phase, hold or track belonging to no flight would break every join."""
    gold = to_gold_flights(flights([silver_flight("aaa_1")]), holds([]))
    orphan = spark.createDataFrame(
        [("ghost_9", 100, None, RELEASE_DATE)], HOLD_SCHEMA
    )

    assert check_references(orphan, gold).failures == 1
    assert check_references(
        spark.createDataFrame([("aaa_1", 100, None, RELEASE_DATE)], HOLD_SCHEMA), gold
    ).passed


def test_inconsistent_hold_rollups_are_caught(spark, flights, holds):
    """has_detected_hold must never disagree with n_detected_holds."""
    gold = to_gold_flights(flights([silver_flight()]), holds([]))
    tampered = gold.withColumn(
        "has_detected_hold", gold.has_detected_hold.cast("boolean") | pyspark.sql.functions.lit(True)
    )

    failed = [r.check for r in validate_gold_flights(tampered) if not r.passed]

    assert "hold rollups agree with each other" in failed
