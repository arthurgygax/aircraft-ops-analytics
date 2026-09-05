from datetime import date, datetime, timedelta

import pytest

pyspark = pytest.importorskip("pyspark", reason="Spark tests run in the spark container")

from pyspark.sql.types import (  # noqa: E402
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from adsb.bi_export import to_movements, to_track_sample, write_parquet  # noqa: E402

T0 = datetime(2025, 12, 30, 8, 0, 0)
RELEASE_DATE = date(2025, 12, 30)

# the columns airport_movements() produces that the export reads
MOVEMENT_SCHEMA = StructType([
    StructField("segment_id", StringType()),
    StructField("movement_type", StringType()),
    StructField("event_time", TimestampType()),
    StructField("ident", StringType()),
    StructField("iata_code", StringType()),
    StructField("airport_name", StringType()),
    StructField("iso_country", StringType()),
    StructField("distance_km", DoubleType()),
    StructField("release_date", DateType()),
])

POINT_SCHEMA = StructType([
    StructField("flight_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("observation_seq", IntegerType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("altitude_ft", DoubleType()),
    StructField("ground_speed_kt", DoubleType()),
    StructField("track_deg", DoubleType()),
    StructField("release_date", DateType()),
])

HOLD_SCHEMA = StructType([
    StructField("flight_id", StringType()),
    StructField("duration_seconds", IntegerType()),
])


@pytest.fixture
def movements(spark):
    return spark.createDataFrame([
        ("a_1", "departure", T0, "LSZH", "ZRH", "Zurich", "CH", 1.234, RELEASE_DATE),
        ("a_1", "arrival", T0 + timedelta(hours=1), "EGLL", None, "Heathrow", "GB",
         2.5, RELEASE_DATE),
    ], MOVEMENT_SCHEMA)


def points(spark, flight_id, seconds, step=5):
    return spark.createDataFrame([
        (flight_id, T0 + timedelta(seconds=i * step), i + 1,
         47.0 + i * 0.001, 8.0 + i * 0.001, 10000.0, 300.0, 90.0, RELEASE_DATE)
        for i in range(seconds // step)
    ], POINT_SCHEMA)


# --- movements ---------------------------------------------------------------


def test_movements_are_renamed_to_the_reporting_names(movements):
    result = to_movements(movements)

    assert set(result.columns) == {
        "flight_id", "movement_type", "movement_time", "airport_ident",
        "airport_iata", "airport_name", "iso_country", "distance_km",
        "release_date",
    }


def test_a_movement_keeps_its_flight_and_direction(movements):
    rows = {r.movement_type: r for r in to_movements(movements).collect()}

    assert rows["departure"].flight_id == "a_1"
    assert rows["departure"].airport_ident == "LSZH"
    assert rows["arrival"].airport_ident == "EGLL"


def test_an_airport_without_an_iata_code_still_produces_a_movement(movements):
    """1.7% of real movements are at airports with no IATA code."""
    arrival = [r for r in to_movements(movements).collect()
               if r.movement_type == "arrival"][0]

    assert arrival.airport_iata is None
    assert arrival.airport_ident == "EGLL", "the ICAO-style key is always present"


# --- trajectory sample -------------------------------------------------------


def test_only_flights_with_a_detected_hold_are_exported(spark):
    holding = points(spark, "held_1", 300)
    plain = points(spark, "plain_2", 300)
    holds = spark.createDataFrame([("held_1", 400)], HOLD_SCHEMA)

    sample = to_track_sample(holding.union(plain), holds, seconds=30)

    assert {r.flight_id for r in sample.collect()} == {"held_1"}


def test_the_sample_keeps_one_point_per_time_bucket(spark):
    """300 seconds at 5 s spacing thins to one point per 30 s bucket."""
    holds = spark.createDataFrame([("held_1", 400)], HOLD_SCHEMA)

    sample = to_track_sample(points(spark, "held_1", 300), holds, seconds=30)

    assert sample.count() == 10, "300s / 30s buckets"


def test_the_bucket_size_is_adjustable(spark):
    holds = spark.createDataFrame([("held_1", 400)], HOLD_SCHEMA)
    track = points(spark, "held_1", 300)

    assert to_track_sample(track, holds, seconds=60).count() == 5
    assert to_track_sample(track, holds, seconds=10).count() == 30


def test_thinning_keeps_the_first_point_of_each_bucket(spark):
    """Deterministic: the same rows come back every run."""
    holds = spark.createDataFrame([("held_1", 400)], HOLD_SCHEMA)
    track = points(spark, "held_1", 300)

    first = sorted(r.observation_seq for r in to_track_sample(track, holds, 30).collect())
    second = sorted(r.observation_seq for r in to_track_sample(track, holds, 30).collect())

    assert first == second
    assert first[0] == 1, "each bucket contributes its earliest observation"


def test_the_sample_carries_what_a_map_and_profile_need(spark):
    holds = spark.createDataFrame([("held_1", 400)], HOLD_SCHEMA)

    sample = to_track_sample(points(spark, "held_1", 300), holds, seconds=30)

    assert set(sample.columns) == {
        "flight_id", "event_time", "observation_seq", "latitude", "longitude",
        "altitude_ft", "ground_speed_kt", "track_deg", "release_date",
    }


# --- writing -----------------------------------------------------------------


def test_each_table_is_written_as_one_openable_file(spark, tmp_path, movements):
    """Power BI's Parquet connector wants a file, not a directory of parts."""
    name, rows, size = write_parquet(to_movements(movements), tmp_path, "movements")

    target = tmp_path / "movements.parquet"
    assert target.is_file()
    assert (name, rows) == ("movements", 2)
    assert size > 0
    assert not list(tmp_path.glob("_*_staging")), "staging directory cleaned up"
    assert spark.read.parquet(f"file://{target}").count() == 2
