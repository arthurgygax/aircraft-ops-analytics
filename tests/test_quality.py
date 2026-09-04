from datetime import date, datetime

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

from adsb.quality import (  # noqa: E402
    DataQualityError,
    assert_valid,
    check_not_empty,
    check_observations_conserved,
    check_rows,
    check_unique,
    validate_bronze,
    validate_flight_segments,
    validate_gold,
    validate_silver,
)

T0 = datetime(2025, 12, 30, 8, 0, 0)
T1 = datetime(2025, 12, 30, 9, 0, 0)

# One valid row per layer; tests corrupt a single field to prove each rule bites.
LAYERS = {
    "bronze": (
        validate_bronze,
        StructType([
            StructField("icao", StringType()),
            StructField("event_time", TimestampType()),
            StructField("latitude", DoubleType()),
            StructField("longitude", DoubleType()),
            StructField("source_file", StringType()),
            StructField("release_tag", StringType()),
            StructField("ingested_at", TimestampType()),
        ]),
        {"icao": "a1b2c3", "event_time": T0, "latitude": 47.4, "longitude": 8.5,
         "source_file": "s3a://adsb/raw/x.json.gz", "release_tag": "v2025.12.30",
         "ingested_at": T0},
    ),
    "silver": (
        validate_silver,
        StructType([
            StructField("icao", StringType()),
            StructField("event_time", TimestampType()),
            StructField("latitude", DoubleType()),
            StructField("longitude", DoubleType()),
            StructField("is_icao_address", BooleanType()),
            StructField("ground_speed_kt", DoubleType()),
            StructField("vertical_rate_fpm", DoubleType()),
            StructField("callsign", StringType()),
            StructField("release_tag", StringType()),
        ]),
        {"icao": "a1b2c3", "event_time": T0, "latitude": 47.4, "longitude": 8.5,
         "is_icao_address": True, "ground_speed_kt": 300.0,
         "vertical_rate_fpm": 500.0, "callsign": "SWR1",
         "release_tag": "v2025.12.30"},
    ),
    "flight_segments": (
        validate_flight_segments,
        StructType([
            StructField("segment_id", StringType()),
            StructField("icao", StringType()),
            StructField("start_time", TimestampType()),
            StructField("end_time", TimestampType()),
            StructField("duration_seconds", LongType()),
            StructField("n_observations", LongType()),
            StructField("start_latitude", DoubleType()),
            StructField("start_longitude", DoubleType()),
            StructField("end_latitude", DoubleType()),
            StructField("end_longitude", DoubleType()),
        ]),
        {"segment_id": "s1", "icao": "a1b2c3", "start_time": T0, "end_time": T1,
         "duration_seconds": 3600, "n_observations": 10, "start_latitude": 47.4,
         "start_longitude": 8.5, "end_latitude": 46.0, "end_longitude": 7.0},
    ),
    "gold": (
        validate_gold,
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
        ]),
        {"operations_date": date(2025, 12, 30), "airport_ident": "LSZH",
         "arrivals": 3, "departures": 2, "total_operations": 5,
         "unique_aircraft": 4, "airport_latitude": 47.458, "airport_longitude": 8.548,
         "first_operation_time": T0, "last_operation_time": T1,
         "metric_source": "adsb_inferred"},
    ),
}


@pytest.fixture
def table(spark):
    def _make(layer, **overrides):
        _, schema, valid = LAYERS[layer]
        row = {**valid, **overrides}
        return spark.createDataFrame([tuple(row[f.name] for f in schema)], schema)

    return _make


@pytest.mark.parametrize("layer", list(LAYERS))
def test_a_valid_row_passes_every_check(table, layer):
    validator = LAYERS[layer][0]

    failures = [r for r in validator(table(layer)) if not r.passed]

    assert failures == []


# Each case corrupts one field and names the check that must catch it.
CORRUPTIONS = [
    ("bronze", {"icao": None}, "icao is present"),
    ("bronze", {"latitude": 91.0}, "latitude within [-90, 90]"),
    ("bronze", {"longitude": -181.0}, "longitude within [-180, 180]"),
    ("bronze", {"event_time": None}, "event_time is present"),
    # the regex regression that actually happened
    ("bronze", {"release_tag": ""}, "release_tag is recorded"),
    ("silver", {"ground_speed_kt": 1800.0}, "implausible ground speed removed"),
    ("silver", {"vertical_rate_fpm": -64000.0}, "implausible vertical rate removed"),
    ("silver", {"callsign": ""}, "blank callsign normalized to NULL"),
    ("silver", {"is_icao_address": None}, "is_icao_address is set"),
    ("flight_segments", {"end_time": datetime(2025, 12, 30, 7, 0, 0)},
     "segment does not end before it starts"),
    ("flight_segments", {"duration_seconds": -1}, "duration is not negative"),
    ("flight_segments", {"n_observations": 0},
     "segment has at least one observation"),
    ("flight_segments", {"duration_seconds": 12345},
     "duration agrees with the timestamps"),
    ("gold", {"total_operations": 99},
     "arrivals and departures sum to total_operations"),
    ("gold", {"unique_aircraft": 99}, "unique_aircraft does not exceed operations"),
    ("gold", {"operations_date": None}, "operations_date is present"),
    ("gold", {"airport_ident": None}, "airport_ident is present"),
    ("gold", {"metric_source": "official"}, "every row is labelled as inferred"),
    ("gold", {"last_operation_time": datetime(2025, 12, 30, 7, 0, 0)},
     "last operation is not before the first"),
    ("gold", {"first_operation_time": datetime(2025, 12, 31, 8, 0, 0)},
     "operations fall on the reported date"),
]


@pytest.mark.parametrize("layer,corruption,expected_check", CORRUPTIONS)
def test_a_corrupt_row_is_caught_by_the_right_check(
    table, layer, corruption, expected_check
):
    """A check that never fires is worse than no check at all."""
    validator = LAYERS[layer][0]

    failed = [r.check for r in validator(table(layer, **corruption)) if not r.passed]

    assert expected_check in failed


def test_range_checks_ignore_missing_values(table):
    """A NULL coordinate is missing, not out of range; only IS NULL rules police it."""
    failed = [r.check for r in validate_bronze(table("bronze", latitude=None))
              if not r.passed]

    assert "latitude within [-90, 90]" not in failed


def test_an_empty_table_is_a_failure(spark, table):
    """The silent-empty failure mode: everything downstream builds, emptily."""
    empty = table("bronze").limit(0)

    assert check_not_empty(empty).failures == 1
    assert check_not_empty(table("bronze")).passed


def test_duplicate_keys_are_counted(spark, table):
    doubled = table("silver").union(table("silver"))

    assert check_unique(doubled, ("icao", "event_time")).failures == 1
    assert check_unique(table("silver"), ("icao", "event_time")).passed


def test_lost_observations_are_detected(spark, table):
    """Segments must account for every observation, exactly once."""
    observations = table("silver").union(table("silver").limit(1))  # 2 rows

    matching = table("flight_segments", n_observations=2)
    assert check_observations_conserved(observations, matching).passed

    dropping = table("flight_segments", n_observations=1)
    assert check_observations_conserved(observations, dropping).failures == 1


def test_check_rows_reports_the_number_of_offending_rows(spark, table):
    three_bad = table("bronze", latitude=91.0).union(table("bronze", latitude=91.0))

    results = check_rows(three_bad, {"latitude in range": "latitude > 90"})

    assert results[0].failures == 2


def test_assert_valid_names_every_failing_check(table):
    results = validate_gold(table("gold", total_operations=99, unique_aircraft=100))

    with pytest.raises(DataQualityError) as raised:
        assert_valid("gold", results)

    message = str(raised.value)
    assert "arrivals and departures sum to total_operations" in message
    assert "unique_aircraft does not exceed operations" in message
    assert "gold failed 2 of" in message


def test_assert_valid_is_silent_when_everything_passes(table):
    assert_valid("gold", validate_gold(table("gold"))) is None
