from datetime import date, datetime

import pytest

pyspark = pytest.importorskip("pyspark", reason="Spark tests run in the spark container")

from pyspark.sql.types import (  # noqa: E402
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from adsb.delta_io import release_date_predicate, write_delta  # noqa: E402
from adsb.ingest import release_date  # noqa: E402

DAY1 = "2025-12-30"
DAY2 = "2025-12-31"

OBS_SCHEMA = StructType([
    StructField("icao", StringType()),
    StructField("value", LongType()),
    StructField("release_date", DateType()),
])


@pytest.fixture
def table(spark, tmp_path):
    """A Delta table you can write days into, and read back."""
    path = str(tmp_path / "observations")

    def day(day_str, rows):
        d = date.fromisoformat(day_str)
        return spark.createDataFrame([(i, v, d) for i, v in rows], OBS_SCHEMA)

    def write(day_str, rows, incremental=True):
        write_delta(
            day(day_str, rows),
            path,
            release_date=day_str if incremental else None,
        )

    def read():
        return spark.read.format("delta").load(path)

    def parquet_files(day_str):
        partition = tmp_path / "observations" / f"release_date={day_str}"
        return sorted(p.name for p in partition.glob("*.parquet"))

    write.read = read
    write.path = path
    write.parquet_files = parquet_files
    write.day = day
    return write


def rows_by_day(df):
    counted = df.groupBy("release_date").count().collect()
    return {str(r["release_date"]): r["count"] for r in counted}


# 1. first processing


def test_processing_the_first_day_creates_the_table(table):
    table(DAY1, [("aaa", 1), ("bbb", 2)])

    result = table.read()

    assert result.count() == 2
    assert rows_by_day(result) == {DAY1: 2}


# 2. processing a second day


def test_a_second_day_is_added_alongside_the_first(table):
    table(DAY1, [("aaa", 1), ("bbb", 2)])

    table(DAY2, [("ccc", 3)])

    assert rows_by_day(table.read()) == {DAY1: 2, DAY2: 1}


def test_adding_a_second_day_does_not_rewrite_the_first(table):
    """The point of incremental processing: day 1's files are never touched."""
    table(DAY1, [("aaa", 1), ("bbb", 2)])
    before = table.parquet_files(DAY1)

    table(DAY2, [("ccc", 3)])

    assert table.parquet_files(DAY1) == before, "day 1 was rewritten"
    assert before, "expected day 1 to have parquet files"


# 3. processing the same day twice


def test_reprocessing_a_day_does_not_duplicate_it(table):
    table(DAY1, [("aaa", 1), ("bbb", 2)])
    table(DAY2, [("ccc", 3)])

    table(DAY2, [("ccc", 3)])  # exactly the same input again

    assert rows_by_day(table.read()) == {DAY1: 2, DAY2: 1}
    assert table.read().count() == 3


def test_reprocessing_replaces_a_day_rather_than_appending_to_it(table):
    """Deterministic: the day ends up as whatever the latest run produced."""
    table(DAY1, [("aaa", 1)])
    table(DAY2, [("ccc", 3), ("ddd", 4)])

    table(DAY2, [("eee", 9)])  # corrected upstream data for that day

    result = table.read()
    assert rows_by_day(result) == {DAY1: 1, DAY2: 1}
    assert {r.icao for r in result.where("release_date = '2025-12-31'").collect()} == {
        "eee"
    }


# 4. the rest of the data stays correct


def test_reprocessing_one_day_leaves_the_others_untouched(table):
    table(DAY1, [("aaa", 1), ("bbb", 2)])
    table(DAY2, [("ccc", 3)])
    day1_files = table.parquet_files(DAY1)

    table(DAY2, [("zzz", 99)])

    day1 = table.read().where("release_date = '2025-12-30'")
    assert {(r.icao, r.value) for r in day1.collect()} == {("aaa", 1), ("bbb", 2)}
    assert table.parquet_files(DAY1) == day1_files, "day 1 was rewritten"


def test_a_day_cannot_overwrite_a_different_days_partition(table):
    """Guards the hazard that makes release_date safe to key on."""
    table(DAY1, [("aaa", 1)])

    with pytest.raises(Exception) as raised:
        # rows labelled DAY2, but told to replace DAY1
        write_delta(table.day(DAY2, [("ccc", 3)]), table.path, release_date=DAY1)

    assert table.read().count() == 1, "the bad write must not have landed"
    assert "replaceWhere" in str(raised.value) or "CHECK" in str(raised.value).upper()


# supporting pieces


@pytest.mark.parametrize(
    "tag,expected",
    [
        ("v2025.12.30-planes-readsb-prod-0", "2025-12-30"),
        ("v2026.01.05-planes-readsb-staging-0", "2026-01-05"),
    ],
)
def test_release_date_is_read_from_the_release_tag(tag, expected):
    assert release_date(tag) == expected


def test_an_unparseable_release_tag_is_rejected():
    with pytest.raises(ValueError):
        release_date("not-a-release")


def test_the_replace_where_predicate_rejects_anything_but_a_date():
    assert release_date_predicate("2025-12-30") == "release_date = '2025-12-30'"
    for junk in ("2025-12-30' OR '1'='1", "yesterday", ""):
        with pytest.raises(ValueError):
            release_date_predicate(junk)


# the layers carry the partition key through


def test_silver_carries_the_partition_key_through(spark):
    from adsb.silver import to_silver

    bronze_schema = StructType([
        StructField("icao", StringType()),
        StructField("event_time", StructField("x", StringType()).dataType),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("on_ground", StringType()),
        StructField("altitude_ft", DoubleType()),
        StructField("ground_speed_kt", DoubleType()),
        StructField("track_deg", DoubleType()),
        StructField("vertical_rate_fpm", DoubleType()),
        StructField("callsign", StringType()),
        StructField("registration", StringType()),
        StructField("aircraft_type", StringType()),
        StructField("operator", StringType()),
        StructField("release_tag", StringType()),
        StructField("release_date", DateType()),
        StructField("ingested_at", StringType()),
    ])
    row = ("a1b2c3", "2025-12-30 10:00:00", 47.4, 8.5, "false", 1000.0, 250.0,
           90.0, 0.0, "SWR1", "HB-ABC", "A320", "SWISS", "v2025.12.30",
           date.fromisoformat(DAY1), "2026-01-01 00:00:00")

    silver = to_silver(spark.createDataFrame([row], bronze_schema))

    assert "release_date" in silver.columns
    assert str(silver.first().release_date) == DAY1
