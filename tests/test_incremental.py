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
            full_rebuild=not incremental,
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


def test_a_day_is_written_as_one_file_not_one_per_shuffle_partition(table):
    """The tiny-file guard.

    Spark's default 200 shuffle partitions once turned a 731-row day of
    movements into 195 files of four rows, and the explorer pays for each as a
    separate object read. ``write_delta`` repartitions by the partition column,
    so a day is one file however many partitions produced it.
    """
    table(DAY1, [(f"a{n:04d}", n) for n in range(500)])

    assert len(table.parquet_files(DAY1)) == 1


def test_one_day_can_be_read_without_touching_the_others(table):
    """Step one of the app's access pattern: filter by date.

    The trajectory store is partitioned so this prunes rather than scans; the
    partition layout is what makes it a directory choice rather than a filter.
    """
    table(DAY1, [("aaa", 1), ("bbb", 2)])
    table(DAY2, [("ccc", 3)])

    one_day = table.read().where(f"release_date = '{DAY2}'")

    assert {(r.icao, r.value) for r in one_day.collect()} == {("ccc", 3)}
    assert len(table.parquet_files(DAY2)) == 1, "the day is one prunable file"


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


# the lifecycle rules: day-scoped by default, full rebuilds by name


def test_a_write_without_a_day_is_refused(table):
    """The destructive path has to be asked for, not fallen into.

    A bare overwrite tombstones the whole table. Ten exploratory runs doing
    that is how the object store reached 29 GB for ~4 GB of live data.
    """
    with pytest.raises(ValueError, match="release_date"):
        write_delta(table.day(DAY1, [("aaa", 1)]), table.path)


def test_a_full_rebuild_replaces_every_day(table):
    table(DAY1, [("aaa", 1), ("bbb", 2)])
    table(DAY2, [("ccc", 3)])

    table(DAY2, [("zzz", 9)], incremental=False)

    assert rows_by_day(table.read()) == {DAY2: 1}, "day 1 was meant to go"


def test_a_full_rebuild_and_a_day_are_mutually_exclusive(table):
    with pytest.raises(ValueError):
        write_delta(
            table.day(DAY1, [("aaa", 1)]),
            table.path,
            release_date=DAY1,
            full_rebuild=True,
        )


# the pipeline carries the partition key through


def test_observations_carry_the_partition_key_through(spark, raw_path):
    from adsb.observations import to_observations
    from adsb.spark_explore import read_aircraft

    observations = to_observations(read_aircraft(spark, raw_path))

    assert "release_date" in observations.columns
    assert {str(r.release_date) for r in observations.collect()} == {"2025-12-30"}
