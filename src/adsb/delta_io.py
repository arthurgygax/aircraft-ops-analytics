"""Partitioned, idempotent Delta writes, and the lifecycle around them.

Every table writes one day at a time and must be safe to re-run, so all six
share this. It is a module of three functions, not a framework.

WHY release_date IS REQUIRED
    A write is scoped to one day unless the caller says otherwise in as many
    words. ``mode("overwrite")`` without a partition predicate tombstones the
    *whole* table, and when that is what happens by default -- because an
    argument was omitted -- every exploratory re-run leaves a full dead copy
    behind. Ten of those grew this project's object store to 29 GB for ~4 GB of
    live data. The destructive path now has to be asked for by name:
    ``full_rebuild=True``.

WHY replaceWhere AND NOT MERGE
    Reprocessing a day should reproduce that day exactly, not reconcile it row
    by row. ``replaceWhere`` atomically swaps one partition's files and leaves
    every other partition's files untouched, which is precisely "rebuild this
    day, keep the others". MERGE would be the tool if we received corrections
    to individual observations; we do not -- we receive whole days.

WHY release_date IS THE PARTITION KEY
    One adsb.lol release is one UTC day, and ``release_date`` is derived from
    the release *identifier* rather than from row timestamps. A key derived
    from the data could let a stray observation near midnight drag another
    day's partition into the write, so reprocessing day N would destroy part of
    day N-1. Deriving it from the release makes that impossible. (Measured: no
    trace offset in either release falls outside [0, 86400], so the two agree
    today -- the guarantee is what matters.)

WHY VACUUM IS A PIPELINE STEP
    Reclaiming is part of writing, not a chore someone remembers. Every
    overwrite tombstones the files it replaced; nothing else removes them.
    ``vacuum_tables`` runs at the end of a successful pipeline run.
"""

from __future__ import annotations

import os
import re

from pyspark.sql import DataFrame

RELEASE_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")

PARTITION_COLUMN = "release_date"


def _root() -> str:
    """Table root. ``ADSB_ROOT`` is what separates a dev run from a real one."""
    return os.environ.get("ADSB_ROOT", "s3a://adsb").rstrip("/")


def table_uri(name: str) -> str:
    return f"{_root()}/{name}"


# The six persisted tables, and how long each keeps its old versions.
#
# observations is 44.6M rows and fully reproducible from the raw objects, so
# its history buys nothing a rebuild would not: reclaim it aggressively. The
# small tables are where a threshold change is visible from one run to the
# next, so they keep Delta's default week.
RETENTION_HOURS: dict[str, float] = {
    "observations": 24.0,
    "flights": 168.0,
    "movements": 168.0,
    "flight_phases": 168.0,
    "flight_holds": 168.0,
    "airport_daily_operations": 168.0,
}

TABLES = tuple(RETENTION_HOURS)


def release_date_predicate(release_date: str) -> str:
    """A ``replaceWhere`` predicate for one day.

    The date is validated rather than interpolated blindly -- it reaches here
    from a command line and ends up inside a SQL predicate.
    """
    if not RELEASE_DATE_PATTERN.match(release_date):
        raise ValueError(
            f"release_date must look like YYYY-MM-DD, got {release_date!r}"
        )
    return f"{PARTITION_COLUMN} = '{release_date}'"


def write_delta(
    df: DataFrame,
    path: str,
    release_date: str | None = None,
    full_rebuild: bool = False,
    partition_by: str | None = PARTITION_COLUMN,
) -> None:
    """Replace one day's partition, or -- only when asked -- the whole table.

    With ``release_date``: overwrite just that day, leaving the rest of the
    table alone. Re-running is a no-op in effect; the day's files are replaced
    by an identical set, so the result cannot accumulate duplicates.

    With ``full_rebuild=True``: overwrite everything, and allow the schema to
    change. This is the only path that may drop days, and the only path that
    tombstones the whole table.
    """
    if full_rebuild and release_date is not None:
        raise ValueError("a full rebuild replaces every day; do not also pass one")
    if not full_rebuild and release_date is None:
        raise ValueError(
            "write_delta needs a release_date. Pass full_rebuild=True to replace "
            "the whole table on purpose."
        )

    writer = df.write.format("delta").mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(partition_by)

    if full_rebuild:
        # only a whole-table rebuild may change the schema
        writer = writer.option("overwriteSchema", "true")
    else:
        # Delta verifies the written rows all satisfy this predicate, so a day
        # carrying another day's rows fails loudly instead of silently
        # clobbering a neighbouring partition.
        writer = writer.option("replaceWhere", release_date_predicate(release_date))

    writer.save(path)


def vacuum_tables(
    spark,
    tables: dict[str, float] | None = None,
    override_hours: float | None = None,
) -> list[tuple[str, int, int]]:
    """Reclaim tombstoned files. Returns (table, live_files, live_bytes).

    Retention is per table (see ``RETENTION_HOURS``); ``override_hours`` forces
    one value everywhere, which is what a development root wants. Delta's own
    floor is 168 h and protects readers still holding an older snapshot, so
    going below it needs the check disabled -- appropriate when nothing else is
    reading, not otherwise.
    """
    from delta.tables import DeltaTable

    plan = dict(tables or RETENTION_HOURS)
    if override_hours is not None:
        plan = {name: override_hours for name in plan}
    if any(hours < 168.0 for hours in plan.values()):
        spark.conf.set(
            "spark.databricks.delta.retentionDurationCheck.enabled", "false"
        )

    results = []
    for name, hours in plan.items():
        uri = table_uri(name)
        try:
            table = DeltaTable.forPath(spark, uri)
        except Exception:  # a table this run did not create yet
            continue
        table.vacuum(retentionHours=hours)
        detail = table.detail().first()
        results.append((name, detail["numFiles"], detail["sizeInBytes"]))
    return results


def main(argv: list[str] | None = None) -> None:
    """``python -m adsb.delta_io`` reclaims dead files across every table."""
    import argparse

    from adsb.spark_explore import build_session

    parser = argparse.ArgumentParser(description="Delta table maintenance")
    parser.add_argument(
        "--retention-hours",
        type=float,
        default=None,
        help="override the per-table retention (0 only when nothing is reading)",
    )
    args = parser.parse_args(argv)

    spark = build_session("adsb-vacuum")
    try:
        print(f"Vacuuming {_root()}")
        for name, files, size in vacuum_tables(
            spark, override_hours=args.retention_hours
        ):
            hours = args.retention_hours
            if hours is None:
                hours = RETENTION_HOURS[name]
            print(
                f"  {name:<26} {files:>5} live files  {size / 1e9:5.2f} GB"
                f"  (kept {hours:g}h)"
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
