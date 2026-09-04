"""Partitioned, idempotent Delta writes.

Every layer writes one day at a time and must be safe to re-run, so all four
share this. It is a single function, not a framework: four call sites needing
identical semantics is the reason it exists.

WHY replaceWhere AND NOT MERGE
    Reprocessing a day should reproduce that day exactly, not reconcile it row
    by row. ``replaceWhere`` atomically swaps one partition's files and leaves
    every other partition's files untouched, which is precisely "rebuild this
    day, keep the others". MERGE would be the tool if we received corrections
    to individual observations; we do not -- we receive whole days.

WHY release_date IS THE PARTITION KEY
    One adsb.lol release is one UTC day, and ``release_date`` is derived from
    the release *identifier* rather than from row timestamps. That matters:
    a key derived from the data could let a stray observation near midnight
    drag another day's partition into the write, and reprocessing day N would
    then destroy part of day N-1. Deriving it from the release makes that
    impossible.
"""

from __future__ import annotations

import re

from pyspark.sql import DataFrame

RELEASE_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")

PARTITION_COLUMN = "release_date"


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
    mode: str = "overwrite",
    partition_by: str | None = PARTITION_COLUMN,
    release_date: str | None = None,
) -> None:
    """Write a Delta table, replacing just one day when ``release_date`` is set.

    With ``release_date``: overwrite only that partition, leaving the rest of
    the table alone. Re-running it is a no-op in effect -- the day's files are
    replaced by an identical set, so the result cannot accumulate duplicates.

    Without it: a full overwrite of the whole table.
    """
    writer = df.write.format("delta").mode(mode)

    if partition_by:
        writer = writer.partitionBy(partition_by)

    if release_date is not None:
        if mode != "overwrite":
            raise ValueError("replaceWhere applies to overwrite mode only")
        # Delta verifies the written rows all satisfy this predicate, so a day
        # carrying another day's rows fails loudly instead of silently
        # clobbering a neighbouring partition.
        writer = writer.option("replaceWhere", release_date_predicate(release_date))
    elif mode == "overwrite":
        # only a whole-table rebuild may change the schema
        writer = writer.option("overwriteSchema", "true")

    writer.save(path)
