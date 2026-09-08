"""Run the whole pipeline in one Spark session, over the study period.

    raw -> observations -> flights + movements + phases + holds + airport ops

Each stage is still a standalone command (``python -m adsb.observations`` and
so on), which is what reprocessing a single day uses. This driver exists
because running them as separate processes wastes time in two measured ways:

* **Startup.** A stage costs about 55 seconds before it reads a row: ~9 s to
  create a container from the 1.8 GB image, ~14 s more for Compose's dependency
  check and volume mounts, and ~32 s for the JVM and Spark session (the image
  carries 598 MB of jars).

* **Reading the same tables repeatedly.** Separate processes each re-open every
  Delta table they touch. One session keeps the catalog and the S3A clients
  warm across all of them.

The same argument applies across days, so the seven releases of the study
period are a loop inside one session rather than seven invocations. Each day
still writes only its own partition, so a day can be re-run on its own and the
result is identical either way.

WHAT THE SCOPE DOES TO THE SHAPE OF THIS
    ``adsb.scope`` defines the study period and the study airports. Two filters
    come out of it and both are applied here, because this is the only module
    that sees the whole flow:

    1. Before anything is decoded, aircraft that never came near a study
       airport are dropped -- a row-level test on the raw trace array. This is
       what makes the run affordable: 2.25% of the day's aircraft survive it.
    2. After airport matching, flights that did not actually depart from or
       arrive at a study airport are dropped, and every table is restricted to
       the flights that did.

    Between the two, ``candidate`` holds the decoded observations of every
    aircraft that came near -- more than the scope, because the flights that
    define the scope do not exist yet. It is persisted rather than recomputed:
    four things read it, and at ~2M rows it is a fraction of the 44.6M-row
    table this pipeline used to carry through the same steps.

WHERE THE TIME GOES, MEASURED
    86% of a day is the raw read: parsing 1.3 GB of gzipped JSON to find the
    2.25% of aircraft that came near a study airport. Every stage after the
    pre-filter runs in 10-70 s. The scope removed nearly all of the processing
    and none of the reading, because whether an aircraft came near Zurich is
    only knowable once its trace has been parsed.

    Which is why the one obvious-looking further optimization -- caching the
    filtered aircraft so the release is parsed once instead of twice -- was
    measured before being kept, and then removed. See ``process_day``.

ORDER
    Holds need a flight's inferred arrival airport, and the flight table wants
    hold rollups on it. That is not a cycle: the airport columns come from the
    movement pivot, which is ready before either. So the order is movements,
    then phases and holds, then the flight table, then the airport rollup.
"""

from __future__ import annotations

import time
from contextlib import contextmanager

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from adsb import scope as scope_module
from adsb.airports import (
    DEFAULT_AIRPORT_OPERATIONS_URI,
    DEFAULT_AIRPORTS_URI,
    DEFAULT_MOVEMENTS_URI,
    airport_movements,
    read_airport_operations,
    read_airports,
    read_movements,
    to_airport_operations,
    write_airport_operations,
    write_movements,
)
from adsb.delta_io import vacuum_tables
from adsb.flights import (
    DEFAULT_FLIGHTS_URI,
    aggregate_flights,
    movement_pivot,
    read_flights,
    to_flights,
    write_flights,
)
from adsb.holds import (
    DEFAULT_HOLDS_URI,
    read_flight_holds,
    to_flight_holds,
    write_flight_holds,
)
from adsb.observations import (
    DEFAULT_OBSERVATIONS_URI,
    GAP_SECONDS,
    read_observations,
    to_observations,
    write_observations,
)
from adsb.phases import (
    DEFAULT_PHASES_URI,
    read_flight_phases,
    to_flight_phases,
    write_flight_phases,
)
from adsb.quality import (
    assert_valid,
    check_observations_conserved,
    collisions_resolved,
    validate_airport_operations,
    validate_flight_holds,
    validate_flight_phases,
    validate_flights,
    validate_movements,
    validate_observations,
)
from adsb.spark_explore import DEFAULT_RAW_URI, build_session, read_aircraft

_timings: list[tuple[str, float]] = []
_volumes: list[dict] = []


@contextmanager
def stage(name: str):
    print(f"--- {name} ---", flush=True)
    started = time.time()
    yield
    elapsed = time.time() - started
    _timings.append((name, elapsed))
    print(f"    {name} finished in {elapsed:6.1f}s", flush=True)


def _validated(
    name: str, table: DataFrame, validator, counts: dict | None = None
) -> DataFrame:
    """Report a table's size and fail the run if it violates its own rules.

    The count goes into ``counts`` rather than being taken again later: the
    run's volume summary is the same number this check already measured.
    """
    rows = table.count()
    print(f"    {name}: {rows:,} rows", flush=True)
    if counts is not None:
        counts[name] = rows
    assert_valid(name, validator(table))
    return table


def _for_day(frame: DataFrame, release_date: str | None) -> DataFrame:
    if release_date is None:
        return frame
    return frame.where(F.col("release_date") == F.lit(release_date))


def process_day(
    spark,
    airports: DataFrame,
    boxes,
    scope: scope_module.Scope,
    tag: str,
    day: str,
    raw_root: str,
    gap_seconds: int,
    full_rebuild: bool,
) -> dict:
    """Build every table for one release. Returns the day's volumes."""
    counts: dict[str, int] = {}
    aircraft = read_aircraft(spark, f"{raw_root.rstrip('/')}/{tag}")
    # NOT persisted, though it is read twice -- once to decode, once to count
    # the raw trace points the dedup collapsed. Caching it looks obviously
    # right, because the read is 86% of the day, and it is not: measured on
    # 2025-12-25, persist 802.2 s against no-persist 752.4 s, with the ordering
    # chosen to favour the cached variant. Holding 1.9M points of nested string
    # arrays costs about what re-reading them costs. Do not re-add it without
    # measuring again.
    near = scope_module.near_scope(aircraft, boxes)

    with stage(f"{day} scope selection"):
        # This one is persisted, and the difference from `near` above is the
        # number of consumers: four read it -- the count, the flight
        # aggregate, the collision measure and the trajectory write -- and
        # each would otherwise re-read and re-decode the release.
        candidate = to_observations(near, gap_seconds).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        n_candidate = candidate.count()
        # small enough to hold: a day of near-Zurich/Dusseldorf traffic is a
        # few thousand flights and at most two movements each
        base_all = aggregate_flights(candidate).cache()
        movements_all = airport_movements(base_all, airports).cache()
        ids = scope_module.scope_flight_ids(movements_all, scope).cache()

        n_scope = ids.count()
        print(f"    candidate observations: {n_candidate:,}", flush=True)
        print(f"    candidate flights:      {base_all.count():,}", flush=True)
        print(f"    in-scope flights:       {n_scope:,}", flush=True)
        # measured against the aircraft that were decoded, so it stays a
        # statement about deduplication rather than about the scope filter
        points, kept = collisions_resolved(near, candidate)
        print(
            f"    same-timestamp receptions collapsed: {points - kept:,}"
            f" of {points:,} trace points"
            f" ({100 * (points - kept) / max(points, 1):.4f}%)",
            flush=True,
        )

    with stage(f"{day} observations"):
        write_observations(
            scope_module.restrict_to_flights(candidate, ids),
            DEFAULT_OBSERVATIONS_URI,
            release_date=None if full_rebuild else day,
            full_rebuild=full_rebuild,
        )
        observations = _validated(
            "observations",
            _for_day(read_observations(spark, DEFAULT_OBSERVATIONS_URI), day),
            validate_observations,
            counts,
        )

    with stage(f"{day} movements"):
        write_movements(
            scope_module.at_study_airports(movements_all, scope),
            DEFAULT_MOVEMENTS_URI,
            release_date=None if full_rebuild else day,
            full_rebuild=full_rebuild,
        )
        movements = _validated(
            "movements",
            _for_day(read_movements(spark, DEFAULT_MOVEMENTS_URI), day),
            validate_movements,
            counts,
        )
    # both ends of an in-scope flight, not just the study-airport end: this is
    # what puts "where did this Zurich departure go" on the flight table
    pivot = movement_pivot(
        scope_module.restrict_to_flights(movements_all, ids)
    ).cache()

    with stage(f"{day} phases"):
        write_flight_phases(
            to_flight_phases(observations),
            DEFAULT_PHASES_URI,
            release_date=None if full_rebuild else day,
            full_rebuild=full_rebuild,
        )
        _validated(
            "flight_phases",
            _for_day(read_flight_phases(spark, DEFAULT_PHASES_URI), day),
            validate_flight_phases,
            counts,
        )

    with stage(f"{day} holds"):
        write_flight_holds(
            to_flight_holds(observations, pivot, airports),
            DEFAULT_HOLDS_URI,
            release_date=None if full_rebuild else day,
            full_rebuild=full_rebuild,
        )
        holds = _validated(
            "flight_holds",
            _for_day(read_flight_holds(spark, DEFAULT_HOLDS_URI), day),
            validate_flight_holds,
            counts,
        )

    with stage(f"{day} flights"):
        write_flights(
            to_flights(scope_module.restrict_to_flights(base_all, ids), pivot, holds),
            DEFAULT_FLIGHTS_URI,
            release_date=None if full_rebuild else day,
            full_rebuild=full_rebuild,
        )
        flights = _validated(
            "flights",
            _for_day(read_flights(spark, DEFAULT_FLIGHTS_URI), day),
            validate_flights,
            counts,
        )
        assert_valid(
            "flights", [check_observations_conserved(observations, flights)]
        )

    with stage(f"{day} airport operations"):
        write_airport_operations(
            to_airport_operations(movements, holds),
            DEFAULT_AIRPORT_OPERATIONS_URI,
            release_date=None if full_rebuild else day,
            full_rebuild=full_rebuild,
        )
        _validated(
            "airport_daily_operations",
            _for_day(
                read_airport_operations(spark, DEFAULT_AIRPORT_OPERATIONS_URI), day
            ),
            validate_airport_operations,
            counts,
        )

    volumes = {
        "day": day,
        "candidate_observations": n_candidate,
        "flights": counts["flights"],
        "observations": counts["observations"],
        "movements": counts["movements"],
        "holds": counts["flight_holds"],
    }
    for frame in (candidate, base_all, movements_all, ids, pivot):
        frame.unpersist()
    return volumes


def main(argv: list[str] | None = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--raw", default=DEFAULT_RAW_URI)
    parser.add_argument("--airports", default=DEFAULT_AIRPORTS_URI)
    parser.add_argument("--gap-seconds", type=int, default=GAP_SECONDS)
    scope_module.add_arguments(parser)
    parser.add_argument(
        "--tag",
        action="append",
        dest="tags",
        default=None,
        help="process these releases instead of every day in the scope",
    )
    parser.add_argument(
        "--full-rebuild",
        action="store_true",
        help="replace every table wholesale, then write the period into it",
    )
    parser.add_argument(
        "--no-vacuum",
        action="store_true",
        help="skip reclaiming tombstoned files at the end of the run",
    )
    args = parser.parse_args(argv)

    scope = scope_module.from_args(args)
    releases = scope.releases
    if args.tags:
        from adsb.ingest import release_date as release_date_of

        releases = tuple((tag, release_date_of(tag)) for tag in args.tags)

    started = time.time()
    spark = build_session("adsb-pipeline")
    try:
        airports = read_airports(spark, args.airports)
        boxes = scope_module.airport_boxes(airports, scope)
        print(f"Scope: {scope.describe()}")
        for box in boxes:
            print(
                f"  {box.ident} pre-filter box "
                f"lat [{box.min_lat:.3f}, {box.max_lat:.3f}] "
                f"lon [{box.min_lon:.3f}, {box.max_lon:.3f}]"
            )
        print(f"Releases: {', '.join(tag for tag, _ in releases)}\n")

        for index, (tag, day) in enumerate(releases):
            # A full rebuild replaces the table once, on the first day; the
            # rest of the period is written a partition at a time on top of it.
            # Rebuilding on every day would leave only the last one.
            _volumes.append(
                process_day(
                    spark,
                    airports,
                    boxes,
                    scope,
                    tag,
                    day,
                    args.raw,
                    args.gap_seconds,
                    full_rebuild=args.full_rebuild and index == 0,
                )
            )

        if not args.no_vacuum:
            # reclaiming is part of writing: every overwrite above tombstoned
            # the files it replaced, and nothing else removes them
            with stage("vacuum"):
                for name, files, size in vacuum_tables(spark):
                    print(f"    {name:<26} {files:>5} files  {size / 1e9:5.2f} GB")
    finally:
        spark.stop()

    total = time.time() - started
    print("\n--- volumes ---")
    header = ("day", "candidate obs", "flights", "trajectory obs", "movements", "holds")
    print("  {:<12}{:>15}{:>10}{:>16}{:>11}{:>8}".format(*header))
    for row in _volumes:
        print(
            "  {day:<12}{candidate_observations:>15,}{flights:>10,}"
            "{observations:>16,}{movements:>11,}{holds:>8,}".format(**row)
        )
    if len(_volumes) > 1:
        totals = {
            key: sum(row[key] for row in _volumes)
            for key in ("candidate_observations", "flights", "observations",
                        "movements", "holds")
        }
        print(
            "  {:<12}{candidate_observations:>15,}{flights:>10,}"
            "{observations:>16,}{movements:>11,}{holds:>8,}".format(
                "TOTAL", **totals
            )
        )

    print("\n--- timings ---")
    for name, elapsed in _timings:
        print(f"  {name:<44} {elapsed:6.1f}s  {100 * elapsed / total:4.1f}%")
    print(f"  {'TOTAL (one session)':<44} {total:6.1f}s")


if __name__ == "__main__":
    main()
