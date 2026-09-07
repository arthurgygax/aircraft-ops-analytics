"""Run the whole pipeline in one Spark session.

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

WHAT CHANGED, AND WHY THERE IS NO LONGER A CACHED SEGMENTATION
    The pipeline used to materialize the 44.6M-row point grain three times --
    Bronze, Silver, flight_observations -- and cache the segmentation to disk
    so the flight table and the trajectory table could share one pass. Now
    there is one point-grain table: the segmentation runs once inside the
    decode, is written once, and everything downstream reads it back. The
    ~21 GB disk cache that shared pass needed is simply gone.

    The one thing still held in memory is the flight aggregate, at 107,630
    rows: movements and the flight table both consume it, and caching a table
    that small is cheaper than scanning 44.6M observations twice.

ORDER
    Holds need a flight's inferred arrival airport, and the flight table wants
    hold rollups on it. That is not a cycle: the airport columns come from the
    movement pivot, which is ready before either. So the order is movements,
    then phases and holds, then the flight table, then the airport rollup.
"""

from __future__ import annotations

import time
from contextlib import contextmanager

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

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
from adsb.ingest import release_date as release_date_of
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


@contextmanager
def stage(name: str):
    print(f"--- {name} ---", flush=True)
    started = time.time()
    yield
    elapsed = time.time() - started
    _timings.append((name, elapsed))
    print(f"    {name} finished in {elapsed:6.1f}s", flush=True)


def _validated(name: str, table: DataFrame, validator) -> DataFrame:
    """Report a table's size and fail the run if it violates its own rules."""
    print(f"    {name}: {table.count():,} rows", flush=True)
    assert_valid(name, validator(table))
    return table


def _for_day(frame: DataFrame, release_date: str | None) -> DataFrame:
    if release_date is None:
        return frame
    return frame.where(F.col("release_date") == F.lit(release_date))


def main(argv: list[str] | None = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--raw", default=DEFAULT_RAW_URI)
    parser.add_argument("--airports", default=DEFAULT_AIRPORTS_URI)
    parser.add_argument("--gap-seconds", type=int, default=GAP_SECONDS)
    parser.add_argument(
        "--tag",
        default=None,
        help="the release to process, e.g. v2025.12.30-planes-readsb-prod-0",
    )
    parser.add_argument(
        "--release-date",
        default=None,
        help="the day to replace; taken from --tag when that is given",
    )
    parser.add_argument(
        "--full-rebuild",
        action="store_true",
        help="replace every day in every table, not just this release's",
    )
    parser.add_argument(
        "--no-vacuum",
        action="store_true",
        help="skip reclaiming tombstoned files at the end of the run",
    )
    args = parser.parse_args(argv)

    raw_uri = args.raw if not args.tag else f"{args.raw.rstrip('/')}/{args.tag}"
    day = args.release_date or (release_date_of(args.tag) if args.tag else None)
    if day is None and not args.full_rebuild:
        parser.error(
            "pass --tag (or --release-date) to replace one day, "
            "or --full-rebuild to replace the whole table on purpose"
        )
    if args.full_rebuild:
        day = None
    started = time.time()

    spark = build_session("adsb-pipeline")
    try:
        airports = read_airports(spark, args.airports)
        aircraft = read_aircraft(spark, raw_uri)

        with stage("observations"):
            write_observations(
                to_observations(aircraft, args.gap_seconds),
                DEFAULT_OBSERVATIONS_URI,
                release_date=day,
                full_rebuild=args.full_rebuild,
            )
            observations = _validated(
                "observations",
                _for_day(read_observations(spark, DEFAULT_OBSERVATIONS_URI), day),
                validate_observations,
            )
            points, kept = collisions_resolved(aircraft, observations)
            print(
                f"    same-timestamp receptions collapsed: {points - kept:,}"
                f" of {points:,} trace points"
                f" ({100 * (points - kept) / max(points, 1):.4f}%)",
                flush=True,
            )

        # 107,630 rows, read twice: once to match airports, once to build the
        # flight table. Cheaper to hold than to re-aggregate 44.6M rows.
        base = aggregate_flights(observations).cache()

        with stage("movements"):
            write_movements(
                airport_movements(base, airports),
                DEFAULT_MOVEMENTS_URI,
                release_date=day,
                full_rebuild=args.full_rebuild,
            )
            movements = _validated(
                "movements",
                _for_day(read_movements(spark, DEFAULT_MOVEMENTS_URI), day),
                validate_movements,
            )
        pivot = movement_pivot(movements).cache()

        with stage("phases"):
            write_flight_phases(
                to_flight_phases(observations),
                DEFAULT_PHASES_URI,
                release_date=day,
                full_rebuild=args.full_rebuild,
            )
            _validated(
                "flight_phases",
                _for_day(read_flight_phases(spark, DEFAULT_PHASES_URI), day),
                validate_flight_phases,
            )

        with stage("holds"):
            write_flight_holds(
                to_flight_holds(observations, pivot, airports),
                DEFAULT_HOLDS_URI,
                release_date=day,
                full_rebuild=args.full_rebuild,
            )
            holds = _validated(
                "flight_holds",
                _for_day(read_flight_holds(spark, DEFAULT_HOLDS_URI), day),
                validate_flight_holds,
            )

        with stage("flights"):
            write_flights(
                to_flights(base, pivot, holds),
                DEFAULT_FLIGHTS_URI,
                release_date=day,
                full_rebuild=args.full_rebuild,
            )
            flights = _validated(
                "flights",
                _for_day(read_flights(spark, DEFAULT_FLIGHTS_URI), day),
                validate_flights,
            )
            assert_valid(
                "flights", [check_observations_conserved(observations, flights)]
            )

        with stage("airport operations"):
            write_airport_operations(
                to_airport_operations(movements, holds),
                DEFAULT_AIRPORT_OPERATIONS_URI,
                release_date=day,
                full_rebuild=args.full_rebuild,
            )
            _validated(
                "airport_daily_operations",
                _for_day(
                    read_airport_operations(spark, DEFAULT_AIRPORT_OPERATIONS_URI), day
                ),
                validate_airport_operations,
            )

        base.unpersist()
        pivot.unpersist()

        if not args.no_vacuum:
            # reclaiming is part of writing: every overwrite above tombstoned
            # the files it replaced, and nothing else removes them
            with stage("vacuum"):
                for name, files, size in vacuum_tables(spark):
                    print(f"    {name:<26} {files:>5} files  {size / 1e9:5.2f} GB")
    finally:
        spark.stop()

    total = time.time() - started
    print("\n--- timings ---")
    for name, elapsed in _timings:
        print(f"  {name:<44} {elapsed:6.1f}s  {100 * elapsed / total:4.1f}%")
    print(f"  {'TOTAL (one session)':<44} {total:6.1f}s")


if __name__ == "__main__":
    main()
