"""Export the Gold layer as Parquet files Power BI can open directly.

    gold Delta tables (MinIO)  ->  data/powerbi/*.parquet

WHY AN EXPORT EXISTS AT ALL
    Power BI Desktop has no connector for Delta on S3-compatible storage. It
    reads Parquet natively, so the shortest honest path from this project to a
    dashboard is a small set of single-file Parquet extracts. Nothing is
    recomputed here: every table is a projection of Gold, and the one reshape
    (movements) already exists inside the pipeline.

WHAT IS EXPORTED, AND WHY EACH ONE
    flights                   107,630  the flight grain: airline, aircraft
                                       type, both airports, both times, hold
                                       rollups. Drives airline and aircraft
                                       analysis.
    movements                  92,951  one row per inferred airport movement.
                                       gold.flights carries departure and
                                       arrival as two columns, which forces
                                       either two relationships to an airport
                                       or awkward DAX. At movement grain,
                                       "arrivals vs departures" and "operations
                                       by hour" are ordinary slicers.
    airport_daily_operations    1,382  pre-aggregated airport x date. Power BI
                                       needs no DAX for the headline KPIs.
    flight_holds                4,393  hold grain, for the distribution of
                                       durations and circuits.
    flight_phases             740,488  phase grain. Optional: none of the
                                       required pages need it, but it is small
                                       enough to include.
    flight_tracks_sample      ~500,000 trajectories, and only for flights with
                                       a detected hold, thinned to one point
                                       per TRACK_SAMPLE_SECONDS.

TRAJECTORIES: WHY A SAMPLE AND NOT THE WHOLE TABLE
    The full point table is 44.6M rows and 1.29 GB, and Power BI's map visuals
    cap out after a few thousand points per render anyway -- a map is only ever
    useful filtered to one flight. Measured alternatives:

        full trajectories                    44,619,824 rows
        flights with a detected hold          2,992,908 rows
        those, one point per 30 s               ~500,000 rows

    The last is imported. It keeps every flight the holding pages care about,
    renders roughly 100-200 points for a selected flight, and leaves
    full-fidelity exploration of any flight to the Streamlit app, which reads
    Delta directly and has no such limit. Positions stay as plain latitude and
    longitude columns; no geometry encoding is involved.

EVERYTHING HERE IS INFERRED
    Flights are reconstructed from radio observations, airport events are
    matched geographically, phases and holds are derived from the trajectory.
    None of it comes from an airline, an airport or ATC. The exported
    ``flights`` table carries a ``data_source`` column saying so, so the
    caveat survives into any report built on it.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DEFAULT_EXPORT_DIR = Path(os.environ.get("ADSB_BI_EXPORT_DIR", "/app/data/powerbi"))

# One position per this many seconds in the trajectory sample. A time bucket,
# not every Nth row: sampling is irregular (median 4 s, mean 8.1 s), so every
# Nth row would thin dense flights and sparse ones by different amounts.
TRACK_SAMPLE_SECONDS = 30

DATA_SOURCE_LABEL = "ADS-B derived (adsb.lol) - not official airline or ATC data"

_MOVEMENTS_SQL = """
SELECT
    segment_id              AS flight_id,
    movement_type,
    event_time              AS movement_time,
    ident                   AS airport_ident,
    iata_code               AS airport_iata,
    airport_name,
    iso_country,
    ROUND(distance_km, 2)   AS distance_km,
    release_date
FROM {movements}
"""

_TRACK_SAMPLE_SQL = """
WITH bucketed AS (
    SELECT
        p.flight_id, p.event_time, p.observation_seq,
        p.latitude, p.longitude, p.altitude_ft,
        p.ground_speed_kt, p.track_deg, p.release_date,
        ROW_NUMBER() OVER (
            PARTITION BY p.flight_id,
                         FLOOR(UNIX_TIMESTAMP(p.event_time) / {seconds})
            ORDER BY p.observation_seq
        ) AS in_bucket
    FROM {points} p
    -- only the flights the holding pages are about; everything else is
    -- explored in the Streamlit app, which has no point budget
    WHERE p.flight_id IN (SELECT DISTINCT flight_id FROM {holds})
)
SELECT flight_id, event_time, observation_seq, latitude, longitude,
       altitude_ft, ground_speed_kt, track_deg, release_date
FROM bucketed WHERE in_bucket = 1
"""


def to_movements(airport_movements: DataFrame) -> DataFrame:
    """One row per inferred movement, narrowed to what a report needs."""
    airport_movements.createOrReplaceTempView("bi_movements_input")
    return airport_movements.sparkSession.sql(
        _MOVEMENTS_SQL.format(movements="bi_movements_input")
    )


def to_track_sample(
    points: DataFrame, holds: DataFrame, seconds: int = TRACK_SAMPLE_SECONDS
) -> DataFrame:
    """Thinned trajectories for the flights that have a detected hold."""
    spark = points.sparkSession
    points.createOrReplaceTempView("bi_points_input")
    holds.createOrReplaceTempView("bi_holds_input")
    return spark.sql(
        _TRACK_SAMPLE_SQL.format(
            points="bi_points_input", holds="bi_holds_input", seconds=seconds
        )
    )


def write_parquet(frame: DataFrame, directory: Path, name: str) -> tuple[str, int, int]:
    """Write one table as a single Parquet file Power BI can open by path.

    Spark writes a directory of part files; Power BI's Parquet connector wants
    a file. Coalescing to one partition and promoting that part file keeps the
    connection instructions to "open this path", without pulling pandas into
    the pipeline image just to serialize an extract.
    """
    directory.mkdir(parents=True, exist_ok=True)
    rows = frame.count()

    staging = directory / f"_{name}_staging"
    frame.coalesce(1).write.mode("overwrite").parquet(f"file://{staging}")

    part = next(staging.glob("part-*.parquet"))
    target = directory / f"{name}.parquet"
    target.unlink(missing_ok=True)
    part.replace(target)
    shutil.rmtree(staging)

    return name, rows, target.stat().st_size


def main(argv: list[str] | None = None) -> None:
    import argparse

    from adsb.airports import DEFAULT_AIRPORTS_URI, airport_movements, read_airports
    from adsb.flight_model import (
        DEFAULT_FLIGHT_OBSERVATIONS_URI,
        read_flight_observations,
    )
    from adsb.flights import DEFAULT_FLIGHTS_URI, read_flight_segments
    from adsb.gold import (
        DEFAULT_GOLD_FLIGHTS_URI,
        DEFAULT_GOLD_URI,
        read_airport_metrics,
        read_gold_flights,
    )
    from adsb.holds import DEFAULT_HOLDS_URI, read_flight_holds
    from adsb.phases import DEFAULT_PHASES_URI, read_flight_phases
    from adsb.spark_explore import build_session

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=DEFAULT_EXPORT_DIR)
    parser.add_argument(
        "--track-seconds", type=int, default=TRACK_SAMPLE_SECONDS,
        help="one trajectory point per this many seconds",
    )
    parser.add_argument(
        "--skip-tracks", action="store_true",
        help="omit the trajectory sample (the slowest table to export)",
    )
    args = parser.parse_args(argv)

    spark = build_session("adsb-bi-export")
    try:
        flights = read_gold_flights(spark, DEFAULT_GOLD_FLIGHTS_URI).withColumn(
            "data_source", F.lit(DATA_SOURCE_LABEL)
        )
        holds = read_flight_holds(spark, DEFAULT_HOLDS_URI)
        movements = to_movements(
            airport_movements(
                read_flight_segments(spark, DEFAULT_FLIGHTS_URI),
                read_airports(spark, DEFAULT_AIRPORTS_URI),
            )
        )

        exports = [
            ("flights", flights),
            ("movements", movements),
            ("airport_daily_operations", read_airport_metrics(spark, DEFAULT_GOLD_URI)),
            ("flight_holds", holds),
            ("flight_phases", read_flight_phases(spark, DEFAULT_PHASES_URI)),
        ]
        if not args.skip_tracks:
            exports.append((
                "flight_tracks_sample",
                to_track_sample(
                    read_flight_observations(spark, DEFAULT_FLIGHT_OBSERVATIONS_URI),
                    holds,
                    args.track_seconds,
                ),
            ))

        print(f"Exporting to {args.out}")
        total_bytes = 0
        for name, frame in exports:
            table, rows, size = write_parquet(frame, args.out, name)
            total_bytes += size
            print(f"  {table:<26} {rows:>9,} rows  {size / 1e6:7.1f} MB")
        print(f"  {'total':<26} {'':>9}       {total_bytes / 1e6:7.1f} MB")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
