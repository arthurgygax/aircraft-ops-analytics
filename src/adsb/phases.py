"""Operational phases inferred from ADS-B trajectories.

    silver/flight_observations  ->  gold/flight_phases

One row per contiguous phase of a flight, so a flight has several rows. Phases
are never folded into ``silver.flights``: a flight has many, and flattening
them would force an arbitrary choice of which one.

THESE ARE INFERRED, NOT OPERATIONAL RECORDS
    Every phase here is deduced from radio observations of an aircraft's
    altitude, vertical rate and ground flag. Nothing comes from a flight plan,
    an airline's operational system, or an air traffic control record. A phase
    boundary is where the *evidence* changes, which is close to but not the
    same as where the aircraft actually changed regime.

EVIDENCE ACTUALLY AVAILABLE (measured on the sample, 44.6M points)
    on_ground           99.94% populated -- the strongest signal we have
    ground_speed_kt     98.75%
    vertical_rate_fpm   88.88%
    altitude_ft         88.49% (null whenever on the ground, by construction)

    Sampling is irregular: the median gap between observations within a flight
    is 4 s, the 90th percentile 20 s, the mean 8.1 s.

THE ALGORITHM
    1. Smooth the vertical rate over a time window (not a row window). ADS-B
       vertical rate is noisy and the sampling is irregular, so averaging a
       fixed *number* of rows would cover 40 s for one flight and 5 minutes for
       another. ``RANGE BETWEEN`` over seconds averages the same amount of
       flight time regardless of how densely that aircraft was observed.
    2. Label each observation:
         on the ground   -> taxi_out / taxi_in / taxi, by whether it falls
                            before, after, or between the airborne
                            observations of that flight
         airborne        -> climb / descent when the smoothed rate is outside
                            the level band, otherwise cruise
         no evidence     -> unknown
    3. Collapse consecutive identical labels into intervals.

THRESHOLDS
    Two, both named constants at the top of this module and both adjustable.

    LEVEL_BAND_FPM = 300. Airborne vertical rates in this data have a 25th
    percentile of -640 fpm and a 75th of +512 fpm, so a +/-300 fpm band sits
    well inside genuine climbs and descents while matching the conventional
    tolerance for "level flight". Raising it merges shallow climbs into cruise;
    lowering it splits cruise on noise.

    SMOOTHING_WINDOW_SECONDS = 60 (+/-30 s around each observation). Long
    enough to suppress the sample-to-sample noise in reported vertical rate,
    short enough not to blur a real top-of-climb, which takes minutes.

WHAT IS DELIBERATELY NOT DETECTED
    takeoff, landing and approach. The data supports "on the ground" versus
    "airborne" and the sign of the vertical rate; it does not support a
    defensible takeoff-roll or final-approach boundary without runway geometry
    and airport-relative position. The ground-to-air transition is already
    visible as taxi_out -> climb, and inventing a fixed-duration "takeoff"
    around it would add a threshold with no evidence behind it.

LIMITATIONS
    * Taxi phases require ground observations, which only 43.9% of flights
      have. For the rest the flight simply starts in climb or cruise.
    * A flight tracked through a turnaround (see adsb.flights) yields a
      mid-flight ``taxi`` run between two airborne runs.
    * ``cruise`` means level airborne flight at any altitude. At low altitude
      that may be a level-off or a circuit rather than true cruise.
    * Phases are only as good as the reconstruction they sit on: coverage
      holes, merged turnarounds and midnight clipping all propagate here.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession

from adsb.delta_io import write_delta

DEFAULT_PHASES_URI = os.environ.get(
    "ADSB_PHASES_URI", "s3a://adsb/gold/flight_phases"
)

# Vertical rate within +/- this is treated as level flight. See THRESHOLDS.
LEVEL_BAND_FPM = 300.0

# Total width of the smoothing window, centred on each observation.
SMOOTHING_WINDOW_SECONDS = 60

PHASES = ("taxi_out", "climb", "cruise", "descent", "taxi_in", "taxi", "unknown")

_LABEL_SQL = """
WITH ordered AS (
    SELECT
        flight_id, event_time, observation_seq, release_date,
        altitude_ft, on_ground, ground_speed_kt, vertical_rate_fpm,
        UNIX_TIMESTAMP(event_time) AS event_epoch
    FROM {source}
),
smoothed AS (
    SELECT
        *,
        -- a time window, not a row window: sampling is irregular, so N rows
        -- covers wildly different amounts of flight time between aircraft
        AVG(vertical_rate_fpm) OVER (
            PARTITION BY flight_id ORDER BY event_epoch
            RANGE BETWEEN {half_window} PRECEDING AND {half_window} FOLLOWING
        ) AS smoothed_vertical_rate_fpm,
        -- where the airborne part of this flight begins and ends, so ground
        -- time can be told apart as before it, after it, or between
        MIN(CASE WHEN NOT on_ground THEN observation_seq END)
            OVER (PARTITION BY flight_id) AS first_airborne_seq,
        MAX(CASE WHEN NOT on_ground THEN observation_seq END)
            OVER (PARTITION BY flight_id) AS last_airborne_seq
    FROM ordered
)
SELECT
    *,
    CASE
        WHEN on_ground IS NULL                              THEN 'unknown'
        WHEN on_ground AND first_airborne_seq IS NULL       THEN 'taxi'
        WHEN on_ground AND observation_seq < first_airborne_seq THEN 'taxi_out'
        WHEN on_ground AND observation_seq > last_airborne_seq  THEN 'taxi_in'
        WHEN on_ground                                      THEN 'taxi'
        WHEN smoothed_vertical_rate_fpm IS NULL             THEN 'unknown'
        WHEN smoothed_vertical_rate_fpm >  {band}           THEN 'climb'
        WHEN smoothed_vertical_rate_fpm < -{band}           THEN 'descent'
        ELSE 'cruise'
    END AS phase
FROM smoothed
"""

_INTERVAL_SQL = """
WITH changes AS (
    SELECT
        *,
        CASE WHEN phase = LAG(phase) OVER (
                 PARTITION BY flight_id ORDER BY observation_seq)
             THEN 0 ELSE 1 END AS is_new_run
    FROM {source}
),
runs AS (
    SELECT
        *,
        SUM(is_new_run) OVER (
            PARTITION BY flight_id ORDER BY observation_seq
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS run_no
    FROM changes
),
intervals AS (
    SELECT
        flight_id,
        run_no,
        MAX(phase)                                  AS phase,
        MIN(event_time)                             AS start_time,
        MAX(event_time)                             AS end_time,
        UNIX_TIMESTAMP(MAX(event_time))
            - UNIX_TIMESTAMP(MIN(event_time))       AS duration_seconds,
        COUNT(*)                                    AS n_observations,
        MIN_BY(altitude_ft, event_time)             AS start_altitude_ft,
        MAX_BY(altitude_ft, event_time)             AS end_altitude_ft,
        MIN(altitude_ft)                            AS min_altitude_ft,
        MAX(altitude_ft)                            AS max_altitude_ft,
        ROUND(AVG(ground_speed_kt), 1)              AS avg_ground_speed_kt,
        ROUND(AVG(smoothed_vertical_rate_fpm), 1)   AS avg_vertical_rate_fpm,
        MAX(release_date)                           AS release_date
    FROM runs
    GROUP BY flight_id, run_no
)
SELECT
    flight_id,
    ROW_NUMBER() OVER (PARTITION BY flight_id ORDER BY start_time) AS phase_seq,
    phase, start_time, end_time, duration_seconds, n_observations,
    start_altitude_ft, end_altitude_ft, min_altitude_ft, max_altitude_ft,
    avg_ground_speed_kt, avg_vertical_rate_fpm, release_date
FROM intervals
"""


def label_observations(
    observations: DataFrame,
    level_band_fpm: float = LEVEL_BAND_FPM,
    smoothing_window_seconds: int = SMOOTHING_WINDOW_SECONDS,
) -> DataFrame:
    """Tag every observation with the phase it belongs to.

    Exposed separately from the interval table because it is the interesting
    half to test, and because colouring a trajectory by phase needs it.
    """
    view = "phase_input_observations"
    observations.createOrReplaceTempView(view)
    return observations.sparkSession.sql(
        _LABEL_SQL.format(
            source=view,
            band=level_band_fpm,
            half_window=smoothing_window_seconds // 2,
        )
    )


def to_flight_phases(
    observations: DataFrame,
    level_band_fpm: float = LEVEL_BAND_FPM,
    smoothing_window_seconds: int = SMOOTHING_WINDOW_SECONDS,
) -> DataFrame:
    """Collapse labelled observations into one row per contiguous phase."""
    labelled = label_observations(
        observations, level_band_fpm, smoothing_window_seconds
    )
    view = "labelled_observations"
    labelled.createOrReplaceTempView(view)
    return labelled.sparkSession.sql(_INTERVAL_SQL.format(source=view))


def write_flight_phases(
    df: DataFrame, path: str, mode: str = "overwrite", release_date: str | None = None
) -> None:
    write_delta(df, path, mode=mode, release_date=release_date)


def read_flight_phases(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def main(argv: list[str] | None = None) -> None:
    import argparse

    from pyspark.sql import functions as F

    from adsb.flight_model import (
        DEFAULT_FLIGHT_OBSERVATIONS_URI,
        read_flight_observations,
    )
    from adsb.quality import assert_valid, report, validate_flight_phases
    from adsb.spark_explore import build_session

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--observations", default=DEFAULT_FLIGHT_OBSERVATIONS_URI)
    parser.add_argument("--phases", default=DEFAULT_PHASES_URI)
    parser.add_argument("--level-band-fpm", type=float, default=LEVEL_BAND_FPM)
    parser.add_argument(
        "--smoothing-seconds", type=int, default=SMOOTHING_WINDOW_SECONDS
    )
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])
    parser.add_argument(
        "--release-date",
        default=None,
        help="process one day only, replacing just that partition",
    )
    args = parser.parse_args(argv)

    spark = build_session("adsb-phases")
    try:
        points = read_flight_observations(spark, args.observations)
        if args.release_date:
            points = points.where(F.col("release_date") == F.lit(args.release_date))

        phases = to_flight_phases(
            points, args.level_band_fpm, args.smoothing_seconds
        )

        print(f"Writing {args.phases} (band={args.level_band_fpm} fpm, "
              f"smoothing={args.smoothing_seconds}s)")
        write_flight_phases(phases, args.phases, args.mode, args.release_date)

        table = read_flight_phases(spark, args.phases)
        table.createOrReplaceTempView("phases")
        print(f"phase rows: {table.count():,}")
        print(f"flights covered: {table.select('flight_id').distinct().count():,}")

        print("\n--- schema ---")
        table.printSchema()

        print("--- phase mix ---")
        spark.sql(
            """
            SELECT phase,
                   COUNT(*)                                   AS runs,
                   COUNT(DISTINCT flight_id)                  AS flights,
                   PERCENTILE_APPROX(duration_seconds, 0.5)   AS median_seconds,
                   ROUND(AVG(avg_vertical_rate_fpm))          AS mean_vertical_rate_fpm
            FROM phases GROUP BY phase ORDER BY runs DESC
            """
        ).show(truncate=False)

        print("--- runs per flight (fragmentation) ---")
        spark.sql(
            """
            SELECT PERCENTILE_APPROX(n, array(0.5, 0.9, 0.99)) AS pct_50_90_99,
                   MAX(n) AS worst
            FROM (SELECT flight_id, COUNT(*) AS n FROM phases GROUP BY flight_id)
            """
        ).show(truncate=False)

        results = validate_flight_phases(table)
        print(report("flight_phases", results))
        assert_valid("flight_phases", results)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
