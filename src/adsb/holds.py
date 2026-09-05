"""Holding patterns detected in ADS-B trajectories.

    silver/flight_observations + silver/flights  ->  gold/flight_holds

WHAT THIS DETECTS, AND WHAT IT DOES NOT
    This finds *observed* circling: stretches where an aircraft turned through
    at least a full circle while staying inside a small area. That geometry is
    what a holding pattern looks like from the outside.

    It does not establish that the aircraft was instructed to hold, was
    published-procedure holding, or was delayed. No flight plan, clearance or
    ATC record is involved -- only positions and headings. Every column here
    describes an ADS-B-derived observation, which is why the table talks about
    detected holds rather than about holding instructions.

EVIDENCE (measured on the sample)
    track_deg   98.38% of airborne observations
    altitude_ft 100% of airborne observations
    Turning is normally gentle: per-step turn has a median of 0.1 deg and a
    90th percentile of 4.3 deg, and turn rate a median of 0.02 deg/s. Sustained
    turning therefore stands out clearly from ordinary flight.

    A rolling six-minute signed turn reaching a full circle occurs somewhere in
    7,128 of 97,394 flights (7.3%); two circles in 1,609 (1.7%). So the base
    signal is selective without any further filtering.

THE ALGORITHM
    1. Per airborne observation, the signed heading change from the previous
       one, wrapped into (-180, 180] so 359 -> 1 degrees reads as +2 and not
       -358. Changes spanning a gap longer than MAX_STEP_GAP_SECONDS are
       ignored: across a coverage hole the heading difference says nothing
       about how the aircraft got there.
    2. Sum those changes over a window centred on each observation. Centred,
       not trailing: a trailing window only marks a point once a whole circle
       has already accumulated behind it, which clips the start of every hold
       and loses short ones entirely. Signed, so a left turn followed by an
       equal right turn cancels -- an S-bend is not circling.
    3. Mark observations where that sum reaches a full circle, and collapse
       consecutive marks into candidate intervals. ``turn_degrees`` reports the
       strongest window sum inside an interval rather than the interval's own
       net heading change, because the window that flagged a point extends
       beyond the flagged stretch.
    4. Keep a candidate only if it is also sustained, spatially confined and
       roughly level.

    Geometry stays deliberately primitive: a bounding box converted to
    kilometres with a flat-earth approximation, which is accurate to well under
    a percent over the tens of kilometres a hold spans. No geospatial library
    is involved, and a reviewer can check the arithmetic by hand.

THRESHOLDS -- all named constants, all adjustable from the command line
    TURN_WINDOW_SECONDS 360   Total width, centred on each observation. One
                              standard circuit takes about four minutes at low
                              level and up to six higher up, so six minutes
                              covers a circuit at any altitude.
    MIN_TURN_DEGREES    360   A complete circle. A base-to-final turn is about
                              90 degrees and a procedure turn 180-270, so this
                              already excludes ordinary approach manoeuvring
                              without needing a separate rule for it.
    MIN_DURATION_SECONDS 240  One full circuit at low level; shorter circling
                              is more likely a tight orbit than a hold.
    MAX_SPAN_KM          25   A racetrack at typical holding speed spans
                              roughly 15-20 km, so this admits real patterns
                              while rejecting a wide sweeping turn that happens
                              to total 360 degrees.
    MAX_ALTITUDE_RANGE_FT 4000 Holds are flown level, or descend step by step
                              in a stack. A spiral descent is not a hold.

KNOWN LIMITATIONS -- read before trusting a row
    * A circle is a circle. Aerial survey, photography, training circuits and
      police or medical orbits all produce the same geometry and will appear
      here. Nothing in ADS-B distinguishes their intent from a hold's.
    * Association with an airport is the *flight's own inferred arrival
      airport*, not a geometric search for the nearest airfield. It is null
      whenever that inference failed, which is often (arrival airports resolve
      for about 42% of flights). ``distance_to_arrival_airport_km`` says how
      far the circling was from it, so an en-route hold can be told from a
      terminal-area one.
    * A hold flown so far from coverage that the turn is sampled sparsely may
      be missed entirely; ``max_sample_gap_seconds`` exposes that per row.
    * Nothing here separates a hold from a go-around that circles back, or
      from vectoring that happens to close a full circle.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession

from adsb.delta_io import write_delta

DEFAULT_HOLDS_URI = os.environ.get("ADSB_HOLDS_URI", "s3a://adsb/gold/flight_holds")

TURN_WINDOW_SECONDS = 360
MIN_TURN_DEGREES = 360.0
MIN_DURATION_SECONDS = 240
MAX_SPAN_KM = 25.0
MAX_ALTITUDE_RANGE_FT = 4000.0

# A heading change measured across a longer gap than this tells us nothing
# about the path actually flown between the two observations.
MAX_STEP_GAP_SECONDS = 60

KM_PER_DEGREE_LATITUDE = 111.32

_CANDIDATE_SQL = """
WITH stepped AS (
    SELECT
        flight_id, event_time, observation_seq, release_date,
        latitude, longitude, altitude_ft,
        UNIX_TIMESTAMP(event_time) AS event_epoch,
        UNIX_TIMESTAMP(event_time) - UNIX_TIMESTAMP(
            LAG(event_time) OVER (PARTITION BY flight_id ORDER BY observation_seq)
        ) AS step_seconds,
        -- wrapped into (-180, 180]: 359 -> 1 degrees is +2, not -358
        PMOD(
            track_deg - LAG(track_deg) OVER (
                PARTITION BY flight_id ORDER BY observation_seq
            ) + 180, 360
        ) - 180 AS step_turn_degrees
    FROM {source}
    WHERE NOT on_ground AND track_deg IS NOT NULL
),
usable AS (
    SELECT
        *,
        CASE WHEN step_seconds BETWEEN 1 AND {max_step_gap}
             THEN step_turn_degrees ELSE 0 END AS counted_turn
    FROM stepped
),
windowed AS (
    SELECT
        *,
        -- signed, so a left turn cancelled by a right turn is not circling
        SUM(counted_turn) OVER (
            PARTITION BY flight_id ORDER BY event_epoch
            RANGE BETWEEN {half_window} PRECEDING AND {half_window} FOLLOWING
        ) AS turn_in_window_degrees
    FROM usable
),
marked AS (
    SELECT *, CASE WHEN ABS(turn_in_window_degrees) >= {min_turn} THEN 1 ELSE 0 END AS circling
    FROM windowed
),
runs AS (
    SELECT
        *,
        SUM(is_new_run) OVER (
            PARTITION BY flight_id ORDER BY observation_seq
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS run_no
    FROM (
        SELECT *, CASE WHEN circling = LAG(circling) OVER (
                     PARTITION BY flight_id ORDER BY observation_seq)
                 THEN 0 ELSE 1 END AS is_new_run
        FROM marked
    )
)
SELECT
    flight_id,
    MIN(event_time)                                     AS hold_start,
    MAX(event_time)                                     AS hold_end,
    UNIX_TIMESTAMP(MAX(event_time))
        - UNIX_TIMESTAMP(MIN(event_time))               AS duration_seconds,
    COUNT(*)                                            AS n_observations,
    ROUND(AVG(latitude), 5)                             AS centroid_latitude,
    ROUND(AVG(longitude), 5)                            AS centroid_longitude,
    -- bounding box diagonal, flat-earth: exact enough over tens of kilometres
    ROUND(SQRT(
        POW((MAX(latitude) - MIN(latitude)) * {km_per_deg}, 2)
        + POW((MAX(longitude) - MIN(longitude)) * {km_per_deg}
              * COS(RADIANS(AVG(latitude))), 2)
    ), 2)                                               AS span_km,
    MIN(altitude_ft)                                    AS min_altitude_ft,
    MAX(altitude_ft)                                    AS max_altitude_ft,
    ROUND(AVG(altitude_ft))                             AS mean_altitude_ft,
    -- the strongest circling seen inside this stretch, which is what triggered
    -- the detection. Summing the run's own steps would understate it: the
    -- window that flagged a point reaches beyond the flagged run itself, so a
    -- slow wide circle can produce a run whose own turn totals under 360.
    -- Signed, so the sign still gives the direction of turn.
    ROUND(MAX_BY(turn_in_window_degrees, ABS(turn_in_window_degrees)), 1)
                                                        AS turn_degrees,
    ROUND(MAX(ABS(turn_in_window_degrees)) / 360.0, 1)  AS circuits,
    MAX(step_seconds)                                   AS max_sample_gap_seconds,
    MAX(release_date)                                   AS release_date
FROM runs
WHERE circling = 1
GROUP BY flight_id, run_no
"""

_HOLDS_SQL = """
SELECT
    c.flight_id,
    ROW_NUMBER() OVER (PARTITION BY c.flight_id ORDER BY c.hold_start) AS hold_seq,
    c.hold_start, c.hold_end, c.duration_seconds, c.n_observations,
    c.centroid_latitude, c.centroid_longitude, c.span_km,
    c.min_altitude_ft, c.max_altitude_ft, c.mean_altitude_ft,
    c.turn_degrees, c.circuits, c.max_sample_gap_seconds,
    -- the flight's own inferred arrival airport, not a nearest-airport search
    f.arrival_airport_ident, f.arrival_airport_iata,
    CASE WHEN a.latitude_deg IS NULL THEN NULL ELSE ROUND(
        6371 * 2 * ASIN(SQRT(
            POW(SIN(RADIANS(a.latitude_deg - c.centroid_latitude) / 2), 2)
            + COS(RADIANS(c.centroid_latitude)) * COS(RADIANS(a.latitude_deg))
            * POW(SIN(RADIANS(a.longitude_deg - c.centroid_longitude) / 2), 2)
        )), 1) END                                  AS distance_to_arrival_airport_km,
    c.release_date
FROM {candidates} c
LEFT JOIN {flights} f  ON c.flight_id = f.flight_id
LEFT JOIN {airports} a ON f.arrival_airport_ident = a.ident
"""


def detect_hold_candidates(
    observations: DataFrame,
    turn_window_seconds: int = TURN_WINDOW_SECONDS,
    min_turn_degrees: float = MIN_TURN_DEGREES,
    max_step_gap_seconds: int = MAX_STEP_GAP_SECONDS,
) -> DataFrame:
    """Stretches of sustained circling, before the conservative filters."""
    view = "hold_input_observations"
    observations.createOrReplaceTempView(view)
    return observations.sparkSession.sql(
        _CANDIDATE_SQL.format(
            source=view,
            half_window=turn_window_seconds // 2,
            min_turn=min_turn_degrees,
            max_step_gap=max_step_gap_seconds,
            km_per_deg=KM_PER_DEGREE_LATITUDE,
        )
    )


def to_flight_holds(
    observations: DataFrame,
    flights: DataFrame,
    airports: DataFrame,
    turn_window_seconds: int = TURN_WINDOW_SECONDS,
    min_turn_degrees: float = MIN_TURN_DEGREES,
    min_duration_seconds: int = MIN_DURATION_SECONDS,
    max_span_km: float = MAX_SPAN_KM,
    max_altitude_range_ft: float = MAX_ALTITUDE_RANGE_FT,
) -> DataFrame:
    """Detected holds: circling that is also sustained, confined and level."""
    spark = observations.sparkSession
    candidates = detect_hold_candidates(
        observations, turn_window_seconds, min_turn_degrees
    ).where(
        f"duration_seconds >= {min_duration_seconds}"
        f" AND span_km <= {max_span_km}"
        f" AND max_altitude_ft - min_altitude_ft <= {max_altitude_range_ft}"
    )
    candidates.createOrReplaceTempView("hold_candidates")
    flights.createOrReplaceTempView("hold_flights")
    airports.createOrReplaceTempView("hold_airports")
    return spark.sql(
        _HOLDS_SQL.format(
            candidates="hold_candidates",
            flights="hold_flights",
            airports="hold_airports",
        )
    )


def write_flight_holds(
    df: DataFrame, path: str, mode: str = "overwrite", release_date: str | None = None
) -> None:
    write_delta(df, path, mode=mode, release_date=release_date)


def read_flight_holds(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def main(argv: list[str] | None = None) -> None:
    import argparse

    from pyspark.sql import functions as F

    from adsb.airports import DEFAULT_AIRPORTS_URI, read_airports
    from adsb.flight_model import (
        DEFAULT_FLIGHT_OBSERVATIONS_URI,
        DEFAULT_FLIGHTS_MODEL_URI,
        read_flight_observations,
        read_flights,
    )
    from adsb.quality import assert_valid, report, validate_flight_holds
    from adsb.spark_explore import build_session

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--observations", default=DEFAULT_FLIGHT_OBSERVATIONS_URI)
    parser.add_argument("--flights", default=DEFAULT_FLIGHTS_MODEL_URI)
    parser.add_argument("--airports", default=DEFAULT_AIRPORTS_URI)
    parser.add_argument("--holds", default=DEFAULT_HOLDS_URI)
    parser.add_argument("--min-turn-degrees", type=float, default=MIN_TURN_DEGREES)
    parser.add_argument("--max-span-km", type=float, default=MAX_SPAN_KM)
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])
    parser.add_argument(
        "--release-date",
        default=None,
        help="process one day only, replacing just that partition",
    )
    args = parser.parse_args(argv)

    spark = build_session("adsb-holds")
    try:
        points = read_flight_observations(spark, args.observations)
        flights = read_flights(spark, args.flights)
        if args.release_date:
            day = F.lit(args.release_date)
            points = points.where(F.col("release_date") == day)
            flights = flights.where(F.col("release_date") == day)

        holds = to_flight_holds(
            points,
            flights,
            read_airports(spark, args.airports),
            min_turn_degrees=args.min_turn_degrees,
            max_span_km=args.max_span_km,
        )

        print(f"Writing {args.holds} (min_turn={args.min_turn_degrees} deg, "
              f"max_span={args.max_span_km} km)")
        write_flight_holds(holds, args.holds, args.mode, args.release_date)

        table = read_flight_holds(spark, args.holds)
        table.createOrReplaceTempView("holds")
        print(f"detected holds: {table.count():,}")
        print(f"flights with a detected hold: "
              f"{table.select('flight_id').distinct().count():,}")

        print("\n--- schema ---")
        table.printSchema()

        print("--- shape of what was detected ---")
        spark.sql(
            """
            SELECT
              PERCENTILE_APPROX(duration_seconds, array(0.5, 0.9))  AS duration_50_90,
              PERCENTILE_APPROX(circuits, array(0.5, 0.9))          AS circuits_50_90,
              PERCENTILE_APPROX(span_km, array(0.5, 0.9))           AS span_km_50_90,
              PERCENTILE_APPROX(mean_altitude_ft, array(0.5, 0.9))  AS altitude_50_90,
              ROUND(100.0 * COUNT(arrival_airport_ident) / COUNT(*), 1) AS pct_with_airport
            FROM holds
            """
        ).show(truncate=False)

        print("--- how far from the arrival airport ---")
        spark.sql(
            """
            SELECT CASE
                     WHEN distance_to_arrival_airport_km IS NULL THEN 'airport unknown'
                     WHEN distance_to_arrival_airport_km <  50 THEN 'terminal area (<50 km)'
                     WHEN distance_to_arrival_airport_km < 150 THEN 'near (50-150 km)'
                     ELSE 'en route (>150 km)' END AS where_held,
                   COUNT(*) AS holds
            FROM holds GROUP BY 1 ORDER BY holds DESC
            """
        ).show(truncate=False)

        results = validate_flight_holds(table)
        print(report("flight_holds", results))
        assert_valid("flight_holds", results)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
