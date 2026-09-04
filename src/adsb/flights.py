"""Reconstruct inferred flight segments from Silver observations.

    s3a://<bucket>/silver/observations  ->  s3a://<bucket>/silver/flight_segments

WHAT THIS IS NOT
    These are not airline schedules or airport movement records. Nothing here
    comes from a flight plan, an AODB or an official source. A "flight segment"
    is a period during which one ADS-B transponder was tracked continuously by
    the adsb.lol receiver network -- an inference from radio observations,
    which is usually one flight but is not guaranteed to be.

WHAT THE DATA ACTUALLY SUPPORTS
    Measured on the development sample before choosing the algorithm:

    * Tracking is dense while an aircraft is in coverage: the median gap
      between consecutive observations is 4 s and the 99th percentile is 35 s.
      Gaps long enough to mean "left coverage" are therefore easy to separate
      from normal cadence.
    * Ground coverage is poor. Only 103 of 224 aircraft have any on-ground
      observation at all, so takeoff and landing cannot be detected reliably
      from ``on_ground`` transitions. Time gaps are the robust signal; ground
      state is recorded as supporting evidence, not used for segmentation.
    * Callsigns are unreliable as a segmentation key: 29 aircraft never report
      one, and of 249 callsign changes only 24 coincide with a gap over ten
      minutes. Splitting on callsign change would invent boundaries mid-flight.

ALGORITHM
    Order each aircraft's observations by time and start a new segment whenever
    the gap to the previous observation exceeds ``GAP_SECONDS``. That is the
    whole rule.

    The threshold was chosen by measurement, not taste. Segments holding more
    than one distinct callsign -- the signature of two flights merged into one
    segment -- number 14-19 for thresholds between 5 and 30 minutes, then jump
    to 47 at 60 minutes and 84 at 120. Below 10 minutes, single-observation
    fragments grow instead (20 at 5 minutes, 6 at 15). Fifteen minutes sits in
    the stable middle.

LIMITATIONS
    * A long coverage hole splits one real flight into two segments. Oceanic
      legs are the obvious case.
    * An aircraft tracked continuously through a turnaround yields one segment
      covering two real flights; 15 such segments carry more than one callsign
      in the sample.
    * Segments are clipped by the boundaries of the day being processed, so a
      flight crossing midnight is truncated.
    * A segment may be a single observation; ``n_observations`` is published so
      callers can decide what is usable rather than having that decided here.
    * Non-ICAO (``~``) addresses are TIS-B/ADS-R relays and can produce
      duplicate shadows of real aircraft. They are kept and flagged via
      ``is_icao_address`` rather than silently dropped.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession

from adsb.quality import assert_valid, report, validate_flight_segments

DEFAULT_FLIGHTS_URI = os.environ.get(
    "ADSB_FLIGHTS_URI", "s3a://adsb/silver/flight_segments"
)

# See ALGORITHM above: measured, not guessed.
GAP_SECONDS = 15 * 60

_SEGMENT_SQL = """
WITH ordered AS (
    SELECT
        *,
        UNIX_TIMESTAMP(event_time) - UNIX_TIMESTAMP(
            LAG(event_time) OVER (PARTITION BY icao ORDER BY event_time)
        ) AS gap_seconds
    FROM {source}
),
marked AS (
    -- first observation of an aircraft has a NULL gap and starts a segment
    SELECT *, CASE WHEN gap_seconds IS NULL OR gap_seconds > {gap} THEN 1 ELSE 0 END AS is_break
    FROM ordered
),
segmented AS (
    SELECT
        *,
        SUM(is_break) OVER (
            PARTITION BY icao ORDER BY event_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS segment_no,
        -- the callsign this segment reported first; segments spanning a
        -- turnaround will have more than one, hence n_callsigns below
        FIRST_VALUE(callsign) IGNORE NULLS OVER (
            PARTITION BY icao, SUM(is_break) OVER (
                PARTITION BY icao ORDER BY event_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
            ORDER BY event_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS segment_callsign
    FROM marked
)
SELECT
    CONCAT(icao, '_', DATE_FORMAT(MIN(event_time), 'yyyyMMddHHmmss')) AS segment_id,
    icao,
    MAX(is_icao_address)                        AS is_icao_address,
    MAX(registration)                           AS registration,
    MAX(aircraft_type)                          AS aircraft_type,
    MAX(operator)                               AS operator,
    MAX(segment_callsign)                       AS callsign,
    COUNT(DISTINCT callsign)                    AS n_callsigns,
    MIN(event_time)                             AS start_time,
    MAX(event_time)                             AS end_time,
    UNIX_TIMESTAMP(MAX(event_time))
        - UNIX_TIMESTAMP(MIN(event_time))       AS duration_seconds,
    COUNT(*)                                    AS n_observations,
    MIN_BY(latitude, event_time)                AS start_latitude,
    MIN_BY(longitude, event_time)               AS start_longitude,
    MAX_BY(latitude, event_time)                AS end_latitude,
    MAX_BY(longitude, event_time)               AS end_longitude,
    MIN_BY(on_ground, event_time)               AS started_on_ground,
    MAX_BY(on_ground, event_time)               AS ended_on_ground,
    -- altitude at each endpoint, not just the maximum: airport attribution
    -- needs to tell a real departure from a segment that merely started
    -- mid-cruise over an airport
    MIN_BY(altitude_ft, event_time)             AS start_altitude_ft,
    MAX_BY(altitude_ft, event_time)             AS end_altitude_ft,
    MAX(CASE WHEN on_ground THEN 1 ELSE 0 END) = 1 AS saw_ground,
    MAX(altitude_ft)                            AS max_altitude_ft,
    MAX(ground_speed_kt)                        AS max_ground_speed_kt,
    MAX(release_tag)                            AS release_tag
FROM segmented
GROUP BY icao, segment_no
"""


def to_flight_segments(observations: DataFrame, gap_seconds: int = GAP_SECONDS) -> DataFrame:
    """Segment observations into inferred flights on temporal gaps."""
    view = "silver_observations"
    observations.createOrReplaceTempView(view)
    return observations.sparkSession.sql(
        _SEGMENT_SQL.format(source=view, gap=gap_seconds)
    )


def write_flight_segments(segments: DataFrame, path: str, mode: str = "overwrite") -> None:
    writer = segments.write.format("delta").mode(mode)
    if mode == "overwrite":
        # Delta refuses a schema change by default. An overwrite replaces the
        # table wholesale, so accepting the new schema there is the intent;
        # append still gets the protection.
        writer = writer.option("overwriteSchema", "true")
    writer.save(path)


def read_flight_segments(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def main(argv: list[str] | None = None) -> None:
    import argparse

    from adsb.silver import DEFAULT_SILVER_URI, read_silver
    from adsb.spark_explore import build_session

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--silver", default=DEFAULT_SILVER_URI)
    parser.add_argument("--flights", default=DEFAULT_FLIGHTS_URI)
    parser.add_argument("--gap-seconds", type=int, default=GAP_SECONDS)
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])
    args = parser.parse_args(argv)

    spark = build_session("adsb-flights")
    try:
        observations = read_silver(spark, args.silver)
        segments = to_flight_segments(observations, args.gap_seconds)

        print(f"Writing {args.flights} (gap={args.gap_seconds}s, mode={args.mode})")
        write_flight_segments(segments, args.flights, args.mode)

        table = read_flight_segments(spark, args.flights)
        print(f"observations: {observations.count():,}")
        print(f"inferred flight segments: {table.count():,}")

        print("\n--- schema ---")
        table.printSchema()

        table.createOrReplaceTempView("flight_segments")
        print("--- segment profile ---")
        spark.sql(
            """
            SELECT
                COUNT(*)                                            AS segments,
                COUNT(DISTINCT icao)                                AS aircraft,
                ROUND(PERCENTILE_APPROX(duration_seconds/60, 0.5),1) AS median_minutes,
                ROUND(PERCENTILE_APPROX(duration_seconds/60, 0.9),1) AS p90_minutes,
                SUM(CASE WHEN n_callsigns > 1 THEN 1 ELSE 0 END)    AS multi_callsign,
                SUM(CASE WHEN n_observations < 2 THEN 1 ELSE 0 END) AS single_observation,
                SUM(CASE WHEN saw_ground THEN 1 ELSE 0 END)         AS touched_ground
            FROM flight_segments
            """
        ).show(truncate=False)

        print("--- longest segments ---")
        spark.sql(
            """
            SELECT segment_id, callsign, aircraft_type,
                   ROUND(duration_seconds/60) AS minutes, n_observations,
                   ROUND(max_altitude_ft) AS max_alt_ft
            FROM flight_segments ORDER BY duration_seconds DESC LIMIT 5
            """
        ).show(truncate=False)

        results = validate_flight_segments(table)
        print(report("flight_segments", results))
        assert_valid("flight_segments", results)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
