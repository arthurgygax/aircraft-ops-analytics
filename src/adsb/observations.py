"""The canonical observation table: decode, clean, deduplicate, segment.

    s3a://<bucket>/raw/...  ->  <root>/observations

One pass from the raw objects to the only point-grain dataset in the pipeline.
It replaces what used to be three separate 44.6M-row tables -- Bronze, Silver
and flight_observations -- which held the same rows at the same grain.

WHAT A ROW IS
    One accepted ADS-B reception, keyed ``(flight_id, observation_seq)`` and
    unique on ``(icao, event_time)``.

WHY THE DEDUP HAPPENS HERE, IN THE ARRAY
    ``readsb`` records one trace point per reception. When a position arrives
    twice in the same hundredth of a second -- overwhelmingly because an ADS-R
    or TIS-B relay rebroadcast a position we also received directly -- two
    points share a timestamp. Measured across both releases: 72,438 rows,
    0.162% of the data, and a duplicate rate of 0.012% on direct reception
    against 3.9-22% on relayed reception.

    Every one of the 72,277 colliding keys is a pair of *adjacent entries in
    one aircraft's own trace array*. There were no exceptions. Resolving them
    is therefore a within-row operation on the array, which is why it happens
    during decoding: ``sort_array`` orders each aircraft's points and a
    ``filter`` keeps the first of each timestamp. No shuffle is involved.

    The previous implementation exploded the arrays into 44.7M rows, wrote
    them, read them back and then re-partitioned all of them by ``icao`` to
    rediscover an adjacency the source had already given for free.

THE WINNER RULE, STATED RATHER THAN IMPLIED
    Of two receptions sharing an ``(icao, event_time)``, keep the more complete
    row -- how many of altitude, ground speed, track, vertical rate and
    callsign are present -- then the lower ``(latitude, longitude)``, then the
    earlier point in the source array.

    The completeness score ties in 71.1% of cases, so most of the time the rule
    reduces to "keep whichever reception is further south, then west". That is
    deterministic and reproducible, not objectively preferable: the two rows
    are a median 1.8 m apart. ``adsb.quality`` publishes the collisions
    resolved per day so the number is observable rather than being the
    difference between two row counts.

WHAT IS DECODED, AND WHAT IS NOT
    The positional trace arrays become named typed columns, because
    ``array<array<string>>`` is not something you can query or evolve;
    ``event_time`` is materialized from the day-start epoch plus each point's
    offset, since the raw offset is meaningless without its file's header; and
    altitude's ``number | "ground"`` union is split into a numeric
    ``altitude_ft`` and a boolean ``on_ground``, because a column has one type.

    Two classes of impossible value are nulled, both measured rather than
    assumed. Bad *values* are nulled rather than their rows dropped, so a
    glitched speed never costs a valid position fix. Coordinate ranges are
    asserted in ``adsb.quality``, never silently repaired.

SEGMENTATION
    A flight is a period during which one transponder was tracked
    continuously, split when the gap between observations exceeds
    ``GAP_SECONDS``. That is the whole rule, and the threshold was measured;
    see ``adsb.flights`` for the calibration and for what a flight is not.

    This is the pipeline's one genuinely global window -- partition by
    ``icao``, order by time -- and it is irreducible: reconstructing flights
    from tracking gaps is what it means.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from adsb.delta_io import table_uri, write_delta

DEFAULT_OBSERVATIONS_URI = os.environ.get(
    "ADSB_OBSERVATIONS_URI", table_uri("observations")
)

# Above any plausible ground speed for civil traffic: the fastest credible
# reports in the sample are B788s at 657 kt, and everything past 700 kt is a
# glitch (a DR400 light aircraft at 1800 kt, a helicopter at 944 kt).
MAX_GROUND_SPEED_KT = 700

# Civil aircraft do not sustain 20,000 fpm; one row exceeds this.
MAX_VERTICAL_RATE_FPM = 20000

# See SEGMENTATION above: measured, not guessed.
GAP_SECONDS = 15 * 60

# Each trace point is a positional array whose element types vary by position.
# Read as strings and cast here, so the mixed `number | "ground"` at position 3
# keeps both its forms instead of whichever one loses.
_ALTITUDE = "CASE WHEN p[3] <> 'ground' THEN CAST(p[3] AS DOUBLE) END"
_CALLSIGN = "TRIM(GET_JSON_OBJECT(p[8], '$.flight'))"

# The struct's FIELD ORDER IS THE DEDUP ORDER: sort_array compares structs
# field by field, ascending, nulls first -- which is exactly the ordering the
# window used to express. rank_score is negated so ascending means "most
# complete first", and idx makes the order total, so the choice among
# otherwise identical points is the earliest one rather than an arbitrary one.
_TYPED_POINTS_SQL = f"""
transform(trace, (p, i) -> struct(
    to_timestamp(timestamp + CAST(p[0] AS DOUBLE))          AS event_time,
    -(  (CASE WHEN ({_ALTITUDE}) IS NULL THEN 0 ELSE 1 END)
      + (CASE WHEN CAST(p[4] AS DOUBLE) IS NULL THEN 0 ELSE 1 END)
      + (CASE WHEN CAST(p[5] AS DOUBLE) IS NULL THEN 0 ELSE 1 END)
      + (CASE WHEN CAST(p[7] AS DOUBLE) IS NULL THEN 0 ELSE 1 END)
      + (CASE WHEN ({_CALLSIGN}) IS NULL THEN 0 ELSE 1 END))  AS rank_score,
    CAST(p[1] AS DOUBLE)                                    AS latitude,
    CAST(p[2] AS DOUBLE)                                    AS longitude,
    i                                                       AS idx,
    (p[3] = 'ground')                                       AS on_ground,
    {_ALTITUDE}                                             AS altitude_ft,
    CAST(p[4] AS DOUBLE)                                    AS ground_speed_kt,
    CAST(p[5] AS DOUBLE)                                    AS track_deg,
    CAST(p[7] AS DOUBLE)                                    AS vertical_rate_fpm,
    {_CALLSIGN}                                             AS callsign
))
"""

# Keep the first point of each timestamp. `points` is already sorted, so "first
# of its timestamp" is "its predecessor carries a different one" -- which means
# each element has to see the element before it.
#
# The obvious spelling, `filter(points, (x, i) -> points[i-1]...)`, reaches back
# into the array from inside the lambda, and Spark then re-evaluates the whole
# sort once per element. Measured: it did not finish a 293k-row release in five
# minutes. Pairing each point with its predecessor first, in one row-level pass,
# keeps the lambda looking only at its own argument.
_PREV_TIME_SQL = """
concat(
    array(CAST(NULL AS TIMESTAMP)),
    slice(transform(points, x -> x.event_time), 1, GREATEST(size(points) - 1, 0))
)
"""

_DEDUP_SQL = "filter(paired, z -> NOT (z.prev_time <=> z.points.event_time))"

_PROJECT_SQL = f"""
SELECT
    icao,
    -- readsb prefixes non-ICAO addresses (TIS-B, ADS-R relays) with '~'
    NOT icao LIKE '~%'                              AS is_icao_address,
    registration,
    aircraft_type,
    operator,
    p.event_time,
    p.latitude,
    p.longitude,
    p.on_ground,
    p.altitude_ft,
    CASE WHEN p.ground_speed_kt > {MAX_GROUND_SPEED_KT}
         THEN NULL ELSE p.ground_speed_kt END       AS ground_speed_kt,
    p.track_deg,
    CASE WHEN ABS(p.vertical_rate_fpm) > {MAX_VERTICAL_RATE_FPM}
         THEN NULL ELSE p.vertical_rate_fpm END     AS vertical_rate_fpm,
    -- a padded-empty callsign is a missing callsign, not a value
    NULLIF(p.callsign, '')                          AS callsign,
    release_tag,
    release_date
FROM {{source}}
"""

# The segmentation. Ordering by (event_time, latitude, longitude) rather than
# event_time alone keeps observation_seq reproducible whatever order Spark
# happened to read the files in; the dedup above means the tie-break never
# fires today, and it costs nothing to keep the order total anyway.
_ASSIGN_SQL = """
WITH ordered AS (
    SELECT
        *,
        UNIX_TIMESTAMP(event_time) - UNIX_TIMESTAMP(
            LAG(event_time) OVER (PARTITION BY icao ORDER BY event_time)
        ) AS gap_seconds
    FROM {source}
),
marked AS (
    -- first observation of an aircraft has a NULL gap and starts a flight
    SELECT *, CASE WHEN gap_seconds IS NULL OR gap_seconds > {gap}
                   THEN 1 ELSE 0 END AS is_break
    FROM ordered
),
segmented AS (
    SELECT
        *,
        SUM(is_break) OVER (
            PARTITION BY icao ORDER BY event_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS segment_no
    FROM marked
)
SELECT
    -- the flight identifier, stamped on every observation of the flight:
    -- aircraft address + the instant it was first seen in this flight
    CONCAT(
        icao, '_',
        DATE_FORMAT(MIN(event_time) OVER (PARTITION BY icao, segment_no),
                    'yyyyMMddHHmmss')
    ) AS flight_id,
    icao,
    is_icao_address,
    registration,
    aircraft_type,
    operator,
    event_time,
    ROW_NUMBER() OVER (
        PARTITION BY icao, segment_no ORDER BY event_time, latitude, longitude
    ) AS observation_seq,
    latitude,
    longitude,
    on_ground,
    altitude_ft,
    ground_speed_kt,
    track_deg,
    vertical_rate_fpm,
    -- kept per point, not just per flight: it is how a callsign change part
    -- way through a flight stays visible
    callsign,
    release_tag,
    release_date
FROM segmented
"""


def decode(aircraft: DataFrame) -> DataFrame:
    """Raw aircraft rows -> deduplicated, cleaned observations.

    Entirely within a row: each aircraft's trace array is typed, sorted and
    collapsed before it is exploded, so no part of this shuffles.

    ``_metadata.file_path`` is read on the aircraft rows, before the explode,
    and carried down as an ordinary column. That is deliberate:
    ``input_file_name()`` evaluated after an explode is fragile and can come
    back empty, whereas the metadata column is materialized by the scan.
    """
    release_tag = F.regexp_extract(
        # .../<release-tag>/traces/1c/x.json.gz -- anchored on the layout the
        # archive itself defines, not on the bucket prefix we happened to use
        F.col("_metadata.file_path"), r"([^/]+)/traces/", 1
    )
    typed = aircraft.select(
        F.col("icao"),
        F.col("r").alias("registration"),
        F.col("t").alias("aircraft_type"),
        F.col("ownOp").alias("operator"),
        release_tag.alias("release_tag"),
        # partition key, taken from the release identity rather than from the
        # observations: see adsb.delta_io
        F.to_date(
            F.regexp_extract(release_tag, r"^v(\d{4}\.\d{2}\.\d{2})", 1), "yyyy.MM.dd"
        ).alias("release_date"),
        F.expr(f"sort_array({_TYPED_POINTS_SQL})").alias("points"),
    )
    paired = typed.withColumn(
        "paired", F.arrays_zip(F.col("points"), F.expr(_PREV_TIME_SQL).alias("prev_time"))
    )
    kept = paired.withColumn("paired", F.expr(_DEDUP_SQL))

    view = "decoded_aircraft"
    kept.select(
        "icao", "registration", "aircraft_type", "operator",
        "release_tag", "release_date",
        F.explode("paired").alias("z"),
    ).select(
        "icao", "registration", "aircraft_type", "operator",
        "release_tag", "release_date",
        F.col("z.points").alias("p"),
    ).createOrReplaceTempView(view)
    return aircraft.sparkSession.sql(_PROJECT_SQL.format(source=view))


def assign_flights(
    observations: DataFrame, gap_seconds: int = GAP_SECONDS
) -> DataFrame:
    """Stamp every observation with the flight it belongs to.

    This is the segmentation. Aggregating the result gives the flight table;
    keeping it gives the trajectory. Both come from this one pass, so they can
    never disagree about where a flight starts.
    """
    view = "cleaned_observations"
    observations.createOrReplaceTempView(view)
    return observations.sparkSession.sql(
        _ASSIGN_SQL.format(source=view, gap=gap_seconds)
    )


def to_observations(
    aircraft: DataFrame, gap_seconds: int = GAP_SECONDS
) -> DataFrame:
    """The whole pass: raw aircraft rows -> the canonical observation table."""
    return assign_flights(decode(aircraft), gap_seconds)


def write_observations(
    df: DataFrame,
    path: str,
    release_date: str | None = None,
    full_rebuild: bool = False,
) -> None:
    write_delta(df, path, release_date=release_date, full_rebuild=full_rebuild)


def read_observations(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def main(argv: list[str] | None = None) -> None:
    import argparse

    from adsb.ingest import release_date as release_date_of
    from adsb.quality import assert_valid, collisions_resolved, report, validate_observations
    from adsb.spark_explore import DEFAULT_RAW_URI, build_session, read_aircraft

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--raw", default=DEFAULT_RAW_URI)
    parser.add_argument("--observations", default=DEFAULT_OBSERVATIONS_URI)
    parser.add_argument("--gap-seconds", type=int, default=GAP_SECONDS)
    parser.add_argument(
        "--tag",
        default=None,
        help="process one release, e.g. v2025.12.30-planes-readsb-prod-0",
    )
    parser.add_argument(
        "--full-rebuild",
        action="store_true",
        help="replace every day in the table, not just this release's",
    )
    args = parser.parse_args(argv)

    raw_uri = args.raw
    day = None
    if args.tag:
        raw_uri = f"{args.raw.rstrip('/')}/{args.tag}"
        day = release_date_of(args.tag)
    if day is None and not args.full_rebuild:
        parser.error("pass --tag, or --full-rebuild to replace the whole table")

    spark = build_session("adsb-observations")
    try:
        aircraft = read_aircraft(spark, raw_uri)
        print(f"Writing {args.observations} ({'full rebuild' if args.full_rebuild else day})")
        write_observations(
            to_observations(aircraft, args.gap_seconds),
            args.observations,
            release_date=day,
            full_rebuild=args.full_rebuild,
        )

        table = read_observations(spark, args.observations)
        written = table if day is None else table.where(F.col("release_date") == day)
        print(f"observations written: {written.count():,}")
        print(f"observations total:   {table.count():,}")

        print("\n--- schema ---")
        table.printSchema()

        print("--- dedup ---")
        raw_points, kept = collisions_resolved(aircraft, written)
        print(f"  trace points read:   {raw_points:,}")
        print(f"  observations kept:   {kept:,}")
        print(f"  collisions resolved: {raw_points - kept:,} "
              f"({100 * (raw_points - kept) / max(raw_points, 1):.4f}%)")

        results = validate_observations(table)
        print(report("observations", results))
        assert_valid("observations", results)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
