"""Daily airport operations inferred from ADS-B, as a Gold Delta table.

    silver/flight_segments + reference/airports  ->  gold/airport_daily_operations

WHAT THESE NUMBERS ARE
    Counts of aircraft movements *observed by a volunteer ADS-B receiver
    network and attributed to an airport by proximity*. They are an
    approximation of airport activity, produced entirely from radio
    observations.

WHAT THEY ARE NOT
    Official airport or airline statistics. No flight plan, airline schedule,
    airport AODB or regulatory filing is involved. They will not reconcile
    with published movement counts, and should never be presented as though
    they might. Every row carries ``metric_source = 'adsb_inferred'`` so the
    distinction survives into whatever BI tool reads the table.

    Coverage is the main reason they undercount: adsb.lol sees what its
    feeders see, which is dense over Europe and North America and thin
    elsewhere, and ground coverage is patchy everywhere.

HOW A MOVEMENT IS ATTRIBUTED
    A segment's first observation becomes a *departure* from an airport, and
    its last observation an *arrival* at one, when that endpoint is:

      * within ``MATCH_RADIUS_KM`` of the airport, and
      * on the ground, or below ``MAX_HEIGHT_ABOVE_AIRPORT_FT`` above the
        airport's own elevation.

    The nearest qualifying airport wins. Both thresholds were calibrated on
    the data rather than assumed: segments that began on the ground -- which
    are by definition at an airport -- lie a median 1.0 km and a 90th
    percentile 2.3 km from the nearest large/medium airport. A 5 km radius
    captures 93.7% of them and widening to 10 km adds only 1.1%. Meanwhile
    segments starting above 5,000 ft sit a median 46.4 km away, so the height
    test is what keeps overflights out.

    Height is measured above the airport's elevation, not above sea level, so
    high-altitude airports behave the same as sea-level ones.

LIMITATIONS
    * Only large and medium airports are candidates. Movements at small
      strips and heliports are not counted, and an aircraft on the ground at
      one may be attributed to a larger airport within 5 km.
    * A flight whose coverage begins or ends in mid-air near an airport can
      produce a spurious movement; the height test limits but does not
      eliminate this.
    * Segments that never move are excluded, which removes the fixed ground
      transmitters present in the source (readsb reports some with an
      ``aircraft_type`` of ``TWR``) as well as single-observation fragments.
      Those emitters sit *at* airports, so leaving them in would inflate
      exactly these counts.
    * A turnaround tracked continuously is one segment, so its intermediate
      landing and takeoff are invisible -- inherited from flight
      reconstruction and a source of undercounting.
    * Anything inherited from the reconstruction phase: coverage-hole splits,
      midnight truncation.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from adsb.airports import MATCH_RADIUS_KM, airport_movements  # noqa: F401  (public surface)
from adsb.delta_io import write_delta
from adsb.quality import assert_valid, report, validate_gold

DEFAULT_GOLD_URI = os.environ.get(
    "ADSB_GOLD_URI", "s3a://adsb/gold/airport_daily_operations"
)

_METRICS_SQL = """
SELECT
    DATE(event_time)                                            AS operations_date,
    ident                                                       AS airport_ident,
    iata_code                                                   AS airport_iata,
    airport_name,
    airport_type,
    iso_country,
    latitude_deg                                                AS airport_latitude,
    longitude_deg                                               AS airport_longitude,
    SUM(CASE WHEN movement_type = 'arrival'   THEN 1 ELSE 0 END) AS arrivals,
    SUM(CASE WHEN movement_type = 'departure' THEN 1 ELSE 0 END) AS departures,
    COUNT(*)                                                    AS total_operations,
    COUNT(DISTINCT icao)                                        AS unique_aircraft,
    MIN(event_time)                                             AS first_operation_time,
    MAX(event_time)                                             AS last_operation_time,
    MAX(release_tag)                                            AS release_tag,
    MAX(release_date)                                           AS release_date,
    'adsb_inferred'                                             AS metric_source
FROM {movements}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
"""


def to_airport_metrics(movements: DataFrame) -> DataFrame:
    """Daily operations per airport."""
    movements.createOrReplaceTempView("airport_movements")
    return movements.sparkSession.sql(_METRICS_SQL.format(movements="airport_movements"))


def write_airport_metrics(
    metrics: DataFrame,
    path: str,
    mode: str = "overwrite",
    release_date: str | None = None,
) -> None:
    write_delta(metrics, path, mode=mode, release_date=release_date)


def read_airport_metrics(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def main(argv: list[str] | None = None) -> None:
    import argparse

    from adsb.airports import DEFAULT_AIRPORTS_URI, read_airports
    from adsb.flights import DEFAULT_FLIGHTS_URI, read_flight_segments
    from adsb.spark_explore import build_session

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--flights", default=DEFAULT_FLIGHTS_URI)
    parser.add_argument("--airports", default=DEFAULT_AIRPORTS_URI)
    parser.add_argument("--gold", default=DEFAULT_GOLD_URI)
    parser.add_argument("--radius-km", type=float, default=MATCH_RADIUS_KM)
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])
    parser.add_argument(
        "--release-date",
        default=None,
        help="process one day only, replacing just that partition",
    )
    args = parser.parse_args(argv)

    spark = build_session("adsb-gold")
    try:
        segments = read_flight_segments(spark, args.flights)
        if args.release_date:
            segments = segments.where(
                F.col("release_date") == F.lit(args.release_date)
            )
        airports = read_airports(spark, args.airports)

        movements = airport_movements(segments, airports, args.radius_km)
        metrics = to_airport_metrics(movements)

        print(f"Writing {args.gold} (mode={args.mode})")
        write_airport_metrics(
            metrics, args.gold, args.mode, release_date=args.release_date
        )

        table = read_airport_metrics(spark, args.gold)
        table.createOrReplaceTempView("gold")

        print(f"flight segments in: {segments.count():,}")
        print(f"airports with inferred operations: {table.count():,}")

        print("\n--- Gold schema ---")
        table.printSchema()

        print("--- totals ---")
        spark.sql(
            """
            SELECT SUM(arrivals) AS arrivals, SUM(departures) AS departures,
                   SUM(total_operations) AS operations,
                   COUNT(*) AS airport_days
            FROM gold
            """
        ).show(truncate=False)

        print("--- busiest inferred airports (NOT official statistics) ---")
        spark.sql(
            """
            SELECT airport_ident, airport_iata, airport_name, iso_country,
                   arrivals, departures, total_operations, unique_aircraft
            FROM gold ORDER BY total_operations DESC LIMIT 15
            """
        ).show(truncate=False)

        results = validate_gold(table)
        print(report("gold", results))
        assert_valid("gold", results)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
