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

DEFAULT_GOLD_URI = os.environ.get(
    "ADSB_GOLD_URI", "s3a://adsb/gold/airport_daily_operations"
)

MATCH_RADIUS_KM = 5.0
MAX_HEIGHT_ABOVE_AIRPORT_FT = 5000.0

EARTH_RADIUS_KM = 6371.0

# Each airport is expanded into the 3x3 block of one-degree cells around it so
# an endpoint can hash-join to nearby airports instead of being compared
# against all 5,281 of them. One degree is ~111 km, comfortably wider than the
# match radius.
_AIRPORT_CELLS_SQL = """
SELECT
    ident, iata_code, name, type, iso_country,
    latitude_deg, longitude_deg, elevation_ft,
    FLOOR(latitude_deg) + dlat  AS cell_lat,
    FLOOR(longitude_deg) + dlon AS cell_lon
FROM {airports}
    LATERAL VIEW EXPLODE(ARRAY(-1, 0, 1)) t1 AS dlat
    LATERAL VIEW EXPLODE(ARRAY(-1, 0, 1)) t2 AS dlon
"""

_MOVEMENTS_SQL = f"""
WITH moving AS (
    -- a segment that never changes position is a fixed transmitter or a
    -- single stray observation, not an aircraft movement
    SELECT * FROM {{segments}}
    WHERE NOT (start_latitude = end_latitude AND start_longitude = end_longitude)
),
endpoints AS (
    SELECT segment_id, icao, aircraft_type, operator, callsign, release_tag,
           'departure' AS movement_type,
           start_time AS event_time, start_latitude AS latitude,
           start_longitude AS longitude, start_altitude_ft AS altitude_ft
    FROM moving
    UNION ALL
    SELECT segment_id, icao, aircraft_type, operator, callsign, release_tag,
           'arrival',
           end_time, end_latitude, end_longitude, end_altitude_ft
    FROM moving
),
candidates AS (
    SELECT
        e.*,
        a.ident, a.iata_code, a.name AS airport_name, a.type AS airport_type,
        a.iso_country, a.latitude_deg, a.longitude_deg, a.elevation_ft,
        {EARTH_RADIUS_KM} * 2 * ASIN(SQRT(
            POW(SIN(RADIANS(a.latitude_deg - e.latitude) / 2), 2)
            + COS(RADIANS(e.latitude)) * COS(RADIANS(a.latitude_deg))
            * POW(SIN(RADIANS(a.longitude_deg - e.longitude) / 2), 2)
        )) AS distance_km
    FROM endpoints e
    JOIN {{airport_cells}} a
      ON FLOOR(e.latitude) = a.cell_lat AND FLOOR(e.longitude) = a.cell_lon
),
qualifying AS (
    SELECT * FROM candidates
    WHERE distance_km <= {{radius_km}}
      AND (
            altitude_ft IS NULL  -- on the ground
            OR altitude_ft - COALESCE(elevation_ft, 0) <= {{max_height_ft}}
          )
),
nearest AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY segment_id, movement_type
        ORDER BY distance_km, ident  -- ident breaks ties deterministically
    ) AS airport_rank
    FROM qualifying
)
SELECT * FROM nearest WHERE airport_rank = 1
"""

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
    'adsb_inferred'                                             AS metric_source
FROM {movements}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
"""


def airport_movements(
    segments: DataFrame,
    airports: DataFrame,
    radius_km: float = MATCH_RADIUS_KM,
    max_height_ft: float = MAX_HEIGHT_ABOVE_AIRPORT_FT,
) -> DataFrame:
    """One row per inferred movement: a segment endpoint matched to an airport."""
    spark = segments.sparkSession
    segments.createOrReplaceTempView("flight_segments")
    airports.createOrReplaceTempView("airports")
    spark.sql(_AIRPORT_CELLS_SQL.format(airports="airports")).createOrReplaceTempView(
        "airport_cells"
    )
    return spark.sql(
        _MOVEMENTS_SQL.format(
            segments="flight_segments",
            airport_cells="airport_cells",
            radius_km=radius_km,
            max_height_ft=max_height_ft,
        )
    )


def to_airport_metrics(movements: DataFrame) -> DataFrame:
    """Daily operations per airport."""
    movements.createOrReplaceTempView("airport_movements")
    return movements.sparkSession.sql(_METRICS_SQL.format(movements="airport_movements"))


def write_airport_metrics(metrics: DataFrame, path: str, mode: str = "overwrite") -> None:
    writer = metrics.write.format("delta").mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    writer.save(path)


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
    args = parser.parse_args(argv)

    spark = build_session("adsb-gold")
    try:
        segments = read_flight_segments(spark, args.flights)
        airports = read_airports(spark, args.airports)

        movements = airport_movements(segments, airports, args.radius_km)
        metrics = to_airport_metrics(movements)

        print(f"Writing {args.gold} (mode={args.mode})")
        write_airport_metrics(metrics, args.gold, args.mode)

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
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
