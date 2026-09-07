"""Airport reference data, inferred movements, and daily airport operations.

    OurAirports                     ->  reference/airports.csv
    flight aggregate + airports     ->  <root>/movements
    movements + flight_holds        ->  <root>/airport_daily_operations

Attributing an observed trajectory to an airport needs airport coordinates,
which ADS-B does not carry. OurAirports (https://ourairports.com/data/) is a
public-domain dataset; we keep only the large and medium airports, since small
strips and heliports would attract general-aviation noise without adding
commercial movements.

WHY MOVEMENTS IS A TABLE
    Because three consumers need it -- the flight table's airport columns, the
    daily operations rollup, and the Power BI model, which reports on movement
    grain so that "arrivals vs departures" is a slicer rather than two columns.
    It used to be computed once for each of them and stored for none.

HOW A MOVEMENT IS ATTRIBUTED
    A flight's first observation becomes a *departure* from an airport, and its
    last observation an *arrival* at one, when that endpoint is:

      * within ``MATCH_RADIUS_KM`` of the airport, and
      * on the ground, or below ``MAX_HEIGHT_ABOVE_AIRPORT_FT`` above the
        airport's own elevation.

    The nearest qualifying airport wins. Both thresholds were calibrated on the
    data rather than assumed: flights that began on the ground -- which are by
    definition at an airport -- lie a median 1.0 km and a 90th percentile
    2.3 km from the nearest large/medium airport. A 5 km radius captures 93.7%
    of them and widening to 10 km adds only 1.1%. Flights starting above
    5,000 ft sit a median 46.4 km away, so the height test is what keeps
    overflights out. Height is measured above the airport's elevation, not
    above sea level, so high-altitude airports behave the same as sea-level
    ones.

WHAT THE DAILY NUMBERS ARE
    Counts of aircraft movements *observed by a volunteer ADS-B receiver
    network and attributed to an airport by proximity*. They are not official
    airport or airline statistics: no flight plan, airline schedule, airport
    AODB or regulatory filing is involved, and they will not reconcile with
    published movement counts. Every row carries
    ``metric_source = 'adsb_inferred'`` so the distinction survives into
    whatever BI tool reads the table.

LIMITATIONS
    * Only large and medium airports are candidates. Movements at small strips
      and heliports are not counted, and an aircraft on the ground at one may
      be attributed to a larger airport within 5 km.
    * A flight whose coverage begins or ends in mid-air near an airport can
      produce a spurious movement; the height test limits but does not
      eliminate this.
    * Flights that never move are excluded, which removes the fixed ground
      transmitters present in the source (readsb reports some with an
      ``aircraft_type`` of ``TWR``) as well as single-observation fragments.
      Those emitters sit *at* airports, so leaving them in would inflate
      exactly these counts.
    * A turnaround tracked continuously is one flight, so its intermediate
      landing and takeoff are invisible -- inherited from flight
      reconstruction and a source of undercounting.
    * ``hold_rate`` is per arrival, and is **not a delay metric**: the highest
      rates on this sample are at flight-training fields, where the circling
      being detected is circuit training.
"""

from __future__ import annotations

import csv
import io
import os
import urllib.request
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from adsb.delta_io import table_uri, write_delta

SOURCE_URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"
KEEP_TYPES = ("large_airport", "medium_airport")

DEFAULT_AIRPORTS_URI = os.environ.get(
    "ADSB_AIRPORTS_URI", "s3a://adsb/reference/airports.csv"
)
DEFAULT_AIRPORTS_KEY = "reference/airports.csv"
DEFAULT_MOVEMENTS_URI = os.environ.get("ADSB_MOVEMENTS_URI", table_uri("movements"))
DEFAULT_AIRPORT_OPERATIONS_URI = os.environ.get(
    "ADSB_AIRPORT_OPERATIONS_URI", table_uri("airport_daily_operations")
)

AIRPORT_SCHEMA = StructType(
    [
        StructField("ident", StringType()),  # ICAO-style identifier, e.g. LSZH
        StructField("iata_code", StringType()),
        StructField("name", StringType()),
        StructField("type", StringType()),
        StructField("iso_country", StringType()),
        StructField("latitude_deg", DoubleType()),
        StructField("longitude_deg", DoubleType()),
        StructField("elevation_ft", DoubleType()),
    ]
)


def filter_airports(source_csv: str) -> str:
    """Reduce the full OurAirports export to the columns and rows we use."""
    reader = csv.DictReader(io.StringIO(source_csv))
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=[f.name for f in AIRPORT_SCHEMA])
    writer.writeheader()

    for row in reader:
        if row["type"] not in KEEP_TYPES:
            continue
        if not row["latitude_deg"] or not row["longitude_deg"]:
            continue
        writer.writerow(
            {
                "ident": row["ident"],
                "iata_code": row["iata_code"],
                "name": row["name"],
                "type": row["type"],
                "iso_country": row["iso_country"],
                "latitude_deg": row["latitude_deg"],
                "longitude_deg": row["longitude_deg"],
                # a handful of airports have no elevation; leave it empty
                "elevation_ft": row["elevation_ft"],
            }
        )
    return out.getvalue()


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
    -- a flight that never changes position is a fixed transmitter or a
    -- single stray observation, not an aircraft movement
    SELECT * FROM {{flights}}
    WHERE NOT (first_latitude = last_latitude AND first_longitude = last_longitude)
),
endpoints AS (
    SELECT flight_id, icao, aircraft_type, registered_owner, callsign,
           release_tag, release_date,
           'departure' AS movement_type,
           first_seen_time AS event_time, first_latitude AS latitude,
           first_longitude AS longitude, first_altitude_ft AS altitude_ft
    FROM moving
    UNION ALL
    SELECT flight_id, icao, aircraft_type, registered_owner, callsign,
           release_tag, release_date,
           'arrival',
           last_seen_time, last_latitude, last_longitude, last_altitude_ft
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
        PARTITION BY flight_id, movement_type
        ORDER BY distance_km, ident  -- ident breaks ties deterministically
    ) AS airport_rank
    FROM qualifying
)
SELECT
    flight_id, icao, aircraft_type, registered_owner, callsign,
    movement_type, event_time, latitude, longitude, altitude_ft,
    ident, iata_code, airport_name, airport_type, iso_country,
    latitude_deg, longitude_deg, elevation_ft, distance_km,
    release_tag, release_date
FROM nearest WHERE airport_rank = 1
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
    MAX(release_date)                                           AS release_date,
    'adsb_inferred'                                             AS metric_source
FROM {movements}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
"""

# Holds are attributed to the airport the flight was inferred to be arriving
# at, so hold_rate is per arrival. It is null when there were no arrivals to
# divide by rather than silently zero.
_HOLD_METRICS_SQL = """
SELECT
    DATE(hold_start)                AS operations_date,
    arrival_airport_ident           AS airport_ident,
    COUNT(DISTINCT flight_id)       AS flights_with_detected_holds,
    ROUND(AVG(duration_seconds))    AS avg_hold_duration_seconds
FROM {holds}
WHERE arrival_airport_ident IS NOT NULL
GROUP BY 1, 2
"""

_METRICS_WITH_HOLDS_SQL = """
SELECT
    m.*,
    COALESCE(h.flights_with_detected_holds, 0)  AS flights_with_detected_holds,
    CASE WHEN m.arrivals > 0 THEN
        ROUND(COALESCE(h.flights_with_detected_holds, 0) / m.arrivals, 4)
    END                                         AS hold_rate,
    h.avg_hold_duration_seconds
FROM {metrics} m
LEFT JOIN {hold_metrics} h
       ON m.operations_date = h.operations_date
      AND m.airport_ident   = h.airport_ident
"""


def airport_movements(
    flights: DataFrame,
    airports: DataFrame,
    radius_km: float = MATCH_RADIUS_KM,
    max_height_ft: float = MAX_HEIGHT_ABOVE_AIRPORT_FT,
) -> DataFrame:
    """One row per inferred movement: a flight endpoint matched to an airport."""
    spark = flights.sparkSession
    flights.createOrReplaceTempView("flights_for_movements")
    airports.createOrReplaceTempView("airports")
    spark.sql(_AIRPORT_CELLS_SQL.format(airports="airports")).createOrReplaceTempView(
        "airport_cells"
    )
    return spark.sql(
        _MOVEMENTS_SQL.format(
            flights="flights_for_movements",
            airport_cells="airport_cells",
            radius_km=radius_km,
            max_height_ft=max_height_ft,
        )
    )


def to_airport_operations(
    movements: DataFrame, holds: DataFrame | None = None
) -> DataFrame:
    """Daily operations per airport, with hold metrics when holds are supplied."""
    spark = movements.sparkSession
    movements.createOrReplaceTempView("airport_movements")
    metrics = spark.sql(_METRICS_SQL.format(movements="airport_movements"))
    if holds is None:
        return metrics

    holds.createOrReplaceTempView("airport_holds")
    spark.sql(
        _HOLD_METRICS_SQL.format(holds="airport_holds")
    ).createOrReplaceTempView("airport_hold_metrics")
    metrics.createOrReplaceTempView("airport_metrics_base")
    return spark.sql(
        _METRICS_WITH_HOLDS_SQL.format(
            metrics="airport_metrics_base", hold_metrics="airport_hold_metrics"
        )
    )


def write_movements(
    df: DataFrame,
    path: str,
    release_date: str | None = None,
    full_rebuild: bool = False,
) -> None:
    write_delta(df, path, release_date=release_date, full_rebuild=full_rebuild)


def read_movements(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def write_airport_operations(
    df: DataFrame,
    path: str,
    release_date: str | None = None,
    full_rebuild: bool = False,
) -> None:
    write_delta(df, path, release_date=release_date, full_rebuild=full_rebuild)


def read_airport_operations(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def read_airports(spark: SparkSession, path: str = DEFAULT_AIRPORTS_URI) -> DataFrame:
    return spark.read.schema(AIRPORT_SCHEMA).option("header", "true").csv(path)


def main(argv: list[str] | None = None) -> None:
    """Refresh the airport reference file. The tables are built by the pipeline."""
    import argparse

    from adsb.upload_raw import DEFAULT_BUCKET, build_client, ensure_bucket

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--key", default=DEFAULT_AIRPORTS_KEY)
    parser.add_argument("--local", type=Path, default=None,
                        help="also write the filtered csv here")
    args = parser.parse_args(argv)

    print(f"Downloading {SOURCE_URL}")
    with urllib.request.urlopen(SOURCE_URL) as response:
        source = response.read().decode("utf-8")

    filtered = filter_airports(source)
    rows = filtered.count("\n") - 1

    if args.local:
        args.local.parent.mkdir(parents=True, exist_ok=True)
        args.local.write_text(filtered)

    client = build_client()
    ensure_bucket(client, args.bucket)
    client.put_object(Bucket=args.bucket, Key=args.key, Body=filtered.encode("utf-8"))
    print(f"Wrote {rows:,} airports to s3a://{args.bucket}/{args.key}")


if __name__ == "__main__":
    main()
