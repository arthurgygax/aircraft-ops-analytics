"""Airport reference data, from OurAirports.

Attributing an observed trajectory to an airport needs airport coordinates,
which ADS-B does not carry. OurAirports (https://ourairports.com/data/) is a
public-domain dataset; we keep only the large and medium airports, since small
strips and heliports would attract general-aviation noise without adding
commercial movements.

The filtered file is stored in object storage beside the pipeline's own data
so Spark reads reference and observation data the same way.
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

SOURCE_URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"
KEEP_TYPES = ("large_airport", "medium_airport")

DEFAULT_AIRPORTS_URI = os.environ.get(
    "ADSB_AIRPORTS_URI", "s3a://adsb/reference/airports.csv"
)
DEFAULT_AIRPORTS_KEY = "reference/airports.csv"

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
    -- a segment that never changes position is a fixed transmitter or a
    -- single stray observation, not an aircraft movement
    SELECT * FROM {{segments}}
    WHERE NOT (start_latitude = end_latitude AND start_longitude = end_longitude)
),
endpoints AS (
    SELECT segment_id, icao, aircraft_type, operator, callsign, release_tag,
           release_date,
           'departure' AS movement_type,
           start_time AS event_time, start_latitude AS latitude,
           start_longitude AS longitude, start_altitude_ft AS altitude_ft
    FROM moving
    UNION ALL
    SELECT segment_id, icao, aircraft_type, operator, callsign, release_tag,
           release_date,
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


def read_airports(spark: SparkSession, path: str = DEFAULT_AIRPORTS_URI) -> DataFrame:
    return spark.read.schema(AIRPORT_SCHEMA).option("header", "true").csv(path)


def main(argv: list[str] | None = None) -> None:
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
