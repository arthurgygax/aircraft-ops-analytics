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
