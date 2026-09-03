"""Read the raw adsb.lol traces with PySpark and derive position reports.

Raw ingestion gives one gzipped JSON file per aircraft, each holding a whole
day's ``trace`` for that aircraft. This turns that into the shape ADS-B data is
actually analysed in -- one row per position report -- and runs a few
aggregations over it.

Two things about the source dictate the approach:

* A trace point is a *positional* array whose element types vary by position,
  and position 3 is a number **or** the string ``"ground"``. Spark has no type
  for a heterogeneous array, so ``trace`` is declared as
  ``array<array<string>>`` and each position is cast after exploding. Reading
  the points as strings keeps the mixed values intact instead of dropping
  whichever type loses.
* Position 8 is a nested object (present on only some points) carrying the
  callsign. Declared as a string, Spark hands back its raw JSON, which
  ``get_json_object`` can then query.

Spark runs in local mode. Nothing is written to disk: this phase proves the
data can be read and processed, and the storage layers come later.
"""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_RAW_PATH = PROJECT_ROOT / "data" / "raw" / "adsb"

# Where to read from: a local path, or an s3a:// URI when object storage is in
# play. Both are just paths to Spark, so there is one code path, not two.
DEFAULT_RAW_URI = os.environ.get("ADSB_RAW_URI", str(DEFAULT_RAW_PATH))

# Only the aircraft-level fields we care about; Spark drops the rest.
AIRCRAFT_SCHEMA = StructType(
    [
        StructField("icao", StringType()),
        StructField("r", StringType()),  # registration
        StructField("t", StringType()),  # ICAO type code
        StructField("desc", StringType()),
        StructField("ownOp", StringType()),  # operator
        StructField("timestamp", DoubleType()),  # epoch seconds, start of day
        StructField("trace", ArrayType(ArrayType(StringType()))),
    ]
)


def s3a_options(endpoint: str, access_key: str, secret_key: str) -> dict[str, str]:
    """Hadoop S3A settings for an S3-compatible endpoint.

    Written for MinIO, but the only MinIO-specific parts are the endpoint and
    path-style access. Point this at AWS by dropping ``endpoint`` and leaving
    the rest -- there is no second implementation for real S3.
    """
    return {
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.access.key": access_key,
        "spark.hadoop.fs.s3a.secret.key": secret_key,
        # MinIO serves buckets as a path, not as a DNS subdomain
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ),
    }


def build_session(app_name: str = "adsb-spark-explore") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        # Delta: the jars are baked into the image, so only the hooks are set
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    endpoint = os.environ.get("S3_ENDPOINT")
    if endpoint:
        options = s3a_options(
            endpoint,
            os.environ["S3_ACCESS_KEY"],
            os.environ["S3_SECRET_KEY"],
        )
        for key, value in options.items():
            builder = builder.config(key, value)

    return builder.getOrCreate()


def read_aircraft(spark: SparkSession, path: Path | str) -> DataFrame:
    """One row per aircraft, straight from the gzipped trace files."""
    return (
        spark.read.schema(AIRCRAFT_SCHEMA)
        # walk down to traces/<xx>/ whatever the release tag, and skip the
        # manifest.json sitting beside them
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.json.gz")
        # each file is a single JSON object, not JSON Lines
        .option("multiLine", "true")
        .json(str(path))
    )


def typed_points(aircraft: DataFrame, keep: list[str] | None = None) -> DataFrame:
    """Explode each aircraft's trace into one typed row per position report.

    Decoding only: every source observation survives, including ones with no
    position fix. Bronze needs that fidelity, so the filtering lives in
    ``position_reports`` below rather than here.

    ``keep`` names aircraft-level columns to carry through the explode, which
    is how Bronze attaches its lineage columns.
    """
    keep = keep or []
    point = F.col("point")
    return (
        aircraft.select(
            *keep,
            "icao",
            F.col("r").alias("registration"),
            F.col("t").alias("aircraft_type"),
            F.col("ownOp").alias("operator"),
            "timestamp",
            F.explode("trace").alias("point"),
        )
        .select(
            *keep,
            "icao",
            "registration",
            "aircraft_type",
            "operator",
            F.to_timestamp(F.col("timestamp") + point[0].cast(DoubleType())).alias(
                "event_time"
            ),
            point[1].cast(DoubleType()).alias("latitude"),
            point[2].cast(DoubleType()).alias("longitude"),
            # position 3 is feet, or the string "ground"
            (point[3] == F.lit("ground")).alias("on_ground"),
            F.when(point[3] != F.lit("ground"), point[3].cast(DoubleType())).alias(
                "altitude_ft"
            ),
            point[4].cast(DoubleType()).alias("ground_speed_kt"),
            point[5].cast(DoubleType()).alias("track_deg"),
            point[7].cast(DoubleType()).alias("vertical_rate_fpm"),
            # position 8 is a nested object on some points only
            F.trim(F.get_json_object(point[8], "$.flight")).alias("callsign"),
        )
    )


def position_reports(aircraft: DataFrame) -> DataFrame:
    """Typed points that actually carry a position."""
    return typed_points(aircraft).where(
        F.col("latitude").isNotNull() & F.col("longitude").isNotNull()
    )


def summarize(positions: DataFrame) -> None:
    """Aggregate in Spark; only the small result sets come back to Python."""
    print("\n--- position reports ---")
    print(f"rows: {positions.count():,}")
    positions.select(
        F.countDistinct("icao").alias("aircraft"),
        F.countDistinct("callsign").alias("callsigns"),
        F.min("event_time").alias("first_seen"),
        F.max("event_time").alias("last_seen"),
    ).show(truncate=False)

    print("--- airborne vs on ground ---")
    positions.groupBy("on_ground").agg(
        F.count("*").alias("reports"),
        F.round(F.avg("altitude_ft"), 0).alias("avg_altitude_ft"),
    ).orderBy("on_ground").show()

    print("--- busiest aircraft types ---")
    positions.groupBy("aircraft_type").agg(
        F.count("*").alias("reports"),
        F.countDistinct("icao").alias("aircraft"),
        F.round(F.max("altitude_ft"), 0).alias("max_altitude_ft"),
    ).orderBy(F.desc("reports")).show(10, truncate=False)

    print("--- fastest airborne reports ---")
    positions.where(~F.col("on_ground")).orderBy(
        F.desc("ground_speed_kt")
    ).select(
        "icao", "callsign", "aircraft_type", "event_time", "ground_speed_kt",
        "altitude_ft",
    ).show(5, truncate=False)


def main(argv: list[str] | None = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    # a plain string, not a Path: Path() would collapse the "//" in s3a:// URIs
    parser.add_argument("--path", default=DEFAULT_RAW_URI)
    args = parser.parse_args(argv)

    spark = build_session()
    try:
        aircraft = read_aircraft(spark, args.path)
        print(f"Reading {args.path}")
        print(f"aircraft files read: {aircraft.count():,}")
        print("\n--- source schema ---")
        aircraft.printSchema()

        positions = position_reports(aircraft)
        print("--- position report schema ---")
        positions.printSchema()

        summarize(positions)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
