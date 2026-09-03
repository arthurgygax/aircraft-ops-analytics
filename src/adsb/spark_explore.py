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


def build_session(app_name: str = "adsb-spark-explore") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


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


def position_reports(aircraft: DataFrame) -> DataFrame:
    """Explode each aircraft's trace into one typed row per position report."""
    point = F.col("point")
    return (
        aircraft.select(
            "icao",
            F.col("r").alias("registration"),
            F.col("t").alias("aircraft_type"),
            F.col("ownOp").alias("operator"),
            "timestamp",
            F.explode("trace").alias("point"),
        )
        .select(
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
        .where(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
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
    parser.add_argument("--path", type=Path, default=DEFAULT_RAW_PATH)
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
