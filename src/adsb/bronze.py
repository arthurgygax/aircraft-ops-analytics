"""Write raw ADS-B observations to a Bronze Delta table.

    s3a://<bucket>/raw/...  ->  Spark  ->  s3a://<bucket>/bronze/observations

Bronze stays close to the source: one row per observation exactly as readsb
recorded it, nothing dropped and nothing corrected. The 1800-knot light
aircraft in the sample is preserved on purpose -- deciding it is wrong is data
quality's job, not Bronze's.

The only normalization is technical, and only what a reliable table demands:

* the positional trace arrays are decoded into named, typed columns, because
  ``array<array<string>>`` is not something you can query or evolve;
* ``event_time`` is materialized from the day-start epoch plus each point's
  offset, since the raw offset is meaningless without its file's header;
* altitude's ``number | "ground"`` union is split into a numeric
  ``altitude_ft`` and a boolean ``on_ground``, because a column has one type.

Lineage columns (``source_file``, ``release_tag``, ``ingested_at``) are added
so a row can be traced back to the object it came from.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from adsb.spark_explore import DEFAULT_RAW_URI, build_session, read_aircraft, typed_points

DEFAULT_BRONZE_URI = os.environ.get(
    "ADSB_BRONZE_URI", "s3a://adsb/bronze/observations"
)


def to_bronze(aircraft: DataFrame) -> DataFrame:
    """Decode the traces and stamp each row with where it came from.

    ``_metadata.file_path`` is read on the aircraft rows, before the explode,
    and carried down as an ordinary column. That is deliberate:
    ``input_file_name()`` evaluated after an explode is fragile and can come
    back empty, whereas the metadata column is materialized by the scan.
    """
    source_file = F.col("_metadata.file_path")
    with_source = aircraft.withColumn("source_file", source_file).withColumn(
        # .../<release-tag>/traces/1c/x.json.gz -- anchored on the layout the
        # archive itself defines, not on the bucket prefix we happened to use
        "release_tag",
        F.regexp_extract(source_file, r"([^/]+)/traces/", 1),
    )
    return typed_points(with_source, keep=["source_file", "release_tag"]).withColumn(
        "ingested_at", F.current_timestamp()
    )


def write_bronze(bronze: DataFrame, path: str, mode: str = "overwrite") -> None:
    """Write the Delta table.

    No partitioning: the sample is a single day of ~300k rows, so partitioning
    by date would produce exactly one partition, and by aircraft would produce
    224 tiny files. Revisit when there is more than one day of data.
    """
    bronze.write.format("delta").mode(mode).save(path)


def read_bronze(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def main(argv: list[str] | None = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--raw", default=DEFAULT_RAW_URI)
    parser.add_argument("--bronze", default=DEFAULT_BRONZE_URI)
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])
    args = parser.parse_args(argv)

    spark = build_session("adsb-bronze")
    try:
        bronze = to_bronze(read_aircraft(spark, args.raw))

        print(f"Writing {args.bronze} (mode={args.mode})")
        write_bronze(bronze, args.bronze, args.mode)

        # read it back through Delta, not off the raw files
        table = read_bronze(spark, args.bronze)
        print(f"rows in Bronze: {table.count():,}")
        print("\n--- Bronze schema ---")
        table.printSchema()

        print("--- Delta transaction log ---")
        from delta.tables import DeltaTable

        DeltaTable.forPath(spark, args.bronze).history().select(
            "version", "timestamp", "operation", "operationMetrics"
        ).show(5, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
