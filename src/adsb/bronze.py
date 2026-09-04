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

from adsb.delta_io import write_delta
from adsb.ingest import release_date
from adsb.quality import assert_valid, report, validate_bronze
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
    return (
        typed_points(with_source, keep=["source_file", "release_tag"])
        .withColumn("ingested_at", F.current_timestamp())
        # partition key, taken from the release identity rather than from the
        # observations: see adsb.delta_io
        .withColumn(
            "release_date",
            F.to_date(
                F.regexp_extract(F.col("release_tag"), r"^v(\d{4}\.\d{2}\.\d{2})", 1),
                "yyyy.MM.dd",
            ),
        )
    )


def write_bronze(
    bronze: DataFrame,
    path: str,
    mode: str = "overwrite",
    release_date: str | None = None,
) -> None:
    """Write the Delta table, partitioned by day.

    Partitioning was deliberately skipped while the sample was a single day.
    Now that days arrive one at a time it earns its place: ``release_date``
    lets one day be replaced without touching the others.
    """
    write_delta(bronze, path, mode=mode, release_date=release_date)


def read_bronze(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def main(argv: list[str] | None = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--raw", default=DEFAULT_RAW_URI)
    parser.add_argument("--bronze", default=DEFAULT_BRONZE_URI)
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])
    parser.add_argument(
        "--tag",
        default=None,
        help="process one release incrementally, e.g. v2025.12.30-planes-readsb-prod-0",
    )
    args = parser.parse_args(argv)

    # one release is one day: reading just its prefix and replacing just its
    # partition is what makes a second day cheap and a repeat harmless
    raw_uri = args.raw
    day = None
    if args.tag:
        raw_uri = f"{args.raw.rstrip(chr(47))}/{args.tag}"
        day = release_date(args.tag)

    spark = build_session("adsb-bronze")
    try:
        bronze = to_bronze(read_aircraft(spark, raw_uri))

        scope = f"release_date={day}" if day else "all releases"
        print(f"Writing {args.bronze} (mode={args.mode}, {scope})")
        write_bronze(bronze, args.bronze, args.mode, release_date=day)

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

        results = validate_bronze(table)
        print(report("bronze", results))
        assert_valid("bronze", results)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
