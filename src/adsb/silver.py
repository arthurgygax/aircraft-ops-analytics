"""Clean Bronze observations into a Silver Delta table.

    s3a://<bucket>/bronze/observations  ->  s3a://<bucket>/silver/observations

Every transformation here answers a defect measured in the Bronze sample; see
the counts beside each one. Things the profile showed were already clean --
coordinate ranges, timestamp normalization, column types -- are deliberately
left alone rather than given no-op guards.

Deliberately NOT done here: physical plausibility per aircraft type (a Cessna
at 635 kt is still wrong, but catching that needs performance data and belongs
to the data quality phase), and anything resembling flight reconstruction.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession

from adsb.quality import assert_valid, report, validate_silver

DEFAULT_SILVER_URI = os.environ.get("ADSB_SILVER_URI", "s3a://adsb/silver/observations")

# Above any plausible ground speed for civil traffic: the fastest credible
# reports in the sample are B788s at 657 kt, and everything past 700 kt is a
# glitch (a DR400 light aircraft at 1800 kt, a helicopter at 944 kt).
MAX_GROUND_SPEED_KT = 700

# Civil aircraft do not sustain 20,000 fpm; one row exceeds this.
MAX_VERTICAL_RATE_FPM = 20000

_SILVER_SQL = f"""
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY icao, event_time
            -- same timestamp, two receptions a few dozen metres apart: keep
            -- the more complete row, then break ties on position so the
            -- choice is deterministic across runs
            ORDER BY
                ( CASE WHEN altitude_ft       IS NULL THEN 0 ELSE 1 END
                + CASE WHEN ground_speed_kt   IS NULL THEN 0 ELSE 1 END
                + CASE WHEN track_deg         IS NULL THEN 0 ELSE 1 END
                + CASE WHEN vertical_rate_fpm IS NULL THEN 0 ELSE 1 END
                + CASE WHEN callsign          IS NULL THEN 0 ELSE 1 END ) DESC,
                latitude, longitude
        ) AS row_rank
    FROM {{source}}
)
SELECT
    icao,
    -- readsb prefixes non-ICAO addresses (TIS-B, ADS-R relays) with '~'
    NOT icao LIKE '~%'                              AS is_icao_address,
    registration,
    aircraft_type,
    operator,
    event_time,
    latitude,
    longitude,
    on_ground,
    altitude_ft,
    CASE WHEN ground_speed_kt > {MAX_GROUND_SPEED_KT}
         THEN NULL ELSE ground_speed_kt END         AS ground_speed_kt,
    track_deg,
    CASE WHEN ABS(vertical_rate_fpm) > {MAX_VERTICAL_RATE_FPM}
         THEN NULL ELSE vertical_rate_fpm END       AS vertical_rate_fpm,
    -- a padded-empty callsign is a missing callsign, not a value
    NULLIF(TRIM(callsign), '')                      AS callsign,
    release_tag,
    ingested_at
FROM ranked
WHERE row_rank = 1
"""


def to_silver(bronze: DataFrame) -> DataFrame:
    """Deduplicate and normalize Bronze observations.

    Expressed in Spark SQL: this is column-wise cleaning plus a window
    function, which reads better as a query than as chained DataFrame calls.
    """
    view = "bronze_observations"
    bronze.createOrReplaceTempView(view)
    return bronze.sparkSession.sql(_SILVER_SQL.format(source=view))


def write_silver(silver: DataFrame, path: str, mode: str = "overwrite") -> None:
    """Unpartitioned, for the same reason Bronze is: one day of ~300k rows."""
    silver.write.format("delta").mode(mode).save(path)


def read_silver(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def main(argv: list[str] | None = None) -> None:
    import argparse

    from adsb.bronze import DEFAULT_BRONZE_URI, read_bronze
    from adsb.spark_explore import build_session

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bronze", default=DEFAULT_BRONZE_URI)
    parser.add_argument("--silver", default=DEFAULT_SILVER_URI)
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])
    args = parser.parse_args(argv)

    spark = build_session("adsb-silver")
    try:
        bronze = read_bronze(spark, args.bronze)
        silver = to_silver(bronze)

        bronze_rows = bronze.count()
        print(f"Writing {args.silver} (mode={args.mode})")
        write_silver(silver, args.silver, args.mode)

        table = read_silver(spark, args.silver)
        silver_rows = table.count()
        print(f"Bronze rows: {bronze_rows:,}")
        print(f"Silver rows: {silver_rows:,}  ({bronze_rows - silver_rows:,} removed)")

        print("\n--- Silver schema ---")
        table.printSchema()

        print("--- checks ---")
        table.createOrReplaceTempView("silver_observations")
        spark.sql(
            """
            SELECT
                COUNT(*)                                             AS rows,
                COUNT(*) - COUNT(DISTINCT icao, event_time)          AS duplicate_keys,
                SUM(CASE WHEN NOT is_icao_address THEN 1 ELSE 0 END) AS non_icao_rows,
                SUM(CASE WHEN callsign = '' THEN 1 ELSE 0 END)       AS empty_callsigns,
                MAX(ground_speed_kt)                                 AS max_ground_speed_kt
            FROM silver_observations
            """
        ).show(truncate=False)

        results = validate_silver(table)
        print(report("silver", results))
        assert_valid("silver", results)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
