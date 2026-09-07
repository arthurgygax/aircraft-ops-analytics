"""The Spark pipeline, writing local Parquet, for the benchmark.

Calls the same functions ``adsb.run_pipeline`` calls, in the same order, with
the same thresholds. The only difference is the sink: local Parquet instead of
Delta on MinIO, so the comparison measures the two engines rather than the two
storage layers -- pandas has no Delta writer, and giving Spark object storage
and pandas a local disk would measure the wrong thing.

No Spark configuration is set for the benchmark. The session is the project's
own ``build_session``.
"""

from __future__ import annotations

from pathlib import Path

from adsb.airports import airport_movements, read_airports, to_airport_operations
from adsb.flights import aggregate_flights, movement_pivot, to_flights
from adsb.holds import to_flight_holds
from adsb.observations import to_observations
from adsb.phases import to_flight_phases
from adsb.spark_explore import build_session, read_aircraft


def run(raw_dir: str, release_tag: str, airports_csv: str, out_dir: str) -> dict:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    spark = build_session("bench-spark")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        airports = read_airports(spark, f"file://{airports_csv}")
        observations = to_observations(read_aircraft(spark, raw_dir))
        observations.write.mode("overwrite").parquet(f"file://{out / 'observations'}")
        observations = spark.read.parquet(f"file://{out / 'observations'}")

        base = aggregate_flights(observations).cache()
        movements = airport_movements(base, airports)
        movements.write.mode("overwrite").parquet(f"file://{out / 'movements'}")
        movements = spark.read.parquet(f"file://{out / 'movements'}")
        pivot = movement_pivot(movements).cache()

        to_flight_phases(observations).write.mode("overwrite").parquet(
            f"file://{out / 'flight_phases'}"
        )
        to_flight_holds(observations, pivot, airports).write.mode("overwrite").parquet(
            f"file://{out / 'flight_holds'}"
        )
        holds = spark.read.parquet(f"file://{out / 'flight_holds'}")

        to_flights(base, pivot, holds).write.mode("overwrite").parquet(
            f"file://{out / 'flights'}"
        )
        to_airport_operations(movements, holds).write.mode("overwrite").parquet(
            f"file://{out / 'airport_daily_operations'}"
        )

        return {
            name: spark.read.parquet(f"file://{out / name}").count()
            for name in (
                "observations", "flights", "movements", "flight_phases",
                "flight_holds", "airport_daily_operations",
            )
        }
    finally:
        spark.stop()


def main(argv: list[str] | None = None) -> None:
    import argparse
    import json

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--raw", required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--airports", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)

    counts = run(args.raw, args.tag, args.airports, args.out)
    print("ROW_COUNTS " + json.dumps(counts))


if __name__ == "__main__":
    main()
