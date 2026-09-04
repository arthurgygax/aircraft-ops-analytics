"""Data quality checks for the ADS-B pipeline.

No framework: a check is a SQL predicate that matches *invalid* rows, and a
table's checks are counted in a single pass. That is enough for the invariants
this pipeline actually needs, and it keeps the rules readable next to the
transformations they protect.

The rules were chosen from failure modes this pipeline has really shown, not
from a generic checklist:

* **Silent emptiness.** A mistyped URI or an empty bucket produces zero rows,
  and every downstream table then builds successfully and empty. Nothing else
  in the pipeline notices, so every table asserts it is non-empty.
* **Silent string corruption.** ``release_tag`` is filled by a regular
  expression; an earlier version of it quietly produced ``''`` for input from
  an unexpected path. Empty-string checks exist because that actually
  happened.
* **Invariants that hold by construction and would otherwise go unverified.**
  Silver guarantees one row per ``(icao, event_time)``; segments must not end
  before they start; every observation must land in exactly one segment;
  ``arrivals + departures`` must equal ``total_operations``.
* **Coordinate ranges.** Silver deliberately does not *correct* coordinates,
  because profiling found none out of range. That is a statement about one
  release, so it is asserted rather than assumed: bad input fails loudly
  instead of being silently reshaped.

Layer rules differ on purpose. Bronze keeps the source's implausible values
(1800-knot light aircraft and all), so it is only checked for structural
integrity. Silver promises those values are gone, so it is checked for them.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

from pyspark.sql import DataFrame

# Each entry is "invariant name" -> "SQL predicate matching rows that BREAK it".
# NULL-safe by construction: `latitude < -90` is NULL for a NULL latitude, and
# CASE WHEN NULL counts as no violation, so range rules ignore missing values
# and the explicit `IS NULL` rules police required fields.

BRONZE_RULES: Mapping[str, str] = {
    "icao is present": "icao IS NULL OR icao = ''",
    "event_time is present": "event_time IS NULL",
    "latitude within [-90, 90]": "latitude < -90 OR latitude > 90",
    "longitude within [-180, 180]": "longitude < -180 OR longitude > 180",
    "source_file is recorded": "source_file IS NULL OR source_file = ''",
    "release_tag is recorded": "release_tag IS NULL OR release_tag = ''",
    "ingested_at is recorded": "ingested_at IS NULL",
}

SILVER_RULES: Mapping[str, str] = {
    "icao is present": "icao IS NULL OR icao = ''",
    "event_time is present": "event_time IS NULL",
    "latitude within [-90, 90]": "latitude < -90 OR latitude > 90",
    "longitude within [-180, 180]": "longitude < -180 OR longitude > 180",
    "is_icao_address is set": "is_icao_address IS NULL",
    # Silver's own promises, so a regression in the cleaning shows up here
    "implausible ground speed removed": "ground_speed_kt > 700",
    "implausible vertical rate removed": "ABS(vertical_rate_fpm) > 20000",
    "blank callsign normalized to NULL": "callsign = ''",
    "release_tag is recorded": "release_tag IS NULL OR release_tag = ''",
}

FLIGHT_SEGMENT_RULES: Mapping[str, str] = {
    "segment_id is present": "segment_id IS NULL OR segment_id = ''",
    "icao is present": "icao IS NULL OR icao = ''",
    "start_time is present": "start_time IS NULL",
    "end_time is present": "end_time IS NULL",
    "segment does not end before it starts": "end_time < start_time",
    "duration is not negative": "duration_seconds < 0",
    "segment has at least one observation": "n_observations < 1",
    "duration agrees with the timestamps":
        "duration_seconds <> UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time)",
    "start latitude within [-90, 90]": "start_latitude < -90 OR start_latitude > 90",
    "end latitude within [-90, 90]": "end_latitude < -90 OR end_latitude > 90",
    "start longitude within [-180, 180]":
        "start_longitude < -180 OR start_longitude > 180",
    "end longitude within [-180, 180]": "end_longitude < -180 OR end_longitude > 180",
}

FLIGHT_OBSERVATION_RULES: Mapping[str, str] = {
    "flight_id is present": "flight_id IS NULL OR flight_id = ''",
    "icao is present": "icao IS NULL OR icao = ''",
    "event_time is present": "event_time IS NULL",
    "position is present": "latitude IS NULL OR longitude IS NULL",
    "latitude within [-90, 90]": "latitude < -90 OR latitude > 90",
    "longitude within [-180, 180]": "longitude < -180 OR longitude > 180",
    "observation_seq starts at one": "observation_seq < 1",
    # (0,0) is in the Gulf of Guinea: a real fix there is vanishingly unlikely
    # and it is the classic signature of a bad position
    "position is not null island": "latitude = 0 AND longitude = 0",
    "flight_id starts with the aircraft address": "NOT flight_id LIKE CONCAT(icao, '_%')",
}

FLIGHT_RULES: Mapping[str, str] = {
    "flight_id is present": "flight_id IS NULL OR flight_id = ''",
    "icao is present": "icao IS NULL OR icao = ''",
    "flight does not end before it starts": "last_seen_time < first_seen_time",
    "duration is not negative": "duration_seconds < 0",
    "flight has at least one observation": "n_observations < 1",
    # an airline code is three letters or it is absent; never a registration
    "airline_icao is a three letter code":
        "airline_icao IS NOT NULL AND NOT airline_icao RLIKE '^[A-Z]{3}$'",
    "matched airports are within the search radius":
        "departure_distance_km > 50 OR arrival_distance_km > 50",
    "an airport match carries a time":
        "(departure_airport_ident IS NOT NULL AND departure_time IS NULL)"
        " OR (arrival_airport_ident IS NOT NULL AND arrival_time IS NULL)",
}

GOLD_RULES: Mapping[str, str] = {
    "operations_date is present": "operations_date IS NULL",
    "airport_ident is present": "airport_ident IS NULL OR airport_ident = ''",
    "arrivals and departures sum to total_operations":
        "arrivals + departures <> total_operations",
    "operations are not negative": "arrivals < 0 OR departures < 0",
    "an airport-day has at least one operation": "total_operations < 1",
    "unique_aircraft does not exceed operations":
        "unique_aircraft > total_operations",
    "unique_aircraft is at least one": "unique_aircraft < 1",
    "airport latitude within [-90, 90]":
        "airport_latitude < -90 OR airport_latitude > 90",
    "airport longitude within [-180, 180]":
        "airport_longitude < -180 OR airport_longitude > 180",
    "operations fall on the reported date":
        "DATE(first_operation_time) <> operations_date"
        " OR DATE(last_operation_time) <> operations_date",
    "last operation is not before the first":
        "last_operation_time < first_operation_time",
    # the inference label must survive into BI; see adsb.gold
    "every row is labelled as inferred": "metric_source <> 'adsb_inferred'",
}


class DataQualityError(AssertionError):
    """Raised when a table violates one of its invariants."""


@dataclass(frozen=True)
class CheckResult:
    check: str
    failures: int

    @property
    def passed(self) -> bool:
        return self.failures == 0

    def __str__(self) -> str:
        mark = "PASS" if self.passed else "FAIL"
        detail = "" if self.passed else f"  ({self.failures:,} rows)"
        return f"  [{mark}] {self.check}{detail}"


def check_rows(df: DataFrame, rules: Mapping[str, str]) -> list[CheckResult]:
    """Count violations of every rule in one pass over the table."""
    if not rules:
        return []
    names = list(rules)
    view = "quality_target"
    df.createOrReplaceTempView(view)
    counts = ", ".join(
        f"SUM(CASE WHEN ({rules[name]}) THEN 1 ELSE 0 END) AS violation_{i}"
        for i, name in enumerate(names)
    )
    row = df.sparkSession.sql(f"SELECT {counts} FROM {view}").first()
    return [CheckResult(name, int(row[i] or 0)) for i, name in enumerate(names)]


def check_not_empty(df: DataFrame) -> CheckResult:
    """A table that silently built empty is the failure mode this catches."""
    return CheckResult("table is not empty", 0 if df.take(1) else 1)


def check_unique(df: DataFrame, keys: Sequence[str]) -> CheckResult:
    """Count surplus rows beyond one per key combination."""
    total = df.count()
    distinct = df.select(*keys).distinct().count()
    return CheckResult(f"{' + '.join(keys)} is unique", total - distinct)


def check_observations_conserved(
    observations: DataFrame, segments: DataFrame
) -> CheckResult:
    """Every observation must land in exactly one segment -- no loss, no double count."""
    in_segments = segments.selectExpr("SUM(n_observations) AS total").first().total or 0
    return CheckResult(
        "every observation appears in exactly one segment",
        abs(observations.count() - int(in_segments)),
    )


def validate(
    name: str,
    df: DataFrame,
    rules: Mapping[str, str],
    unique_keys: Sequence[str] | None = None,
    extra: Sequence[CheckResult] = (),
) -> list[CheckResult]:
    results = [check_not_empty(df), *check_rows(df, rules)]
    if unique_keys:
        results.append(check_unique(df, unique_keys))
    results.extend(extra)
    return results


def report(name: str, results: Sequence[CheckResult]) -> str:
    failed = [r for r in results if not r.passed]
    header = (
        f"{name}: {len(results) - len(failed)}/{len(results)} checks passed"
        if failed
        else f"{name}: all {len(results)} checks passed"
    )
    return "\n".join([header, *(str(r) for r in results)])


def assert_valid(name: str, results: Sequence[CheckResult]) -> None:
    """Raise with every failing check named, not just the first."""
    failed = [r for r in results if not r.passed]
    if failed:
        detail = "\n".join(f"  - {r.check}: {r.failures:,} offending rows" for r in failed)
        raise DataQualityError(
            f"{name} failed {len(failed)} of {len(results)} data quality checks:\n{detail}"
        )


def validate_bronze(df: DataFrame) -> list[CheckResult]:
    return validate("bronze", df, BRONZE_RULES)


def validate_silver(df: DataFrame) -> list[CheckResult]:
    return validate("silver", df, SILVER_RULES, unique_keys=("icao", "event_time"))


def validate_flight_segments(df: DataFrame) -> list[CheckResult]:
    return validate(
        "flight_segments", df, FLIGHT_SEGMENT_RULES, unique_keys=("segment_id",)
    )


def validate_gold(df: DataFrame) -> list[CheckResult]:
    return validate(
        "gold", df, GOLD_RULES, unique_keys=("operations_date", "airport_ident")
    )


def validate_flight_observations(df: DataFrame) -> list[CheckResult]:
    return validate(
        "flight_observations",
        df,
        FLIGHT_OBSERVATION_RULES,
        unique_keys=("flight_id", "event_time"),
        # a unique sequence per flight is what makes "order by observation_seq"
        # a total order rather than an arbitrary one
        extra=[check_unique(df, ("flight_id", "observation_seq"))],
    )


def validate_flights(df: DataFrame) -> list[CheckResult]:
    return validate("flights", df, FLIGHT_RULES, unique_keys=("flight_id",))


def main(argv: list[str] | None = None) -> None:
    """Validate every published table. Exits non-zero if any check fails."""
    import argparse
    import sys

    from adsb.bronze import DEFAULT_BRONZE_URI, read_bronze
    from adsb.flight_model import (
        DEFAULT_FLIGHT_OBSERVATIONS_URI,
        DEFAULT_FLIGHTS_MODEL_URI,
        read_flight_observations,
        read_flights,
    )
    from adsb.flights import DEFAULT_FLIGHTS_URI, read_flight_segments
    from adsb.gold import DEFAULT_GOLD_URI, read_airport_metrics
    from adsb.silver import DEFAULT_SILVER_URI, read_silver
    from adsb.spark_explore import build_session

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bronze", default=DEFAULT_BRONZE_URI)
    parser.add_argument("--silver", default=DEFAULT_SILVER_URI)
    parser.add_argument("--flights", default=DEFAULT_FLIGHTS_URI)
    parser.add_argument("--gold", default=DEFAULT_GOLD_URI)
    parser.add_argument("--flight-observations", default=DEFAULT_FLIGHT_OBSERVATIONS_URI)
    parser.add_argument("--flights-model", default=DEFAULT_FLIGHTS_MODEL_URI)
    args = parser.parse_args(argv)

    spark = build_session("adsb-quality")
    failures = 0
    try:
        silver = read_silver(spark, args.silver)
        segments = read_flight_segments(spark, args.flights)

        checks = [
            ("bronze", validate_bronze(read_bronze(spark, args.bronze))),
            ("silver", validate_silver(silver)),
            (
                "flight_segments",
                validate_flight_segments(segments)
                + [check_observations_conserved(silver, segments)],
            ),
            (
                "flight_observations",
                validate_flight_observations(
                    read_flight_observations(spark, args.flight_observations)
                ),
            ),
            ("flights", validate_flights(read_flights(spark, args.flights_model))),
            ("gold", validate_gold(read_airport_metrics(spark, args.gold))),
        ]

        for name, results in checks:
            print(report(name, results))
            print()
            failures += sum(1 for r in results if not r.passed)
    finally:
        spark.stop()

    if failures:
        print(f"FAILED: {failures} check(s) did not pass")
        sys.exit(1)
    print("All data quality checks passed.")


if __name__ == "__main__":
    main()
