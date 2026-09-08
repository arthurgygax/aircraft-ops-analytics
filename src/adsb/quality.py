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
  One observation per ``(icao, event_time)``; every observation lands in
  exactly one flight; ``arrivals + departures`` must equal
  ``total_operations``.
* **Coordinate ranges.** The pipeline deliberately does not *correct*
  coordinates, because profiling found none out of range. That is a statement
  about two releases, so it is asserted rather than assumed: bad input fails
  loudly instead of being silently reshaped.

``collisions_resolved`` is not a pass/fail rule but a published number: how
many same-timestamp receptions the decoder collapsed. It used to be visible
only as the difference between two row counts, and it is the tripwire that
would catch the same raw file entering the pipeline twice.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Each entry is "invariant name" -> "SQL predicate matching rows that BREAK it".
# NULL-safe by construction: `latitude < -90` is NULL for a NULL latitude, and
# CASE WHEN NULL counts as no violation, so range rules ignore missing values
# and the explicit `IS NULL` rules police required fields.

OBSERVATION_RULES: Mapping[str, str] = {
    "flight_id is present": "flight_id IS NULL OR flight_id = ''",
    "flight_id starts with the aircraft address":
        "NOT flight_id LIKE CONCAT(icao, '_%')",
    "icao is present": "icao IS NULL OR icao = ''",
    "is_icao_address is set": "is_icao_address IS NULL",
    "event_time is present": "event_time IS NULL",
    "observation_seq starts at one": "observation_seq < 1",
    "position is present": "latitude IS NULL OR longitude IS NULL",
    "latitude within [-90, 90]": "latitude < -90 OR latitude > 90",
    "longitude within [-180, 180]": "longitude < -180 OR longitude > 180",
    # (0,0) is in the Gulf of Guinea: a real fix there is vanishingly unlikely
    # and it is the classic signature of a bad position
    "position is not null island": "latitude = 0 AND longitude = 0",
    # the decoder's own promises, so a regression in the cleaning shows up here
    "implausible ground speed removed": "ground_speed_kt > 700",
    "implausible vertical rate removed": "ABS(vertical_rate_fpm) > 20000",
    "blank callsign normalized to NULL": "callsign = ''",
    "release_tag is recorded": "release_tag IS NULL OR release_tag = ''",
    "release_date is recorded": "release_date IS NULL",
}

MOVEMENT_RULES: Mapping[str, str] = {
    "flight_id is present": "flight_id IS NULL OR flight_id = ''",
    "movement_type is arrival or departure":
        "movement_type NOT IN ('arrival', 'departure')",
    "an airport is matched": "ident IS NULL OR ident = ''",
    "event_time is present": "event_time IS NULL",
    "the match is within the search radius": "distance_km > 50",
    "latitude within [-90, 90]": "latitude < -90 OR latitude > 90",
    "longitude within [-180, 180]": "longitude < -180 OR longitude > 180",
}

FLIGHT_RULES: Mapping[str, str] = {
    "flight_id is present": "flight_id IS NULL OR flight_id = ''",
    "flight_date is present": "flight_date IS NULL",
    "icao is present": "icao IS NULL OR icao = ''",
    "flight does not end before it starts": "last_seen_time < first_seen_time",
    "duration is not negative": "duration_seconds < 0",
    "duration agrees with the timestamps":
        "duration_seconds <> UNIX_TIMESTAMP(last_seen_time)"
        " - UNIX_TIMESTAMP(first_seen_time)",
    "flight has at least one observation": "n_observations < 1",
    "flight_date matches the first observation":
        "flight_date <> DATE(first_seen_time)",
    "first latitude within [-90, 90]": "first_latitude < -90 OR first_latitude > 90",
    "last latitude within [-90, 90]": "last_latitude < -90 OR last_latitude > 90",
    "first longitude within [-180, 180]":
        "first_longitude < -180 OR first_longitude > 180",
    "last longitude within [-180, 180]":
        "last_longitude < -180 OR last_longitude > 180",
    # an airline code is three letters or it is absent; never a registration
    "airline_icao is a three letter code":
        "airline_icao IS NOT NULL AND NOT airline_icao RLIKE '^[A-Z]{3}$'",
    "matched airports are within the search radius":
        "departure_distance_km > 50 OR arrival_distance_km > 50",
    "an airport match carries a time":
        "(departure_airport_ident IS NOT NULL AND departure_time IS NULL)"
        " OR (arrival_airport_ident IS NOT NULL AND arrival_time IS NULL)",
    "hold rollups agree with each other":
        "(n_detected_holds > 0) <> has_detected_hold",
    "hold counts are not negative":
        "n_detected_holds < 0 OR total_hold_seconds < 0",
    "a flight with no holds has no hold time":
        "n_detected_holds = 0 AND total_hold_seconds <> 0",
}

FLIGHT_PHASE_RULES: Mapping[str, str] = {
    "flight_id is present": "flight_id IS NULL OR flight_id = ''",
    "phase is one of the known labels":
        "phase NOT IN ('taxi_out','climb','cruise','descent','taxi_in','taxi','unknown')",
    "start_time is present": "start_time IS NULL",
    "end_time is present": "end_time IS NULL",
    "phase does not end before it starts": "end_time < start_time",
    "duration is not negative": "duration_seconds < 0",
    "phase has at least one observation": "n_observations < 1",
    "duration agrees with the timestamps":
        "duration_seconds <> UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time)",
    "phase_seq starts at one": "phase_seq < 1",
    # a ground phase has no altitude, an airborne one should have some
    "taxi phases carry no altitude":
        "phase IN ('taxi_out','taxi_in','taxi') AND max_altitude_ft IS NOT NULL",
}

FLIGHT_HOLD_RULES: Mapping[str, str] = {
    "flight_id is present": "flight_id IS NULL OR flight_id = ''",
    "hold_start is present": "hold_start IS NULL",
    "hold_end is present": "hold_end IS NULL",
    "hold does not end before it starts": "hold_end < hold_start",
    "duration agrees with the timestamps":
        "duration_seconds <> UNIX_TIMESTAMP(hold_end) - UNIX_TIMESTAMP(hold_start)",
    "hold is sustained": "duration_seconds < 240",
    "hold is spatially confined": "span_km > 25",
    "hold completed at least one circuit": "circuits < 1",
    "hold is roughly level": "max_altitude_ft - min_altitude_ft > 4000",
    "centroid latitude within [-90, 90]":
        "centroid_latitude < -90 OR centroid_latitude > 90",
    "centroid longitude within [-180, 180]":
        "centroid_longitude < -180 OR centroid_longitude > 180",
    "hold_seq starts at one": "hold_seq < 1",
}

AIRPORT_OPERATION_RULES: Mapping[str, str] = {
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
    # the inference label must survive into BI; see adsb.airports
    "every row is labelled as inferred": "metric_source <> 'adsb_inferred'",
    "flights with holds do not exceed arrivals":
        "flights_with_detected_holds > arrivals",
    "hold_rate is a proportion": "hold_rate < 0 OR hold_rate > 1",
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
    """Count surplus rows beyond one per key combination.

    One Spark action, not two. Counting the rows and then counting the
    distinct keys separately scans the table twice, which on a 44M-row table
    measured at 44 s -- three quarters of a stage's whole validation cost. A
    single grouped aggregation gives the same number from one shuffle.
    """
    surplus = (
        df.groupBy(*keys)
        .count()
        .where(F.col("count") > 1)
        .agg(F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("surplus"))
        .first()
        .surplus
    )
    return CheckResult(f"{' + '.join(keys)} is unique", int(surplus))


def check_observations_conserved(
    observations: DataFrame, flights: DataFrame
) -> CheckResult:
    """Every observation lands in exactly one flight -- no loss, no double count."""
    in_flights = flights.selectExpr("SUM(n_observations) AS total").first().total or 0
    return CheckResult(
        "every observation appears in exactly one flight",
        abs(observations.count() - int(in_flights)),
    )


def check_references(
    child: DataFrame, parent: DataFrame, key: str = "flight_id"
) -> CheckResult:
    """Count child rows whose key is absent from the flight table.

    Everything is joined on flight_id alone, so an orphan here means a
    dashboard would show a phase, hold or track belonging to no flight.
    """
    orphans = (
        child.select(key).distinct().join(parent.select(key), key, "left_anti").count()
    )
    return CheckResult(f"every {key} exists in flights", orphans)


def collisions_resolved(aircraft: DataFrame, observations: DataFrame) -> tuple[int, int]:
    """(raw trace points, observations kept) for one release.

    The difference is how many same-timestamp receptions the decoder collapsed.
    Published rather than inferred: a sudden jump means either a change in the
    source's relay mix or the same raw file reaching the pipeline twice, and
    the second of those must not look like the first.
    """
    points = (
        aircraft.select(F.coalesce(F.size("trace"), F.lit(0)).alias("n"))
        .agg(F.sum("n").alias("total"))
        .first()
        .total
        or 0
    )
    return int(points), observations.count()


def validate(
    name: str,
    df: DataFrame,
    rules: Mapping[str, str],
    unique_keys: Sequence[str] | None = None,
    extra: Sequence[CheckResult] = (),
    allow_empty: bool = False,
) -> list[CheckResult]:
    """Run a table's rules. ``allow_empty`` for tables that legitimately can be.

    Emptiness is a defect for every table that has a row per flight or per
    observation, and only for those. See ``validate_flight_holds``.
    """
    results = [] if allow_empty else [check_not_empty(df)]
    results += check_rows(df, rules)
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


def validate_observations(df: DataFrame) -> list[CheckResult]:
    return validate(
        "observations",
        df,
        OBSERVATION_RULES,
        # the manufactured key: one row per reception instant per aircraft
        unique_keys=("icao", "event_time"),
        # a unique sequence per flight is what makes "order by observation_seq"
        # a total order rather than an arbitrary one
        extra=[check_unique(df, ("flight_id", "observation_seq"))],
    )


def validate_movements(df: DataFrame) -> list[CheckResult]:
    return validate(
        "movements", df, MOVEMENT_RULES, unique_keys=("flight_id", "movement_type")
    )


def validate_flights(df: DataFrame) -> list[CheckResult]:
    return validate("flights", df, FLIGHT_RULES, unique_keys=("flight_id",))


def validate_flight_phases(df: DataFrame) -> list[CheckResult]:
    return validate(
        "flight_phases",
        df,
        FLIGHT_PHASE_RULES,
        # one run per position in the flight: overlapping or repeated
        # sequence numbers would mean the interval collapse went wrong
        unique_keys=("flight_id", "phase_seq"),
    )


def validate_flight_holds(df: DataFrame) -> list[CheckResult]:
    """The one table allowed to be empty.

    Every other table has a row per flight or per observation, so building
    empty means something upstream failed. Holds are a *detection* result:
    "no aircraft circled at these airports today" is a measurement, not a
    defect. It is also a real outcome here: 2025-12-24 detected 23 holds and
    2025-12-25, the quietest traffic day of the European year, detected none.
    Under the previous global scope, at 4,393 holds a day, an empty table
    really would have meant something had broken.
    """
    return validate(
        "flight_holds",
        df,
        FLIGHT_HOLD_RULES,
        unique_keys=("flight_id", "hold_seq"),
        allow_empty=True,
    )


def validate_airport_operations(df: DataFrame) -> list[CheckResult]:
    return validate(
        "airport_daily_operations",
        df,
        AIRPORT_OPERATION_RULES,
        unique_keys=("operations_date", "airport_ident"),
    )


def main(argv: list[str] | None = None) -> None:
    """Validate every published table. Exits non-zero if any check fails."""
    import argparse
    import sys

    from adsb.airports import (
        DEFAULT_AIRPORT_OPERATIONS_URI,
        DEFAULT_MOVEMENTS_URI,
        read_airport_operations,
        read_movements,
    )
    from adsb.flights import DEFAULT_FLIGHTS_URI, read_flights
    from adsb.holds import DEFAULT_HOLDS_URI, read_flight_holds
    from adsb.observations import DEFAULT_OBSERVATIONS_URI, read_observations
    from adsb.phases import DEFAULT_PHASES_URI, read_flight_phases
    from adsb.spark_explore import build_session

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--observations", default=DEFAULT_OBSERVATIONS_URI)
    parser.add_argument("--flights", default=DEFAULT_FLIGHTS_URI)
    parser.add_argument("--movements", default=DEFAULT_MOVEMENTS_URI)
    parser.add_argument("--phases", default=DEFAULT_PHASES_URI)
    parser.add_argument("--holds", default=DEFAULT_HOLDS_URI)
    parser.add_argument("--airport-operations", default=DEFAULT_AIRPORT_OPERATIONS_URI)
    args = parser.parse_args(argv)

    spark = build_session("adsb-quality")
    failures = 0
    try:
        observations = read_observations(spark, args.observations)
        flights = read_flights(spark, args.flights)
        movements = read_movements(spark, args.movements)
        phases = read_flight_phases(spark, args.phases)
        holds = read_flight_holds(spark, args.holds)
        operations = read_airport_operations(spark, args.airport_operations)

        checks = [
            ("observations", validate_observations(observations)),
            (
                "flights",
                validate_flights(flights)
                + [check_observations_conserved(observations, flights)],
            ),
            ("movements", validate_movements(movements)),
            ("flight_phases", validate_flight_phases(phases)),
            ("flight_holds", validate_flight_holds(holds)),
            ("airport_daily_operations", validate_airport_operations(operations)),
            # everything is joined on flight_id alone, so every child table
            # must resolve against the flight table
            (
                "referential integrity",
                [
                    check_references(observations, flights),
                    check_references(movements, flights),
                    check_references(phases, flights),
                    check_references(holds, flights),
                ],
            ),
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
