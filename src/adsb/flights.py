"""The flight table: one row per inferred flight.

    <root>/observations + reference/airports + <root>/flight_holds
        ->  <root>/flights

Derived entirely from ``observations``, which already carries ``flight_id``:
the segmentation happened once, during decoding, so the trajectory and the
summary cannot disagree about where a flight starts.

WHAT THIS IS NOT
    These are not airline schedules or airport movement records. Nothing here
    comes from a flight plan, an AODB or an official source. A "flight" is a
    period during which one ADS-B transponder was tracked continuously by the
    adsb.lol receiver network -- an inference from radio observations, usually
    but not always corresponding to one real flight.

WHAT THE DATA SUPPORTS, MEASURED BEFORE THE ALGORITHM WAS CHOSEN
    * Tracking is dense while an aircraft is in coverage: the median gap
      between consecutive observations is 4 s and the 99th percentile is 35 s.
      Gaps meaning "left coverage" are easy to separate from normal cadence.
    * Ground coverage is poor -- only 43.9% of flights have any on-ground
      observation -- so takeoff and landing cannot be detected reliably from
      ``on_ground`` transitions. Time gaps are the robust signal; ground state
      is recorded as supporting evidence, not used for segmentation.
    * Callsigns are unreliable as a segmentation key: of 249 callsign changes
      in the development sample only 24 coincided with a gap over ten minutes,
      so splitting on them would invent boundaries mid-flight.

    The 15-minute threshold was measured, not guessed; see ``adsb.observations``
    for where the segmentation runs and ``docs/pipeline.md`` for the sweep.

    ``readsb`` sets its own "new leg" bit on trace points, which looks like a
    free replacement for the gap rule. It is not: the release carries 45,906
    leg-marked points against 75,561 gap breaks, and 92.8% of the markers
    follow a gap already longer than 900 s. It is a coarser subset of what the
    gap rule finds, so the gap rule stays.

FLIGHT ID
    ``<icao>_<yyyyMMddHHmmss of the first observation>``, e.g.
    ``a4b41c_20251230002514``. Deterministic and reproducible: a function of
    the data alone, so reprocessing a day regenerates identical ids. Both
    halves are needed -- the address repeats across the day's flights, and the
    timestamp alone is not unique across aircraft.

AUTHORITATIVE VS INFERRED
    Authoritative (transmitted by the aircraft): ``icao``, times, positions,
    the on-ground flag, and the altitude/speed/track/vertical-rate columns.

    Reference metadata (looked up by readsb from an aircraft database, not
    transmitted, so only as good as that database): ``registration`` (93% of
    flights), ``aircraft_type`` (92%), ``registered_owner`` (45%).

    Inferred here: ``flight_id`` and the segmentation behind it;
    ``airline_icao``, taken from an airline-style callsign (65%); departure and
    arrival airports, matched geographically (45% / 42%, both ends 26%).

KNOWN LIMITATIONS
    * A long coverage hole splits one real flight into two. Oceanic legs are
      the obvious case.
    * An aircraft tracked continuously through a turnaround yields one flight
      covering two real ones; ``n_callsigns`` exposes the signature.
    * Flights are clipped by the boundaries of the day being processed, so one
      crossing midnight is truncated.
    * A flight may be a single observation; ``n_observations`` is published so
      callers can decide what is usable rather than having that decided here.
    * ``registered_owner`` is the registry owner, NOT the operating airline --
      it is full of leasing trusts. Use ``airline_icao`` for airline questions.
    * ``first_seen_time``/``last_seen_time`` are when tracking started and
      stopped, which is not when the aircraft departed or arrived. Only
      ``departure_time``/``arrival_time`` mean that, and only when an airport
      was matched.
    * Non-ICAO (``~``) addresses are TIS-B/ADS-R relays that can shadow real
      aircraft. They are kept and flagged via ``is_icao_address``, not dropped.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession

from adsb.delta_io import table_uri, write_delta

DEFAULT_FLIGHTS_URI = os.environ.get("ADSB_FLIGHTS_URI", table_uri("flights"))

# An airline-style callsign is three letters then digits (SWR123, RYR4KL).
# Registration-style callsigns (N884GA, D-EABC) must not match: their first
# three characters are not an airline.
AIRLINE_CALLSIGN = r"^[A-Z]{3}[0-9][0-9A-Z]*$"

_AGGREGATE_SQL = """
SELECT
    flight_id,
    icao,
    MAX(is_icao_address)                        AS is_icao_address,
    MAX(registration)                           AS registration,
    MAX(aircraft_type)                          AS aircraft_type,
    -- the registry owner, which is frequently a leasing trust: not the airline
    MAX(operator)                               AS registered_owner,
    -- the callsign this flight reported first; flights spanning a turnaround
    -- report more than one, hence n_callsigns beside it
    MIN_BY(callsign, event_time)
        FILTER (WHERE callsign IS NOT NULL)     AS callsign,
    COUNT(DISTINCT callsign)                    AS n_callsigns,
    MIN(event_time)                             AS first_seen_time,
    MAX(event_time)                             AS last_seen_time,
    UNIX_TIMESTAMP(MAX(event_time))
        - UNIX_TIMESTAMP(MIN(event_time))       AS duration_seconds,
    COUNT(*)                                    AS n_observations,
    MIN_BY(latitude, event_time)                AS first_latitude,
    MIN_BY(longitude, event_time)               AS first_longitude,
    MAX_BY(latitude, event_time)                AS last_latitude,
    MAX_BY(longitude, event_time)               AS last_longitude,
    MIN_BY(on_ground, event_time)               AS started_on_ground,
    MAX_BY(on_ground, event_time)               AS ended_on_ground,
    -- altitude at each endpoint, not just the maximum: airport attribution
    -- needs to tell a real departure from a flight that merely started
    -- mid-cruise over an airport
    MIN_BY(altitude_ft, event_time)             AS first_altitude_ft,
    MAX_BY(altitude_ft, event_time)             AS last_altitude_ft,
    MAX(CASE WHEN on_ground THEN 1 ELSE 0 END) = 1 AS saw_ground,
    MAX(altitude_ft)                            AS max_altitude_ft,
    MAX(ground_speed_kt)                        AS max_ground_speed_kt,
    MAX(release_tag)                            AS release_tag,
    MAX(release_date)                           AS release_date
FROM {source}
GROUP BY flight_id, icao
"""

_MOVEMENT_PIVOT_SQL = """
SELECT
    flight_id,
    MAX(CASE WHEN movement_type = 'departure' THEN ident END)        AS departure_airport_ident,
    MAX(CASE WHEN movement_type = 'departure' THEN iata_code END)    AS departure_airport_iata,
    MAX(CASE WHEN movement_type = 'departure' THEN airport_name END) AS departure_airport_name,
    MAX(CASE WHEN movement_type = 'departure' THEN event_time END)   AS departure_time,
    MAX(CASE WHEN movement_type = 'departure' THEN distance_km END)  AS departure_distance_km,
    MAX(CASE WHEN movement_type = 'arrival'   THEN ident END)        AS arrival_airport_ident,
    MAX(CASE WHEN movement_type = 'arrival'   THEN iata_code END)    AS arrival_airport_iata,
    MAX(CASE WHEN movement_type = 'arrival'   THEN airport_name END) AS arrival_airport_name,
    MAX(CASE WHEN movement_type = 'arrival'   THEN event_time END)   AS arrival_time,
    MAX(CASE WHEN movement_type = 'arrival'   THEN distance_km END)  AS arrival_distance_km
FROM {movements}
GROUP BY flight_id
"""

# One flight table, not three. It carries what the flight model needs (airline,
# both airports) and what a dashboard needs to *find* interesting flights
# without scanning flight_holds first. The 44.6M point-level rows stay in
# observations, read straight from there by whoever draws a map.
_FLIGHTS_SQL = """
SELECT
    b.flight_id,
    -- the day the flight was observed, distinct from release_date which is the
    -- day of source data being processed; equal here, different in meaning
    DATE(b.first_seen_time)             AS flight_date,
    b.icao,
    b.is_icao_address,
    b.registration,
    b.aircraft_type,
    b.callsign,
    b.n_callsigns,
    CASE WHEN b.callsign RLIKE '{airline_pattern}'
         THEN SUBSTRING(b.callsign, 1, 3) END AS airline_icao,
    b.registered_owner,

    -- when tracking started and stopped, which is NOT departure and arrival
    b.first_seen_time,
    b.last_seen_time,
    b.duration_seconds,
    b.n_observations,

    b.first_latitude,
    b.first_longitude,
    b.last_latitude,
    b.last_longitude,
    b.first_altitude_ft,
    b.last_altitude_ft,
    b.started_on_ground,
    b.ended_on_ground,
    b.saw_ground,
    b.max_altitude_ft,
    b.max_ground_speed_kt,

    -- null whenever no airport matched, which is most of the time
    m.departure_airport_ident,
    m.departure_airport_iata,
    m.departure_airport_name,
    m.departure_time,
    m.departure_distance_km,
    m.arrival_airport_ident,
    m.arrival_airport_iata,
    m.arrival_airport_name,
    m.arrival_time,
    m.arrival_distance_km,

    COALESCE(h.n_detected_holds, 0)     AS n_detected_holds,
    COALESCE(h.n_detected_holds, 0) > 0 AS has_detected_hold,
    COALESCE(h.total_hold_seconds, 0)   AS total_hold_seconds,

    b.release_tag,
    b.release_date
FROM {base} b
LEFT JOIN {pivot} m ON b.flight_id = m.flight_id
LEFT JOIN (
    SELECT flight_id,
           COUNT(*)              AS n_detected_holds,
           SUM(duration_seconds) AS total_hold_seconds
    FROM {holds} GROUP BY flight_id
) h ON b.flight_id = h.flight_id
"""


def aggregate_flights(observations: DataFrame) -> DataFrame:
    """Collapse observations into one row per flight.

    The result is what airport matching runs against; it is not persisted on
    its own, because everything it holds survives into ``flights``.
    """
    view = "observations_for_flights"
    observations.createOrReplaceTempView(view)
    return observations.sparkSession.sql(_AGGREGATE_SQL.format(source=view))


def movement_pivot(movements: DataFrame) -> DataFrame:
    """Movements turned back into departure/arrival columns, one row per flight.

    Separate from ``to_flights`` because hold detection needs the same thing --
    a flight's inferred arrival airport -- and computing it once is the point
    of persisting movements at all.
    """
    view = "movements_for_pivot"
    movements.createOrReplaceTempView(view)
    return movements.sparkSession.sql(_MOVEMENT_PIVOT_SQL.format(movements=view))


def to_flights(base: DataFrame, pivot: DataFrame, holds: DataFrame) -> DataFrame:
    """The flight table: the aggregate, its airports, and its hold rollups."""
    spark = base.sparkSession
    pivot.createOrReplaceTempView("movement_pivot")
    base.createOrReplaceTempView("flight_base")
    holds.createOrReplaceTempView("holds_for_flights")
    return spark.sql(
        _FLIGHTS_SQL.format(
            base="flight_base",
            pivot="movement_pivot",
            holds="holds_for_flights",
            airline_pattern=AIRLINE_CALLSIGN,
        )
    )


def write_flights(
    df: DataFrame,
    path: str,
    release_date: str | None = None,
    full_rebuild: bool = False,
) -> None:
    write_delta(df, path, release_date=release_date, full_rebuild=full_rebuild)


def read_flights(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def main(argv: list[str] | None = None) -> None:
    import argparse

    from pyspark.sql import functions as F

    from adsb.airports import (
        DEFAULT_AIRPORTS_URI,
        DEFAULT_MOVEMENTS_URI,
        MATCH_RADIUS_KM,
        airport_movements,
        read_airports,
        read_movements,
        write_movements,
    )
    from adsb.holds import DEFAULT_HOLDS_URI, read_flight_holds
    from adsb.observations import DEFAULT_OBSERVATIONS_URI, read_observations
    from adsb.quality import assert_valid, report, validate_flights, validate_movements
    from adsb.spark_explore import build_session

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--observations", default=DEFAULT_OBSERVATIONS_URI)
    parser.add_argument("--airports", default=DEFAULT_AIRPORTS_URI)
    parser.add_argument("--movements", default=DEFAULT_MOVEMENTS_URI)
    parser.add_argument("--holds", default=DEFAULT_HOLDS_URI)
    parser.add_argument("--flights", default=DEFAULT_FLIGHTS_URI)
    parser.add_argument("--radius-km", type=float, default=MATCH_RADIUS_KM)
    parser.add_argument("--release-date", default=None, help="process one day only")
    parser.add_argument("--full-rebuild", action="store_true")
    args = parser.parse_args(argv)

    day = args.release_date
    if day is None and not args.full_rebuild:
        parser.error("pass --release-date, or --full-rebuild")

    spark = build_session("adsb-flights")
    try:
        observations = read_observations(spark, args.observations)
        holds = read_flight_holds(spark, args.holds)
        if day:
            observations = observations.where(F.col("release_date") == F.lit(day))
            holds = holds.where(F.col("release_date") == F.lit(day))

        base = aggregate_flights(observations)
        movements = airport_movements(
            base, read_airports(spark, args.airports), args.radius_km
        )
        print(f"Writing {args.movements}")
        write_movements(
            movements, args.movements, release_date=day, full_rebuild=args.full_rebuild
        )
        movements = read_movements(spark, args.movements)
        if day:
            movements = movements.where(F.col("release_date") == F.lit(day))

        print(f"Writing {args.flights}")
        write_flights(
            to_flights(base, movement_pivot(movements), holds),
            args.flights,
            release_date=day,
            full_rebuild=args.full_rebuild,
        )

        flights = read_flights(spark, args.flights)
        print(f"movements: {movements.count():,}")
        print(f"flights:   {flights.count():,}")

        print("\n--- schema ---")
        flights.printSchema()

        flights.createOrReplaceTempView("flights")
        print("--- coverage of the inferred attributes ---")
        spark.sql(
            """
            SELECT
                COUNT(*)                                                    AS flights,
                ROUND(100.0 * COUNT(airline_icao) / COUNT(*), 1)            AS pct_airline,
                ROUND(100.0 * COUNT(aircraft_type) / COUNT(*), 1)           AS pct_type,
                ROUND(100.0 * COUNT(departure_airport_ident) / COUNT(*), 1) AS pct_departure,
                ROUND(100.0 * COUNT(arrival_airport_ident) / COUNT(*), 1)   AS pct_arrival,
                SUM(CASE WHEN n_callsigns > 1 THEN 1 ELSE 0 END)            AS multi_callsign,
                SUM(CASE WHEN n_observations < 2 THEN 1 ELSE 0 END)         AS single_observation
            FROM flights
            """
        ).show(truncate=False)

        for name, results in (
            ("movements", validate_movements(movements)),
            ("flights", validate_flights(flights)),
        ):
            print(report(name, results))
            assert_valid(name, results)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
