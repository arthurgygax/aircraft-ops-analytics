"""The flight-level analytical model: one flight table, one trajectory table.

    silver/observations + reference/airports
        -> silver/flights              (one row per inferred flight)
        -> silver/flight_observations  (one row per position, tagged with flight_id)

Both come from a single segmentation pass (``adsb.flights.assign_flight_ids``),
so the trajectory and the summary can never disagree about where a flight
starts. The point-level table exists because a trajectory cannot be redrawn
from a summary: reducing to one row per flight would make the map impossible.

WHAT A FLIGHT IS HERE
    A period during which one transponder was tracked continuously, split when
    the gap between observations exceeds 15 minutes. That threshold was
    measured, not assumed; see ``adsb.flights`` for the calibration and the
    reasons ground state and callsign changes are deliberately *not* used to
    split. A "flight" is therefore an observation artefact, usually but not
    always corresponding to one real flight.

FLIGHT ID
    ``<icao>_<yyyyMMddHHmmss of the first observation>``, e.g.
    ``a4b41c_20251230002514``. Deterministic and reproducible: it is a function
    of the data alone, so reprocessing a day regenerates identical ids. Both
    halves are needed -- the address alone repeats across the day's flights,
    and the timestamp alone is not unique across aircraft. Not a UUID, and
    readable enough to grep for.

AUTHORITATIVE VS INFERRED FIELDS
    Authoritative (transmitted by the aircraft, 100% populated unless noted):
        icao, event_time, latitude, longitude, and the on_ground flag;
        altitude_ft (88%), ground_speed_kt (99%), track_deg (95%),
        vertical_rate_fpm (89%) -- gaps are genuinely absent transmissions.

    Reference metadata (looked up by readsb from an aircraft database, not
    transmitted, so it is only as good as that database):
        registration (93% of flights), aircraft_type (92%),
        registered_owner (45%).

    Inferred by this pipeline:
        flight_id and the segmentation behind it;
        airline_icao -- the ICAO airline designator taken from an airline-style
        callsign; 65% of flights;
        departure/arrival airports -- matched geographically.

THE TRACK DATASET AND ITS CONSUMERS
    ``flight_observations`` is the flight-track dataset; there is no separate
    ``flight_tracks`` copy, because measurement showed one would duplicate
    1.29 GB and exclude nothing. Across all 44.6M points there are no missing,
    out-of-range or null-island coordinates, no missing timestamps, and no
    duplicate ``(flight_id, event_time)``.

    Order it by ``flight_id, observation_seq``: that sequence is a gap-free
    1..n within each flight. Its window orders by ``event_time, latitude,
    longitude`` so the order stays reproducible even if two observations ever
    shared a timestamp -- Silver's uniqueness rule prevents that today, but the
    trajectory must not depend on which file Spark read first.

    Nothing is dropped. 305,735 points (0.7%) carry a position but no altitude,
    speed or track; they are still valid vertices and their columns are simply
    null. Invalid coordinates are asserted against in ``adsb.quality`` rather
    than silently repaired.

    Retrieving one flight's trajectory takes ~2 s against the 44.6M-row table,
    so it is deliberately left unpartitioned beyond ``release_date`` -- no
    Z-ordering, no clustering, no down-sampling.

    Expected readers: an interactive explorer (flight list from ``flights``,
    one or a few trajectories from ``flight_observations``); BI tools, for
    which the flight table is directly usable and the point table wants a day
    or airport filter first; and the phase- and holding-detection logic, which
    is why ``altitude_ft``, ``vertical_rate_fpm``, ``ground_speed_kt`` and
    ``track_deg`` are carried per point.

KNOWN LIMITATIONS
    * Airports resolve for 60.6% of flights, both ends for only 26.2%. The
      airport columns are nullable and frequently null; anything consuming them
      must say "unknown", not drop the flight.
    * ``registered_owner`` is the registry owner, NOT the operating airline. It
      is full of leasing trusts ("BANK OF UTAH TRUSTEE" appears on ~2000
      flights). Use ``airline_icao`` for airline questions.
    * ``airline_icao`` is a code, not a name. Resolving GS -> "Swiss" needs an
      airline reference dataset that this project deliberately does not ship.
    * ``first_seen_time``/``last_seen_time`` are when tracking started and
      stopped, which is not when the aircraft departed or arrived. Only
      ``departure_time``/``arrival_time`` mean that, and only when an airport
      was matched.
    * Everything inherited from reconstruction: coverage-hole splits,
      turnarounds merged into one flight, clipping at midnight.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession

from adsb.airports import MATCH_RADIUS_KM, airport_movements
from adsb.delta_io import write_delta
from adsb.flights import GAP_SECONDS, assign_flight_ids

DEFAULT_FLIGHT_OBSERVATIONS_URI = os.environ.get(
    "ADSB_FLIGHT_OBSERVATIONS_URI", "s3a://adsb/silver/flight_observations"
)
DEFAULT_FLIGHTS_MODEL_URI = os.environ.get(
    "ADSB_FLIGHTS_MODEL_URI", "s3a://adsb/silver/flights"
)

# An airline-style callsign is three letters then digits (SWR123, RYR4KL).
# Registration-style callsigns (N884GA, D-EABC) must not match: their first
# three characters are not an airline.
AIRLINE_CALLSIGN = r"^[A-Z]{3}[0-9][0-9A-Z]*$"

_OBSERVATIONS_SQL = f"""
SELECT
    flight_id,
    icao,
    event_time,
    observation_seq,
    latitude,
    longitude,
    altitude_ft,
    on_ground,
    ground_speed_kt,
    track_deg,
    vertical_rate_fpm,
    -- kept per point, not just per flight: it is how a callsign change part
    -- way through a flight stays visible
    callsign,
    release_date
FROM {{source}}
"""

_MOVEMENT_PIVOT_SQL = """
SELECT
    segment_id AS flight_id,
    MAX(CASE WHEN movement_type = 'departure' THEN ident END)       AS departure_airport_ident,
    MAX(CASE WHEN movement_type = 'departure' THEN iata_code END)   AS departure_airport_iata,
    MAX(CASE WHEN movement_type = 'departure' THEN airport_name END) AS departure_airport_name,
    MAX(CASE WHEN movement_type = 'departure' THEN event_time END)  AS departure_time,
    MAX(CASE WHEN movement_type = 'departure' THEN distance_km END) AS departure_distance_km,
    MAX(CASE WHEN movement_type = 'arrival'   THEN ident END)       AS arrival_airport_ident,
    MAX(CASE WHEN movement_type = 'arrival'   THEN iata_code END)   AS arrival_airport_iata,
    MAX(CASE WHEN movement_type = 'arrival'   THEN airport_name END) AS arrival_airport_name,
    MAX(CASE WHEN movement_type = 'arrival'   THEN event_time END)  AS arrival_time,
    MAX(CASE WHEN movement_type = 'arrival'   THEN distance_km END) AS arrival_distance_km
FROM {movements}
GROUP BY segment_id
"""

_FLIGHTS_SQL = """
SELECT
    s.segment_id                        AS flight_id,
    s.icao,
    s.is_icao_address,
    s.registration,
    s.aircraft_type,
    s.callsign,
    s.n_callsigns,
    CASE WHEN s.callsign RLIKE '{airline_pattern}'
         THEN SUBSTRING(s.callsign, 1, 3) END AS airline_icao,
    -- the registry owner, which is frequently a leasing trust: not the airline
    s.operator                          AS registered_owner,

    -- when tracking started and stopped, which is NOT departure and arrival
    s.start_time                        AS first_seen_time,
    s.end_time                          AS last_seen_time,
    s.duration_seconds,
    s.n_observations,

    s.start_latitude                    AS first_latitude,
    s.start_longitude                   AS first_longitude,
    s.end_latitude                      AS last_latitude,
    s.end_longitude                     AS last_longitude,
    s.start_altitude_ft                 AS first_altitude_ft,
    s.end_altitude_ft                   AS last_altitude_ft,
    s.started_on_ground,
    s.ended_on_ground,
    s.saw_ground,
    s.max_altitude_ft,
    s.max_ground_speed_kt,

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

    s.release_tag,
    s.release_date
FROM {segments} s
LEFT JOIN {movements} m ON s.segment_id = m.flight_id
"""


def to_flight_observations(
    observations: DataFrame, gap_seconds: int = GAP_SECONDS
) -> DataFrame:
    """Point-level trajectory rows, each tagged with its flight."""
    assigned = assign_flight_ids(observations, gap_seconds)
    view = "assigned_for_observations"
    assigned.createOrReplaceTempView(view)
    return assigned.sparkSession.sql(_OBSERVATIONS_SQL.format(source=view))


def to_flights(
    segments: DataFrame,
    airports: DataFrame,
    radius_km: float = MATCH_RADIUS_KM,
) -> DataFrame:
    """Flight-level rows enriched with airline code and matched airports."""
    spark = segments.sparkSession
    movements = airport_movements(segments, airports, radius_km)
    movements.createOrReplaceTempView("movements_for_flights")
    spark.sql(
        _MOVEMENT_PIVOT_SQL.format(movements="movements_for_flights")
    ).createOrReplaceTempView("movement_pivot")

    segments.createOrReplaceTempView("segments_for_flights")
    return spark.sql(
        _FLIGHTS_SQL.format(
            segments="segments_for_flights",
            movements="movement_pivot",
            airline_pattern=AIRLINE_CALLSIGN,
        )
    )


def write_flight_observations(
    df: DataFrame, path: str, mode: str = "overwrite", release_date: str | None = None
) -> None:
    write_delta(df, path, mode=mode, release_date=release_date)


def write_flights(
    df: DataFrame, path: str, mode: str = "overwrite", release_date: str | None = None
) -> None:
    write_delta(df, path, mode=mode, release_date=release_date)


def read_flight_observations(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def read_flights(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def main(argv: list[str] | None = None) -> None:
    import argparse

    from pyspark.sql import functions as F

    from adsb.airports import DEFAULT_AIRPORTS_URI, read_airports
    from adsb.flights import DEFAULT_FLIGHTS_URI, read_flight_segments
    from adsb.quality import (
        assert_valid,
        report,
        validate_flight_observations,
        validate_flights,
    )
    from adsb.silver import DEFAULT_SILVER_URI, read_silver
    from adsb.spark_explore import build_session

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--silver", default=DEFAULT_SILVER_URI)
    parser.add_argument("--segments", default=DEFAULT_FLIGHTS_URI)
    parser.add_argument("--airports", default=DEFAULT_AIRPORTS_URI)
    parser.add_argument("--flight-observations", default=DEFAULT_FLIGHT_OBSERVATIONS_URI)
    parser.add_argument("--flights", default=DEFAULT_FLIGHTS_MODEL_URI)
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])
    parser.add_argument(
        "--release-date",
        default=None,
        help="process one day only, replacing just that partition",
    )
    args = parser.parse_args(argv)

    spark = build_session("adsb-flight-model")
    try:
        observations = read_silver(spark, args.silver)
        segments = read_flight_segments(spark, args.segments)
        if args.release_date:
            day = F.lit(args.release_date)
            observations = observations.where(F.col("release_date") == day)
            segments = segments.where(F.col("release_date") == day)

        print(f"Writing {args.flight_observations}")
        write_flight_observations(
            to_flight_observations(observations),
            args.flight_observations,
            args.mode,
            release_date=args.release_date,
        )

        print(f"Writing {args.flights}")
        write_flights(
            to_flights(segments, read_airports(spark, args.airports)),
            args.flights,
            args.mode,
            release_date=args.release_date,
        )

        points = read_flight_observations(spark, args.flight_observations)
        flights = read_flights(spark, args.flights)
        print(f"flight_observations: {points.count():,}")
        print(f"flights:             {flights.count():,}")

        print("\n--- silver.flights schema ---")
        flights.printSchema()
        print("--- silver.flight_observations schema ---")
        points.printSchema()

        flights.createOrReplaceTempView("flights")
        print("--- coverage of the inferred attributes ---")
        spark.sql(
            """
            SELECT
                COUNT(*)                                                      AS flights,
                ROUND(100.0 * COUNT(airline_icao) / COUNT(*), 1)              AS pct_airline,
                ROUND(100.0 * COUNT(aircraft_type) / COUNT(*), 1)             AS pct_type,
                ROUND(100.0 * COUNT(departure_airport_ident) / COUNT(*), 1)   AS pct_departure,
                ROUND(100.0 * COUNT(arrival_airport_ident) / COUNT(*), 1)     AS pct_arrival,
                ROUND(100.0 * SUM(CASE WHEN departure_airport_ident IS NOT NULL
                                        AND arrival_airport_ident IS NOT NULL
                                       THEN 1 ELSE 0 END) / COUNT(*), 1)      AS pct_both
            FROM flights
            """
        ).show(truncate=False)

        for name, results in (
            ("flight_observations", validate_flight_observations(points)),
            ("flights", validate_flights(flights)),
        ):
            print(report(name, results))
            assert_valid(name, results)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
