"""The analytical scope: which days and which airports the project covers.

    seven consecutive UTC days  x  {LSZH Zurich, EDDL Dusseldorf}

Everything upstream of this module is global. adsb.lol publishes the whole
world and the pipeline decoded all of it, which was the right default while
the question was "does this pipeline work". It is the wrong one now that the
question is "how does traffic differ between Zurich and Dusseldorf". This
module is the single place that answers *which data is in the project*, so no
transformation below it has a date or an airport code written into it.

WHAT IS IN SCOPE, AS A RULE
    A flight is in scope when the pipeline infers that it departed from, or
    arrived at, one of the study airports. Its **whole** trajectory is then in
    scope: every observation of that flight, wherever the aircraft was.

    That distinction is the point. A Zurich departure to New York spends nine
    tenths of its trajectory outside Europe, and keeping only the points near
    the airport would leave a stub rather than a flight. So the filter is on
    flight identity, never on where an individual observation happens to be.

TWO FILTERS, AND WHY THERE HAVE TO BE TWO
    The rule can only be evaluated once flights exist, and flights only exist
    after every observation in the release has been decoded and segmented --
    which is nearly the whole cost of the pipeline, and 95% of it would be
    thrown away immediately afterwards.

    So a deliberately *conservative* filter runs first, on the raw aircraft
    rows before anything is exploded: keep an aircraft only if its trace passes
    within ``prefilter_radius_km`` of a study airport at some point that day.
    It cannot drop an in-scope flight, and that is a property rather than a
    hope. A movement is attributed only when a flight endpoint lies within
    ``airports.MATCH_RADIUS_KM`` of the airport; that endpoint is itself a
    trace point; so every in-scope flight has a point inside a radius five
    times smaller than this one. ``Scope`` refuses a radius that is not larger,
    rather than trusting the two constants to stay in step.

    It is a row-level ``exists`` over each aircraft's own trace array, so it
    shuffles nothing and runs before the explode. Measured on 2025-12-30:
    706 of 31,387 aircraft (2.25%), 2.06M observations instead of 44.6M.

    The second filter is the rule itself, applied to flight ids after airport
    matching. It removes aircraft that merely passed near Zurich, and the other
    legs those in-scope aircraft flew the same day.

WHY THIS MODULE IMPORTS NO PYSPARK
    ``adsb.ingest`` is standard library only, so a sample can be downloaded on
    the host without a container, and it needs the study period to know which
    releases to fetch. Nothing here needs a Spark import to work: the
    predicates are SQL strings and the joins are DataFrame methods. So the
    dependency stays out of the one module that must not have it.
"""

from __future__ import annotations

import math
import os
import re
from dataclasses import dataclass
from datetime import date, timedelta
from typing import TYPE_CHECKING, Iterable, Sequence

from adsb.ingest import release_tag

if TYPE_CHECKING:  # pragma: no cover - typing only, see the module docstring
    from pyspark.sql import DataFrame

# The study period: the last full week of the 2025 archive. It spans Christmas
# Eve through the following Tuesday, which is what makes "how does traffic vary
# by day" a question with an answer -- 25 December is the quietest day in the
# European calendar and the days either side of it are not.
STUDY_START = date(2025, 12, 24)
STUDY_END = date(2025, 12, 30)

# ICAO identifiers, because that is what the movement table keys on. ZRH and
# DUS are the IATA codes and appear in the published tables beside them.
STUDY_AIRPORTS = ("LSZH", "EDDL")

# Five times the movement match radius. Wide enough that the pre-filter cannot
# decide anything the movement rule would have decided differently, narrow
# enough to leave 2.25% of the day's aircraft.
PREFILTER_RADIUS_KM = 25.0

# One degree of latitude on the same 6,371 km sphere ``adsb.airports`` measures
# distances against. Deriving it rather than writing 111.32 keeps the box and
# the movement radius in the same units: on the rounder constant a "25 km" box
# reached 24.97 km, which a test caught and which would have been a silent
# disagreement between the two filters.
KM_PER_DEGREE = math.pi * 6371.0 / 180.0

# An ICAO airport identifier, and the character set the SQL predicates below
# are allowed to interpolate.
_IDENT = re.compile(r"^[A-Z0-9]{3,4}$")


@dataclass(frozen=True)
class Box:
    """A latitude/longitude rectangle around an airport, in plain degrees."""

    ident: str
    min_lat: float
    max_lat: float
    min_lon: float
    max_lon: float


@dataclass(frozen=True)
class Scope:
    """The analytical scope. Constructed once, passed down, never widened."""

    start_date: date = STUDY_START
    end_date: date = STUDY_END
    airports: tuple[str, ...] = STUDY_AIRPORTS
    prefilter_radius_km: float = PREFILTER_RADIUS_KM

    def __post_init__(self) -> None:
        if self.end_date < self.start_date:
            raise ValueError(
                f"scope ends before it starts: {self.start_date} .. {self.end_date}"
            )
        if not self.airports:
            raise ValueError("a scope with no airports selects no flights")
        for ident in self.airports:
            if not _IDENT.match(ident):
                raise ValueError(f"not an airport identifier: {ident!r}")
        if self.prefilter_radius_km <= 0:
            raise ValueError("the pre-filter radius must be positive")
        # The radius must also exceed the movement match radius, or the
        # pre-filter could drop a flight the movement rule would have matched.
        # That check lives in `airport_boxes`, where Spark is already a
        # dependency: reading the constant here would pull adsb.airports, and
        # with it pyspark, into a module adsb.ingest imports on the host.

    @property
    def days(self) -> tuple[date, ...]:
        span = (self.end_date - self.start_date).days + 1
        return tuple(self.start_date + timedelta(days=n) for n in range(span))

    @property
    def release_tags(self) -> tuple[str, ...]:
        """The adsb.lol releases covering the period, one per day."""
        return tuple(release_tag(day) for day in self.days)

    @property
    def releases(self) -> tuple[tuple[str, str], ...]:
        """``(tag, release_date)`` pairs, which is what the pipeline loops over."""
        return tuple((release_tag(day), day.isoformat()) for day in self.days)

    def describe(self) -> str:
        return (
            f"{self.start_date} .. {self.end_date} ({len(self.days)} days), "
            f"airports {'+'.join(self.airports)}, "
            f"pre-filter {self.prefilter_radius_km:g} km"
        )


def default_scope() -> Scope:
    """The project's scope, overridable by environment for a different study."""
    airports = os.environ.get("ADSB_SCOPE_AIRPORTS")
    return Scope(
        start_date=_env_date("ADSB_SCOPE_START", STUDY_START),
        end_date=_env_date("ADSB_SCOPE_END", STUDY_END),
        airports=parse_airports(airports) if airports else STUDY_AIRPORTS,
        prefilter_radius_km=float(
            os.environ.get("ADSB_SCOPE_RADIUS_KM", PREFILTER_RADIUS_KM)
        ),
    )


def parse_airports(value: str) -> tuple[str, ...]:
    return tuple(part.strip().upper() for part in value.split(",") if part.strip())


def _env_date(name: str, fallback: date) -> date:
    raw = os.environ.get(name)
    return date.fromisoformat(raw) if raw else fallback


def add_arguments(parser) -> None:
    """Put the scope on a command line. Defaults are the study period.

    Every flag is prefixed ``--scope-``, matching the ``ADSB_SCOPE_*``
    environment variables. A bare ``--airports`` would be ambiguous: the
    pipeline already has one, and it means the airport *reference file*.
    """
    scope = default_scope()
    parser.add_argument(
        "--scope-start", type=date.fromisoformat, default=scope.start_date,
        help="first UTC day of the study period",
    )
    parser.add_argument(
        "--scope-end", type=date.fromisoformat, default=scope.end_date,
        help="last UTC day of the study period, inclusive",
    )
    parser.add_argument(
        "--scope-airports", type=parse_airports, default=scope.airports,
        help="comma-separated ICAO identifiers, e.g. LSZH,EDDL",
    )
    parser.add_argument(
        "--scope-radius-km", type=float, default=scope.prefilter_radius_km,
        help="pre-filter radius; must exceed the movement match radius",
    )


def from_args(args) -> Scope:
    return Scope(
        start_date=args.scope_start,
        end_date=args.scope_end,
        airports=tuple(args.scope_airports),
        prefilter_radius_km=args.scope_radius_km,
    )


# --- the pre-filter ---------------------------------------------------------


def _in_list(column: str, values: Sequence[str]) -> str:
    # The values are identifiers validated by Scope, so interpolating them is
    # safe -- the same argument adsb.delta_io makes for its replaceWhere
    # predicate, and the same reason the validation is not optional.
    quoted = ", ".join(f"'{value}'" for value in values)
    return f"{column} IN ({quoted})"


def boxes_for(
    airports: Iterable[tuple[str, float, float]], radius_km: float
) -> list[Box]:
    """``(ident, lat, lon)`` -> the rectangle enclosing ``radius_km`` around it.

    Kept separate from the DataFrame lookup so the arithmetic is testable on
    its own, and so a box can be built for a place that is not in the reference
    file.
    """
    # Rounded outwards, once. A box that comes out a couple of centimetres
    # short of its own radius is harmless here -- the margin over the movement
    # radius is 20 km -- but a filter whose whole job is to not drop things
    # should be conservative by construction rather than by rounding.
    padded = radius_km * 1.001

    boxes = []
    for ident, latitude, longitude in airports:
        dlat = padded / KM_PER_DEGREE
        dlon = padded / (KM_PER_DEGREE * math.cos(math.radians(latitude)))
        box = Box(
            ident, latitude - dlat, latitude + dlat, longitude - dlon, longitude + dlon
        )
        if not (-180.0 <= box.min_lon and box.max_lon <= 180.0):
            # A box spanning the antimeridian is two boxes, and the predicate
            # below would silently match nothing. No study airport is near it;
            # fail loudly if one ever is.
            raise ValueError(f"{ident} lies too close to the antimeridian to box")
        boxes.append(box)
    return boxes


def airport_boxes(airports: "DataFrame", scope: Scope) -> list[Box]:
    """Look the study airports up in the reference table and box them.

    This collects to the driver, which is fine and is the only place in the
    pipeline that does: it is at most a handful of rows of reference data, and
    the result is a SQL string, not a dataset.
    """
    from adsb.airports import MATCH_RADIUS_KM

    if scope.prefilter_radius_km <= MATCH_RADIUS_KM:
        raise ValueError(
            f"pre-filter radius {scope.prefilter_radius_km} km must exceed the "
            f"movement match radius {MATCH_RADIUS_KM} km, or the pre-filter can "
            "drop flights the movement rule would have matched"
        )
    rows = {
        row["ident"]: row
        for row in airports.where(_in_list("ident", scope.airports)).collect()
    }
    missing = [ident for ident in scope.airports if ident not in rows]
    if missing:
        raise ValueError(
            f"airports {missing} are not in the reference data -- they may be "
            "outside the large/medium airport types it keeps"
        )
    return boxes_for(
        [
            (ident, rows[ident]["latitude_deg"], rows[ident]["longitude_deg"])
            for ident in scope.airports
        ],
        scope.prefilter_radius_km,
    )


def bbox_predicate(boxes: Sequence[Box], latitude: str, longitude: str) -> str:
    """A SQL predicate true where ``(latitude, longitude)`` is in any box."""
    if not boxes:
        raise ValueError("no boxes: the predicate would drop everything")
    return " OR ".join(
        f"({latitude} BETWEEN {b.min_lat} AND {b.max_lat}"
        f" AND {longitude} BETWEEN {b.min_lon} AND {b.max_lon})"
        for b in boxes
    )


def near_scope_predicate(boxes: Sequence[Box]) -> str:
    """True for an aircraft whose raw trace enters any of the boxes.

    Positions 1 and 2 of a trace point are latitude and longitude; see
    ``adsb.observations`` for the rest of the positional layout. ``exists``
    short-circuits, so an aircraft over Zurich is decided on its first points
    rather than its ten-thousandth.
    """
    inside = bbox_predicate(boxes, "CAST(p[1] AS DOUBLE)", "CAST(p[2] AS DOUBLE)")
    return f"exists(trace, p -> {inside})"


def near_scope(aircraft: "DataFrame", boxes: Sequence[Box]) -> "DataFrame":
    """Raw aircraft rows reduced to those that came near a study airport."""
    return aircraft.where(near_scope_predicate(boxes))


# --- the rule itself --------------------------------------------------------


def at_study_airports(movements: "DataFrame", scope: Scope) -> "DataFrame":
    """The movements the project reports on: arrivals and departures at ZRH/DUS.

    An in-scope flight has a movement at the far end too -- a Zurich departure
    arrives somewhere -- and that movement is real and is kept on the *flight*
    table, where it answers "where do Zurich departures go". It is dropped
    here, because a movements table containing one arrival at Heathrow would
    make the daily rollup say Heathrow saw one arrival that day.
    """
    return movements.where(_in_list("ident", scope.airports))


def scope_flight_ids(movements: "DataFrame", scope: Scope) -> "DataFrame":
    """Flights that departed from or arrived at a study airport.

    One column, ``flight_id``. Both movement types count, so a flight with
    either end at Zurich is in -- which is what "departures from ZRH, arrivals
    at ZRH" asks for.
    """
    return at_study_airports(movements, scope).select("flight_id").distinct()


def restrict_to_flights(df: "DataFrame", flight_ids: "DataFrame") -> "DataFrame":
    """Keep the rows of ``df`` belonging to those flights, and all of them.

    A semi-join rather than an inner join: it adds no columns and cannot
    duplicate a row if the id list ever gained one. The hint is safe at any
    plausible scope size -- a week of two airports is a few thousand ids.
    """
    return df.join(flight_ids.hint("broadcast"), "flight_id", "left_semi")
