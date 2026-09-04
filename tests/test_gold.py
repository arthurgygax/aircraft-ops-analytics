from datetime import date, datetime, timedelta

import pytest

pyspark = pytest.importorskip("pyspark", reason="Spark tests run in the spark container")

from adsb.gold import (  # noqa: E402
    airport_movements,
    read_airport_metrics,
    to_airport_metrics,
    write_airport_metrics,
)

from pyspark.sql.types import (  # noqa: E402
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _schema(strings=(), timestamps=(), doubles=(), dates=()):
    """Explicit schemas: an all-null altitude column defeats type inference."""
    return StructType(
        [StructField(n, StringType()) for n in strings]
        + [StructField(n, TimestampType()) for n in timestamps]
        + [StructField(n, DoubleType()) for n in doubles]
        + [StructField(n, DateType()) for n in dates]
    )


SEGMENT_SCHEMA = _schema(
    strings=("segment_id", "icao", "aircraft_type", "operator", "callsign", "release_tag"),
    timestamps=("start_time", "end_time"),
    doubles=("start_latitude", "start_longitude", "end_latitude", "end_longitude",
             "start_altitude_ft", "end_altitude_ft"),
    dates=("release_date",),
)

AIRPORT_SCHEMA = _schema(
    strings=("ident", "iata_code", "name", "type", "iso_country"),
    doubles=("latitude_deg", "longitude_deg", "elevation_ft"),
)

T0 = datetime(2025, 12, 30, 8, 0, 0)

# real coordinates so the haversine maths is exercised against known places
ZRH = ("LSZH", "ZRH", "Zurich Airport", "large_airport", "CH", 47.458056, 8.548056, 1417.0)
# ~9 km from ZRH: close enough to be a candidate, far enough to lose the tie
ZRH_NEAR = ("LSZX", "ZZZ", "Nearby Field", "medium_airport", "CH", 47.53, 8.60, 1400.0)
# Denver sits at 5,431 ft: the height test must be relative to that, not to sea level
DEN = ("KDEN", "DEN", "Denver International", "large_airport", "US", 39.861656, -104.673178, 5431.0)

ONE_KM_LAT = 0.009  # ~1 km of latitude


def segment(segment_id="s1", icao="a1b2c3", start=(47.458, 8.548), end=(47.458, 8.548),
            start_alt=None, end_alt=None, minutes=60):
    return (segment_id, icao, "A320", "SWISS", "SWR1", "v2025.12.30",
            T0, T0 + timedelta(minutes=minutes),
            start[0], start[1], end[0], end[1], start_alt, end_alt,
            date(2025, 12, 30))


@pytest.fixture
def make(spark):
    def _make(segments, airports=(ZRH,)):
        return (
            spark.createDataFrame(segments, SEGMENT_SCHEMA),
            spark.createDataFrame(list(airports), AIRPORT_SCHEMA),
        )

    return _make


def movements_of(segments, airports):
    return {
        (r.segment_id, r.movement_type): r
        for r in airport_movements(segments, airports).collect()
    }


def test_a_segment_leaving_an_airport_on_the_ground_is_a_departure(make):
    """Starts on the stand at ZRH, ends airborne far away."""
    segments, airports = make([
        segment(start=(47.458, 8.548), end=(46.0, 7.0), start_alt=None, end_alt=30000.0)
    ])

    movements = movements_of(segments, airports)

    assert ("s1", "departure") in movements
    assert ("s1", "arrival") not in movements
    assert movements[("s1", "departure")].ident == "LSZH"


def test_a_segment_arriving_low_over_an_airport_is_an_arrival(make):
    segments, airports = make([
        segment(start=(46.0, 7.0), end=(47.458, 8.548), start_alt=30000.0, end_alt=2000.0)
    ])

    movements = movements_of(segments, airports)

    assert ("s1", "arrival") in movements
    assert ("s1", "departure") not in movements


def test_an_overflight_at_cruise_is_not_a_movement(make):
    """Passing over the airport at altitude must not become a departure."""
    segments, airports = make([
        segment(start=(47.458, 8.548), end=(47.459, 8.549),
                start_alt=35000.0, end_alt=35000.0)
    ])

    assert airport_movements(segments, airports).count() == 0


def test_an_endpoint_beyond_the_radius_matches_nothing(make):
    segments, airports = make([
        segment(start=(47.458 + 50 * ONE_KM_LAT, 8.548), end=(46.0, 7.0), end_alt=30000.0)
    ])

    assert airport_movements(segments, airports).count() == 0


def test_a_segment_that_never_moves_is_excluded(make):
    """Fixed ground transmitters sit at airports and would inflate the counts."""
    segments, airports = make([
        segment(start=(47.458, 8.548), end=(47.458, 8.548))
    ])

    assert airport_movements(segments, airports).count() == 0


def test_the_nearest_airport_wins(make):
    segments, airports = make(
        [segment(start=(47.458, 8.548), end=(46.0, 7.0), end_alt=30000.0)],
        airports=(ZRH_NEAR, ZRH),
    )

    assert movements_of(segments, airports)[("s1", "departure")].ident == "LSZH"


def test_height_is_measured_above_the_airport_not_above_the_sea(make):
    """At Denver, 8,000 ft MSL is only 2,569 ft up -- still a departure."""
    segments, airports = make(
        [segment(start=(39.861656, -104.673178), end=(45.0, -100.0),
                 start_alt=8000.0, end_alt=35000.0)],
        airports=(DEN,),
    )

    assert movements_of(segments, airports)[("s1", "departure")].ident == "KDEN"


def test_daily_metrics_count_arrivals_departures_and_aircraft(make):
    segments, airports = make([
        # two different aircraft departing ZRH, one arriving
        segment("s1", "aaa111", start=(47.458, 8.548), end=(46.0, 7.0), end_alt=30000.0),
        segment("s2", "bbb222", start=(47.459, 8.549), end=(46.0, 7.0), end_alt=30000.0),
        segment("s3", "aaa111", start=(46.0, 7.0), end=(47.458, 8.548),
                start_alt=30000.0, end_alt=1500.0),
    ])

    metrics = to_airport_metrics(airport_movements(segments, airports)).collect()

    assert len(metrics) == 1
    row = metrics[0]
    assert (row.departures, row.arrivals, row.total_operations) == (2, 1, 3)
    assert row.unique_aircraft == 2, "aaa111 flew twice but is one aircraft"
    assert row.airport_ident == "LSZH"
    assert row.airport_iata == "ZRH"
    assert str(row.operations_date) == "2025-12-30"


def test_every_row_is_labelled_as_inferred(make):
    """The distinction from official statistics must survive into BI."""
    segments, airports = make([
        segment(start=(47.458, 8.548), end=(46.0, 7.0), end_alt=30000.0)
    ])

    metrics = to_airport_metrics(airport_movements(segments, airports)).first()

    assert metrics.metric_source == "adsb_inferred"


def test_airport_metrics_round_trip_through_delta(make, spark, tmp_path):
    path = str(tmp_path / "gold")
    segments, airports = make([
        segment(start=(47.458, 8.548), end=(46.0, 7.0), end_alt=30000.0)
    ])
    metrics = to_airport_metrics(airport_movements(segments, airports))

    write_airport_metrics(metrics, path)

    assert read_airport_metrics(spark, path).count() == 1
