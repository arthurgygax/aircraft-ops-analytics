from datetime import date, datetime, timedelta

import pytest

pyspark = pytest.importorskip("pyspark", reason="Spark tests run in the spark container")

from adsb.flights import (  # noqa: E402
    GAP_SECONDS,
    read_flight_segments,
    to_flight_segments,
    write_flight_segments,
)

SILVER_COLUMNS = (
    "icao is_icao_address registration aircraft_type operator event_time latitude "
    "longitude on_ground altitude_ft ground_speed_kt track_deg vertical_rate_fpm "
    "callsign release_tag release_date ingested_at"
).split()

START = datetime(2025, 12, 30, 8, 0, 0)
INGESTED = datetime(2026, 1, 1)
RELEASE_DATE = date(2025, 12, 30)


def obs(icao="a1b2c3", offset_s=0, lat=47.0, lon=8.0, callsign="SWR1",
        on_ground=False, alt=10000.0, gs=300.0):
    """One Silver observation, ``offset_s`` after START."""
    return (icao, True, "HB-ABC", "A320", "SWISS", START + timedelta(seconds=offset_s),
            lat, lon, on_ground, alt, gs, 90.0, 0.0, callsign, "v2025.12.30",
            RELEASE_DATE, INGESTED)


@pytest.fixture
def silver(spark):
    return lambda rows: spark.createDataFrame(rows, SILVER_COLUMNS)


def test_observations_within_the_gap_stay_one_segment(silver):
    df = silver([obs(offset_s=0), obs(offset_s=60), obs(offset_s=120)])

    segments = to_flight_segments(df).collect()

    assert len(segments) == 1
    assert segments[0].n_observations == 3
    assert segments[0].duration_seconds == 120


def test_a_gap_longer_than_the_threshold_starts_a_new_segment(silver):
    df = silver([obs(offset_s=0), obs(offset_s=GAP_SECONDS + 1)])

    assert to_flight_segments(df).count() == 2


def test_a_gap_exactly_at_the_threshold_does_not_split(silver):
    """The rule is strictly greater-than; pin the boundary."""
    df = silver([obs(offset_s=0), obs(offset_s=GAP_SECONDS)])

    assert to_flight_segments(df).count() == 1


def test_aircraft_are_segmented_independently(silver):
    """One aircraft's gap must not break another's segment."""
    df = silver([
        obs(icao="aaa111", offset_s=0),
        obs(icao="bbb222", offset_s=10),
        obs(icao="aaa111", offset_s=GAP_SECONDS + 30),
        obs(icao="bbb222", offset_s=60),
    ])

    counts = {r.icao: r.n_observations for r in to_flight_segments(df).collect()}

    assert to_flight_segments(df).count() == 3
    assert counts["bbb222"] == 2


def test_every_observation_lands_in_exactly_one_segment(silver):
    df = silver([obs(offset_s=o) for o in (0, 30, GAP_SECONDS + 60, GAP_SECONDS + 90)])

    total = sum(r.n_observations for r in to_flight_segments(df).collect())

    assert total == df.count()


def test_segment_endpoints_come_from_first_and_last_observation(silver):
    df = silver([
        obs(offset_s=0, lat=47.0, lon=8.0, on_ground=True),
        obs(offset_s=60, lat=48.0, lon=9.0),
        obs(offset_s=120, lat=49.0, lon=10.0, on_ground=False),
    ])

    segment = to_flight_segments(df).first()

    assert (segment.start_latitude, segment.start_longitude) == (47.0, 8.0)
    assert (segment.end_latitude, segment.end_longitude) == (49.0, 10.0)
    assert segment.started_on_ground is True
    assert segment.ended_on_ground is False
    assert segment.saw_ground is True
    # endpoint altitudes, which airport attribution needs
    assert segment.start_altitude_ft == 10000.0
    assert segment.end_altitude_ft == 10000.0


def test_callsign_is_the_first_reported_one_and_changes_are_counted(silver):
    """A segment spanning a turnaround keeps the first callsign but flags itself."""
    df = silver([
        obs(offset_s=0, callsign=None),
        obs(offset_s=60, callsign="SWR100"),
        obs(offset_s=120, callsign="SWR200"),
    ])

    segment = to_flight_segments(df).first()

    assert segment.callsign == "SWR100", "nulls skipped, earliest real callsign wins"
    assert segment.n_callsigns == 2


def test_a_lone_observation_is_still_a_segment(silver):
    segment = to_flight_segments(silver([obs()])).first()

    assert segment.n_observations == 1
    assert segment.duration_seconds == 0


def test_segment_id_is_derived_from_aircraft_and_start_time(silver):
    df = silver([obs(offset_s=0), obs(offset_s=60)])

    assert to_flight_segments(df).first().segment_id == "a1b2c3_20251230080000"


def test_flight_segments_round_trip_through_delta(silver, spark, tmp_path):
    path = str(tmp_path / "flights")
    segments = to_flight_segments(silver([obs(offset_s=0), obs(offset_s=GAP_SECONDS + 1)]))

    write_flight_segments(segments, path)

    assert read_flight_segments(spark, path).count() == 2
