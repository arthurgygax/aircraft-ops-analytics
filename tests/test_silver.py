from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark", reason="Spark tests run in the spark container")

from adsb.silver import read_silver, to_silver, write_silver  # noqa: E402

BRONZE_COLUMNS = (
    "icao registration aircraft_type operator event_time latitude longitude "
    "on_ground altitude_ft ground_speed_kt track_deg vertical_rate_fpm callsign "
    "release_tag ingested_at"
).split()

T0 = datetime(2025, 12, 30, 10, 0, 0)
T1 = datetime(2025, 12, 30, 10, 0, 5)
INGESTED = datetime(2026, 1, 1, 0, 0, 0)


def row(icao="a1b2c3", event_time=T0, lat=47.4, lon=8.5, alt=1000.0, gs=250.0,
        track=90.0, vrate=500.0, callsign="SWR123", on_ground=False):
    return (icao, "HB-ABC", "A320", "SWISS", event_time, lat, lon, on_ground,
            alt, gs, track, vrate, callsign, "v2025.12.30", INGESTED)


@pytest.fixture
def bronze(spark):
    def _make(rows):
        return spark.createDataFrame(rows, BRONZE_COLUMNS)

    return _make


def test_duplicate_timestamps_collapse_to_the_most_complete_row(bronze, spark):
    """Two receptions of the same instant, metres apart -- keep one."""
    df = bronze([
        row(lat=47.400, gs=None, track=None, vrate=None, callsign=None),
        row(lat=47.401),  # every field populated
    ])

    silver = to_silver(df).collect()

    assert len(silver) == 1
    assert silver[0].latitude == 47.401
    assert silver[0].ground_speed_kt == 250.0


def test_deduplication_is_deterministic_when_completeness_ties(bronze):
    """Equally complete rows tie-break on position, not on read order."""
    rows = [row(lat=47.402), row(lat=47.401)]

    first = to_silver(bronze(rows)).collect()
    second = to_silver(bronze(list(reversed(rows)))).collect()

    assert len(first) == 1
    assert first[0].latitude == second[0].latitude == 47.401


def test_distinct_timestamps_are_all_kept(bronze):
    df = bronze([row(event_time=T0), row(event_time=T1)])

    assert to_silver(df).count() == 2


def test_non_icao_addresses_are_flagged_not_dropped(bronze):
    df = bronze([row(icao="a1b2c3"), row(icao="~ab12cd")])

    flags = {r.icao: r.is_icao_address for r in to_silver(df).collect()}

    assert flags == {"a1b2c3": True, "~ab12cd": False}


@pytest.mark.parametrize("callsign", ["", "   "])
def test_blank_callsigns_become_null(bronze, callsign):
    df = bronze([row(callsign=callsign)])

    assert to_silver(df).first().callsign is None


def test_implausible_speed_is_nulled_but_the_position_is_kept(bronze):
    """A bad speed must not cost us the position fix on that row."""
    df = bronze([row(gs=1800.0)])

    silver = to_silver(df).first()

    assert silver.ground_speed_kt is None
    assert silver.latitude == 47.4
    assert silver.altitude_ft == 1000.0


def test_plausible_speed_survives(bronze):
    """657 kt is a real B788 report in the sample; it must not be scrubbed."""
    assert to_silver(bronze([row(gs=657.0)])).first().ground_speed_kt == 657.0


def test_implausible_vertical_rate_is_nulled(bronze):
    df = bronze([row(vrate=-64000.0)])

    assert to_silver(df).first().vertical_rate_fpm is None


def test_silver_round_trips_through_delta(bronze, spark, tmp_path):
    path = str(tmp_path / "silver")
    silver = to_silver(bronze([row(event_time=T0), row(event_time=T1)]))

    write_silver(silver, path)

    assert read_silver(spark, path).count() == 2
