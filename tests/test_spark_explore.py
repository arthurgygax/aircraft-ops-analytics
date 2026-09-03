import pytest

pyspark = pytest.importorskip("pyspark", reason="Spark tests run in the spark container")

from adsb.spark_explore import position_reports, read_aircraft  # noqa: E402


def test_read_aircraft_decompresses_and_skips_the_manifest(spark, raw_path):
    aircraft = read_aircraft(spark, raw_path)

    assert aircraft.count() == 2, "the manifest.json must not become a row"
    assert sorted(r.icao for r in aircraft.select("icao").collect()) == [
        "7c6b1c",
        "a9e61c",
    ]
    # the trace survived as an array of arrays of strings
    assert dict(aircraft.dtypes)["trace"] == "array<array<string>>"


def test_position_reports_explodes_and_types_each_point(spark, raw_path):
    rows = {
        (r.icao, r.event_time.isoformat()): r
        for r in position_reports(read_aircraft(spark, raw_path)).collect()
    }

    # 4 trace points across 2 aircraft, minus the one with no position fix
    assert len(rows) == 3

    ground = rows[("7c6b1c", "2025-12-30T00:00:10")]
    assert ground.on_ground is True
    assert ground.altitude_ft is None, '"ground" is not an altitude'
    assert ground.callsign == "JST859", "callsign comes from the nested object"
    assert ground.aircraft_type == "A320"

    airborne = rows[("7c6b1c", "2025-12-30T01:00:00.500000")]
    assert airborne.on_ground is False
    assert airborne.altitude_ft == 27975.0
    assert airborne.ground_speed_kt == 434.1
    assert airborne.vertical_rate_fpm == 96.0
    assert airborne.callsign is None, "no nested object on this point"


def test_position_reports_drops_points_without_a_position(spark, raw_path):
    positions = position_reports(read_aircraft(spark, raw_path))

    assert positions.where("latitude is null or longitude is null").count() == 0
