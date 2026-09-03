import gzip
import json

import pytest

pyspark = pytest.importorskip("pyspark", reason="Spark tests run in the spark container")

from adsb.spark_explore import build_session, position_reports, read_aircraft  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    session = build_session("adsb-tests")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def raw_path(tmp_path_factory):
    """Two aircraft laid out the way ingestion writes them."""
    root = tmp_path_factory.mktemp("raw") / "v2025.12.30-planes-readsb-prod-0"
    traces = root / "traces" / "1c"
    traces.mkdir(parents=True)

    # a manifest sits beside the traces and must not be read as an aircraft
    (root / "manifest.json").write_text('{"release_tag": "v2025.12.30"}')

    airliner = {
        "icao": "7c6b1c",
        "r": "VH-VFY",
        "t": "A320",
        "ownOp": "JETSTAR",
        "timestamp": 1767052800.0,
        "trace": [
            # on the ground: altitude is the string "ground", callsign present
            [10.0, -31.9, 115.9, "ground", 0.0, None, 3, None,
             {"type": "adsb_icao", "flight": "JST859  "}, "adsb_icao",
             None, None, None, None],
            # airborne, no nested object on this point
            [3600.5, -28.8, 151.8, 27975, 434.1, 232.2, 0, 96, None,
             "adsb_icao", 29375, 0, 312, 0.2],
            # no position fix: must be filtered out
            [3700.0, None, None, 27000, 430.0, 230.0, 0, 0, None,
             "adsb_icao", None, None, None, None],
        ],
    }
    helicopter = {
        "icao": "a9e61c",
        "r": "N123AB",
        "t": "R44",
        "ownOp": "PRIVATE",
        "timestamp": 1767052800.0,
        "trace": [
            [20.0, 40.1, -74.2, 1200, 90.0, 180.0, 0, 200, None,
             "adsb_icao", 1300, 200, 85, 0.0],
        ],
    }
    for aircraft in (airliner, helicopter):
        path = traces / f"trace_full_{aircraft['icao']}.json.gz"
        path.write_bytes(gzip.compress(json.dumps(aircraft).encode()))

    return root.parent


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
