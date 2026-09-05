import gzip
import json

import pytest

# NOT skipped at module scope: this file is also collected in the app
# container, which has no Spark. The skip belongs to the fixtures that need it.


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark", reason="Spark tests run in the spark container")
    from adsb.spark_explore import build_session

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
