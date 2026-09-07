import pytest

pyspark = pytest.importorskip("pyspark", reason="Spark tests run in the spark container")

from adsb.spark_explore import read_aircraft  # noqa: E402


def test_read_aircraft_decompresses_and_skips_the_manifest(spark, raw_path):
    aircraft = read_aircraft(spark, raw_path)

    assert aircraft.count() == 2, "the manifest.json must not become a row"
    assert sorted(r.icao for r in aircraft.select("icao").collect()) == [
        "7c6b1c",
        "a9e61c",
    ]
    # the trace survived as an array of arrays of strings: reading the points
    # as strings is what keeps the mixed `number | "ground"` at position 3
    assert dict(aircraft.dtypes)["trace"] == "array<array<string>>"
