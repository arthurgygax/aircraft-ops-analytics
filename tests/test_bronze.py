import pytest

pyspark = pytest.importorskip("pyspark", reason="Spark tests run in the spark container")

from adsb.bronze import read_bronze, to_bronze, write_bronze  # noqa: E402
from adsb.spark_explore import position_reports, read_aircraft  # noqa: E402


def test_bronze_keeps_observations_that_have_no_position(spark, raw_path):
    """Bronze stays faithful to the source; dropping rows is a later concern."""
    aircraft = read_aircraft(spark, raw_path)

    assert to_bronze(aircraft).count() == 4
    assert position_reports(aircraft).count() == 3


def test_bronze_stamps_each_row_with_its_source(spark, raw_path):
    rows = to_bronze(read_aircraft(spark, raw_path)).collect()

    assert all(r.source_file.endswith(".json.gz") for r in rows)
    assert {r.release_tag for r in rows} == {"v2025.12.30-planes-readsb-prod-0"}
    assert all(r.ingested_at is not None for r in rows)


def test_bronze_survives_a_delta_write_and_read(spark, raw_path, tmp_path):
    path = str(tmp_path / "bronze")
    bronze = to_bronze(read_aircraft(spark, raw_path))

    write_bronze(bronze, path)
    table = read_bronze(spark, path)

    assert table.count() == bronze.count()
    # names and types round trip; nullability does not -- Delta stores every
    # column as nullable, so ingested_at comes back nullable despite
    # current_timestamp() being non-nullable on the way in
    assert [(f.name, f.dataType) for f in table.schema] == [
        (f.name, f.dataType) for f in bronze.schema
    ]
    assert all(f.nullable for f in table.schema)
    assert (tmp_path / "bronze" / "_delta_log").is_dir(), "no Delta transaction log"
    # the "ground" sentinel survived the round trip as a typed pair of columns
    ground = table.where("on_ground").collect()
    assert len(ground) == 1
    assert ground[0].altitude_ft is None


def test_bronze_overwrite_adds_a_delta_version(spark, raw_path, tmp_path):
    from delta.tables import DeltaTable

    path = str(tmp_path / "bronze")
    bronze = to_bronze(read_aircraft(spark, raw_path))

    write_bronze(bronze, path)
    write_bronze(bronze, path)

    versions = [r.version for r in DeltaTable.forPath(spark, path).history().collect()]
    assert versions == [1, 0]
    # the old version is still readable
    assert spark.read.format("delta").option("versionAsOf", 0).load(path).count() == 4
