from pathlib import Path

from adsb.spark_explore import s3a_options
from adsb.upload_raw import object_key


def test_object_key_mirrors_the_local_layout():
    root = Path("/app/data/raw/adsb")
    local = root / "v2025.12.30-planes-readsb-prod-0" / "traces" / "1c" / "x.json.gz"

    key = object_key(local, root)

    assert key == "raw/adsb/v2025.12.30-planes-readsb-prod-0/traces/1c/x.json.gz"


def test_s3a_options_target_a_path_style_endpoint():
    """MinIO needs path-style access; AWS works by dropping the endpoint."""
    options = s3a_options("http://minio:9000", "key", "secret")

    assert options["spark.hadoop.fs.s3a.endpoint"] == "http://minio:9000"
    assert options["spark.hadoop.fs.s3a.path.style.access"] == "true"
    assert options["spark.hadoop.fs.s3a.access.key"] == "key"
    assert options["spark.hadoop.fs.s3a.secret.key"] == "secret"
