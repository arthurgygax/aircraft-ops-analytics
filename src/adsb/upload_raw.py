"""Copy the local raw sample into object storage.

Ingestion writes to the local filesystem; this moves that output into an S3
bucket so Spark reads it over ``s3a://``. Written against the S3 API, so it
works unchanged against MinIO or AWS -- only the endpoint and credentials
differ.

Object keys mirror the local layout exactly, so the data is laid out the same
way in either place:

    data/raw/adsb/<tag>/traces/1c/x.json.gz  ->  s3a://<bucket>/raw/adsb/<tag>/traces/1c/x.json.gz
"""

from __future__ import annotations

import os
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from adsb.ingest import DEFAULT_DEST_ROOT

DEFAULT_BUCKET = os.environ.get("S3_BUCKET", "adsb")
DEFAULT_PREFIX = "raw/adsb"


def build_client():
    """S3 client. ``S3_ENDPOINT`` points it at MinIO; without it, at AWS."""
    endpoint = os.environ.get("S3_ENDPOINT")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY"),
        # MinIO serves buckets as a path, not as a DNS subdomain
        config=Config(s3={"addressing_style": "path"}) if endpoint else None,
    )


def object_key(local_path: Path, source_root: Path, prefix: str = DEFAULT_PREFIX) -> str:
    """Key for a local file, preserving its layout under ``prefix``."""
    relative = local_path.relative_to(source_root).as_posix()
    return f"{prefix}/{relative}"


def ensure_bucket(client, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError:
        client.create_bucket(Bucket=bucket)


def upload_raw(
    client,
    source_root: Path = DEFAULT_DEST_ROOT,
    bucket: str = DEFAULT_BUCKET,
    prefix: str = DEFAULT_PREFIX,
) -> list[str]:
    """Upload every file under ``source_root``. Returns the keys written."""
    source_root = Path(source_root)
    if not source_root.exists():
        raise FileNotFoundError(
            f"{source_root} does not exist -- run `python -m adsb.ingest` first"
        )

    ensure_bucket(client, bucket)

    keys = []
    for path in sorted(source_root.rglob("*")):
        if not path.is_file():
            continue
        key = object_key(path, source_root, prefix)
        client.upload_file(str(path), bucket, key)
        keys.append(key)
    return keys


def main(argv: list[str] | None = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source", type=Path, default=DEFAULT_DEST_ROOT)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--prefix", default=DEFAULT_PREFIX)
    args = parser.parse_args(argv)

    keys = upload_raw(build_client(), args.source, args.bucket, args.prefix)
    print(f"Uploaded {len(keys)} objects to s3a://{args.bucket}/{args.prefix}/")


if __name__ == "__main__":
    main()
