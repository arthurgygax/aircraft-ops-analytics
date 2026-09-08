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
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from adsb.ingest import DEFAULT_DEST_ROOT

DEFAULT_BUCKET = os.environ.get("S3_BUCKET", "adsb")
DEFAULT_PREFIX = "raw/adsb"

# Enough to hide the round trip, not so many that the connection pool below
# starts queueing; the pool is sized to match.
DEFAULT_WORKERS = 32


def build_client(workers: int = DEFAULT_WORKERS):
    """S3 client. ``S3_ENDPOINT`` points it at MinIO; without it, at AWS."""
    endpoint = os.environ.get("S3_ENDPOINT")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY"),
        config=Config(
            # botocore defaults to 10 connections; uploading on more threads
            # than that just makes them wait for each other
            max_pool_connections=max(workers, 10),
            # MinIO serves buckets as a path, not as a DNS subdomain
            **({"s3": {"addressing_style": "path"}} if endpoint else {}),
        ),
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
    tag: str | None = None,
    workers: int = DEFAULT_WORKERS,
) -> list[str]:
    """Upload files under ``source_root``. Returns the keys written.

    ``tag`` restricts the upload to one release, so adding a day does not
    re-send every day already uploaded. Keys stay relative to ``source_root``
    either way, keeping the layout identical.

    Uploaded in parallel because the study period is 224,652 trace files: each
    one is its own small PUT, the time goes on round trips rather than on
    bytes, and sequentially it measured 33 objects a second -- two hours for a
    9 GB sample that the network could carry in a few minutes. boto3 clients
    are documented as thread-safe for this.
    """
    source_root = Path(source_root)
    if not source_root.exists():
        raise FileNotFoundError(
            f"{source_root} does not exist -- run `python -m adsb.ingest` first"
        )

    ensure_bucket(client, bucket)

    root = source_root / tag if tag else source_root
    paths = [path for path in sorted(root.rglob("*")) if path.is_file()]
    keys = [object_key(path, source_root, prefix) for path in paths]

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(
            lambda pair: client.upload_file(str(pair[0]), bucket, pair[1]),
            zip(paths, keys),
        ))
    return keys


def main(argv: list[str] | None = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source", type=Path, default=DEFAULT_DEST_ROOT)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--prefix", default=DEFAULT_PREFIX)
    parser.add_argument("--tag", default=None, help="upload one release only")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    args = parser.parse_args(argv)

    keys = upload_raw(
        build_client(args.workers),
        args.source,
        args.bucket,
        args.prefix,
        args.tag,
        args.workers,
    )
    print(f"Uploaded {len(keys):,} objects to s3a://{args.bucket}/{args.prefix}/")


if __name__ == "__main__":
    main()
