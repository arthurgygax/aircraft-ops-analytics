"""Acquire a small, reproducible sample of raw adsb.lol globe_history data.

One daily release is ~3.2 GB, so we never download a whole one. The release
asset is an *uncompressed* tar split into 2 GB parts, which means a byte-range
request for a prefix of the first part yields whole, valid tar members.

The archive is laid out as::

    ./heatmap/*.bin.ttf     ~763 MB of binary replay files (not used here)
    ./README.txt, ./acas/, ./LICENSE-*
    ./traces/<xx>/trace_full_<icao>.json    gzipped readsb trace JSON

``./traces/`` therefore starts well past the point where a naive prefix
download would stop. We walk the tar header chain with 512-byte range reads to
find where it starts, then range-download a small window from there.

Files are written byte-for-byte as they appear in the archive: still gzipped,
still the original JSON, no parsing or cleaning.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import tarfile
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

REPO = "adsblol/globe_history_2025"
DEFAULT_TAG = "v2025.12.30-planes-readsb-prod-0"
DEFAULT_SAMPLE_BYTES = 8 * 1024 * 1024

TAR_BLOCK = 512
TRACES_PREFIX = "./traces/"

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_DEST_ROOT = PROJECT_ROOT / "data" / "raw" / "adsb"


RELEASE_TAG_DATE = re.compile(r"^v(\d{4})\.(\d{2})\.(\d{2})-")


def release_date(tag: str) -> str:
    """The UTC day a release covers, e.g. ``v2025.12.30-...`` -> ``2025-12-30``.

    One release is one day, so this is the pipeline's unit of work. Taking it
    from the tag rather than from the observations keeps a day's partition
    independent of what the data happens to contain.
    """
    match = RELEASE_TAG_DATE.match(tag)
    if not match:
        raise ValueError(f"cannot read a date from release tag {tag!r}")
    return "-".join(match.groups())


def asset_url(tag: str) -> str:
    """URL of the first (``.tar.aa``) part of a release."""
    return f"https://github.com/{REPO}/releases/download/{tag}/{tag}.tar.aa"


def http_range_reader(url: str):
    """Return ``read(start, end)`` fetching an inclusive byte range over HTTP."""

    def read(start: int, end: int) -> bytes:
        request = urllib.request.Request(url, headers={"Range": f"bytes={start}-{end}"})
        with urllib.request.urlopen(request) as response:
            return response.read()

    return read


def find_member_offset(read, prefix: str = TRACES_PREFIX, max_headers: int = 5000) -> int:
    """Byte offset of the first tar member whose name starts with ``prefix``.

    Walks the header chain, reading only the 512-byte header of each member and
    skipping over its payload. ``read`` is a ``read(start, end)`` callable so
    this can be exercised without network access.
    """
    offset = 0
    for _ in range(max_headers):
        header = read(offset, offset + TAR_BLOCK - 1)
        if len(header) < TAR_BLOCK:
            raise ValueError(f"truncated tar header at offset {offset}")

        name = header[:100].rstrip(b"\0").decode("utf-8", "replace")
        if not name:
            raise ValueError(f"reached end of archive without finding {prefix!r}")
        if name.startswith(prefix):
            return offset

        raw_size = header[124:136].rstrip(b"\0 ").decode("ascii", "replace")
        size = int(raw_size, 8) if raw_size else 0
        offset += TAR_BLOCK + ((size + TAR_BLOCK - 1) // TAR_BLOCK) * TAR_BLOCK

    raise ValueError(f"did not find {prefix!r} within {max_headers} members")


def local_name(relative: str) -> str:
    """Give a trace file the ``.json.gz`` name its contents actually warrant.

    The archive names these files ``.json`` even though every one of them is
    gzipped -- correct for readsb, which serves them over HTTP with
    ``Content-Encoding: gzip``, but misleading on disk. Tools that pick their
    decompression codec from the file suffix (Spark and Hadoop do) would
    otherwise read the gzip bytes as text and silently produce garbage.

    Only the name changes; the bytes are still written verbatim.
    """
    if relative.endswith(".json"):
        return relative + ".gz"
    return relative


def extract_traces(archive: bytes, dest: Path) -> list[Path]:
    """Write the complete ``./traces/`` files in ``archive`` under ``dest``.

    ``archive`` is expected to be truncated (it is a byte-range slice), so the
    final member is normally incomplete. Incomplete members are discarded
    rather than written as short files.
    """
    dest = dest.resolve()
    written: list[Path] = []

    stream = tarfile.open(fileobj=io.BytesIO(archive), mode="r|")
    try:
        for member in stream:
            if not member.isfile() or not member.name.startswith(TRACES_PREFIX):
                continue

            relative = local_name(member.name[len(TRACES_PREFIX):])
            target = (dest / relative).resolve()
            if os.path.commonpath([str(dest), str(target)]) != str(dest):
                continue  # refuse to write outside dest

            payload = stream.extractfile(member)
            if payload is None:
                continue
            data = payload.read()
            if len(data) != member.size:
                continue  # truncated by the byte-range cut

            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
            written.append(target)
    except (tarfile.ReadError, EOFError):
        pass  # expected: the slice ends mid-member
    finally:
        stream.close()

    return written


def download_sample(
    tag: str = DEFAULT_TAG,
    dest_root: Path = DEFAULT_DEST_ROOT,
    sample_bytes: int = DEFAULT_SAMPLE_BYTES,
) -> dict:
    """Download and unpack a sample of one release's traces. Returns a manifest."""
    url = asset_url(tag)
    dest = Path(dest_root) / tag

    read = http_range_reader(url)
    start = find_member_offset(read)
    archive = read(start, start + sample_bytes - 1)
    files = extract_traces(archive, dest / "traces")

    manifest = {
        "source_repo": REPO,
        "release_tag": tag,
        "source_url": url,
        "byte_range": [start, start + len(archive) - 1],
        "bytes_downloaded": len(archive),
        "slice_sha256": hashlib.sha256(archive).hexdigest(),
        "trace_files": len(files),
        "retrieved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    (dest / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def main(argv: list[str] | None = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--tag", default=DEFAULT_TAG, help="release tag to sample")
    parser.add_argument(
        "--bytes",
        type=int,
        default=DEFAULT_SAMPLE_BYTES,
        dest="sample_bytes",
        help="how many bytes of the traces region to download",
    )
    parser.add_argument("--dest", type=Path, default=DEFAULT_DEST_ROOT)
    args = parser.parse_args(argv)

    print(f"Locating ./traces/ in {args.tag} (walking tar headers)...")
    manifest = download_sample(args.tag, args.dest, args.sample_bytes)
    print(
        f"Wrote {manifest['trace_files']} trace files "
        f"({manifest['bytes_downloaded'] / 1e6:.1f} MB from offset "
        f"{manifest['byte_range'][0]}) to {Path(args.dest) / args.tag}"
    )


if __name__ == "__main__":
    main()
