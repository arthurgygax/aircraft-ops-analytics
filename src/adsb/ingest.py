"""Acquire a small, reproducible sample of raw adsb.lol globe_history data.

One daily release is 2.0-3.2 GB, so we never download a whole one. The release
asset is an *uncompressed* tar, which means a byte-range request for a prefix
of it yields whole, valid tar members.

The archive holds::

    ./traces/<xx>/trace_full_<icao>.json    gzipped readsb trace JSON
    ./heatmap/*.bin.ttf                     binary replay files (not used here)
    ./README.txt, ./acas/, ./LICENSE-*

Neither the order nor the split is a constant of the format, and both vary
across the seven days of the study period, so nothing here assumes either:

* ``./traces/`` is the first member on five of the seven days and starts
  763 MB in on 2025-12-30, so its offset is found by walking the tar header
  chain with 512-byte range reads rather than being a number in the source.
* Six days are split into 2 GB parts (``.tar.aa``, ``.tar.ab``); 2025-12-25 is
  quiet enough to fit in one and is published as a plain ``.tar``. The parts
  are probed rather than assumed, because assuming ``.tar.aa`` is a 404 on
  Christmas Day.
* ``<xx>`` is the *last* two hex digits of the aircraft address, and the
  directories appear in the archive in no particular order, which is what makes
  a byte prefix a usable sample of aircraft rather than a slice of the alphabet.

Files are written byte-for-byte as they appear in the archive: still gzipped,
still the original JSON, no parsing or cleaning.

HOW MUCH TO TAKE, AND WHY IT IS A FRACTION
    A fixed byte budget would sample the seven days unequally, because their
    traces regions differ by a factor of 1.5 -- and it would sample Christmas
    Day, the smallest archive, most heavily of all. "Traffic dropped on the
    25th" would then be indistinguishable from "we downloaded more of the
    25th". So the sample is a *fraction* of each day's traces region, which
    makes the days comparable: each is an equal-rate sample of its own day.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import tarfile
import urllib.error
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path

REPO = "adsblol/globe_history_2025"
DEFAULT_TAG = "v2025.12.30-planes-readsb-prod-0"
DEFAULT_SAMPLE_BYTES = 8 * 1024 * 1024

# Half of each day's traces. Measured yield at this rate: ~500 in-scope flights
# and ~350k trajectory observations per day; see docs/scope.md.
DEFAULT_SAMPLE_FRACTION = 0.5

# In the order they are tried. A split release starts at ``.tar.aa``; an
# unsplit one is a plain ``.tar``.
ASSET_SUFFIXES = (".tar.aa", ".tar")
# Continuation parts of a split release, probed until one is missing.
PART_SUFFIXES = tuple(f".tar.a{letter}" for letter in "abcdefgh")

TAR_BLOCK = 512
TRACES_PREFIX = "./traces/"

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_DEST_ROOT = PROJECT_ROOT / "data" / "raw" / "adsb"


RELEASE_TAG_DATE = re.compile(r"^v(\d{4})\.(\d{2})\.(\d{2})-")
RELEASE_TAG_SUFFIX = "-planes-readsb-prod-0"


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


def release_tag(day: date | str) -> str:
    """The release covering a UTC day: the inverse of ``release_date``.

    ``adsb.scope`` turns a study period into the releases that cover it, and
    this is the one place that knows how a tag is spelled.
    """
    if isinstance(day, str):
        day = date.fromisoformat(day)
    return f"v{day.year}.{day.month:02d}.{day.day:02d}{RELEASE_TAG_SUFFIX}"


def asset_url(tag: str, suffix: str = ASSET_SUFFIXES[0]) -> str:
    """URL of one asset of a release."""
    return f"https://github.com/{REPO}/releases/download/{tag}/{tag}{suffix}"


def content_length(url: str) -> int | None:
    """Size of a published asset, or None when there is no such asset."""
    request = urllib.request.Request(url, method="HEAD")
    try:
        with urllib.request.urlopen(request) as response:
            return int(response.headers["Content-Length"])
    except urllib.error.HTTPError:
        return None


def asset_parts(tag: str, size_of=content_length) -> list[tuple[str, int]]:
    """Every published part of a release, in archive order, with its size.

    Probed rather than assumed: the split is a property of how big the day was,
    not of the format. ``size_of`` is injected so this is testable offline.
    """
    for suffix in ASSET_SUFFIXES:
        first = asset_url(tag, suffix)
        size = size_of(first)
        if size is None:
            continue
        if suffix != ASSET_SUFFIXES[0]:
            return [(first, size)]
        parts = [(first, size)]
        for part in PART_SUFFIXES[1:]:
            url = asset_url(tag, part)
            size = size_of(url)
            if size is None:
                break
            parts.append((url, size))
        return parts
    raise ValueError(f"release {tag!r} publishes no downloadable archive")


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


def traces_region(tag: str, parts: list[tuple[str, int]] | None = None) -> dict:
    """Where the traces start in a release, and how much of them there is.

    ``reachable`` is what a byte range against the *first* asset can return.
    The rest of a split release lives in a second file, and reading across that
    boundary would mean stitching two downloads together; nothing here needs
    to, because the sample is a prefix.
    """
    parts = parts or asset_parts(tag)
    first_url, first_size = parts[0]
    start = find_member_offset(http_range_reader(first_url))
    return {
        "url": first_url,
        "start": start,
        "region_bytes": sum(size for _, size in parts) - start,
        "reachable_bytes": first_size - start,
        "parts": len(parts),
    }


def download_sample(
    tag: str = DEFAULT_TAG,
    dest_root: Path = DEFAULT_DEST_ROOT,
    sample_bytes: int | None = None,
    fraction: float | None = None,
) -> dict:
    """Download and unpack a sample of one release's traces. Returns a manifest.

    Pass ``fraction`` to take that share of the day's traces -- the comparable
    option, and what the study period uses -- or ``sample_bytes`` for a fixed
    budget when the size of the day does not matter.
    """
    if fraction is not None and not 0 < fraction <= 1:
        raise ValueError(f"fraction must be in (0, 1], got {fraction}")

    region = traces_region(tag)
    if fraction is not None:
        wanted = int(region["region_bytes"] * fraction)
    else:
        wanted = sample_bytes if sample_bytes is not None else DEFAULT_SAMPLE_BYTES
    take = min(wanted, region["reachable_bytes"])

    start = region["start"]
    dest = Path(dest_root) / tag
    archive = http_range_reader(region["url"])(start, start + take - 1)
    files = extract_traces(archive, dest / "traces")

    manifest = {
        "source_repo": REPO,
        "release_tag": tag,
        "release_date": release_date(tag),
        "source_url": region["url"],
        "archive_parts": region["parts"],
        "traces_region_bytes": region["region_bytes"],
        "byte_range": [start, start + len(archive) - 1],
        "bytes_downloaded": len(archive),
        # what was actually sampled, which is the number the day-to-day
        # comparison rests on -- not the number that was asked for
        "sampled_fraction": round(len(archive) / region["region_bytes"], 4),
        "requested_bytes": wanted,
        "slice_sha256": hashlib.sha256(archive).hexdigest(),
        "trace_files": len(files),
        "retrieved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    (dest / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def main(argv: list[str] | None = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--tag",
        action="append",
        dest="tags",
        help="release tag to sample; repeatable. Defaults to the study period.",
    )
    parser.add_argument(
        "--bytes",
        type=int,
        default=None,
        dest="sample_bytes",
        help=f"fixed byte budget per release (default {DEFAULT_SAMPLE_BYTES:,})",
    )
    parser.add_argument(
        "--fraction",
        type=float,
        default=None,
        help="share of each day's traces to take instead of a fixed budget; "
             f"the study period uses {DEFAULT_SAMPLE_FRACTION}",
    )
    parser.add_argument("--dest", type=Path, default=DEFAULT_DEST_ROOT)
    args = parser.parse_args(argv)

    tags, fraction = args.tags, args.fraction
    if not tags:
        # deferred: adsb.scope imports this module for release_tag
        from adsb.scope import default_scope

        scope = default_scope()
        tags = list(scope.release_tags)
        if fraction is None and args.sample_bytes is None:
            fraction = DEFAULT_SAMPLE_FRACTION
        print(f"Study period: {scope.describe()}")

    total_files = total_bytes = 0
    for tag in tags:
        print(f"Locating ./traces/ in {tag} (walking tar headers)...")
        manifest = download_sample(tag, args.dest, args.sample_bytes, fraction)
        total_files += manifest["trace_files"]
        total_bytes += manifest["bytes_downloaded"]
        print(
            f"  {manifest['trace_files']:>6,} trace files  "
            f"{manifest['bytes_downloaded'] / 1e9:5.2f} GB  "
            f"({manifest['sampled_fraction']:.0%} of the day's traces, "
            f"from offset {manifest['byte_range'][0]:,})"
        )
    if len(tags) > 1:
        print(f"{len(tags)} releases: {total_files:,} trace files, "
              f"{total_bytes / 1e9:.2f} GB")


if __name__ == "__main__":
    main()
