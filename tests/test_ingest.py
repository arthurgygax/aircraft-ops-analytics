import gzip
import io
import tarfile

import pytest

from adsb.ingest import extract_traces, find_member_offset


def build_tar(members):
    """Build an uncompressed tar in memory from ``(name, payload)`` pairs."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        for name, payload in members:
            info = tarfile.TarInfo(name)
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))
    return buffer.getvalue()


TRACE = gzip.compress(b'{"icao":"abc123","trace":[]}')


def test_find_member_offset_skips_over_heatmap_payloads():
    """The traces offset is found by walking headers, not by scanning bytes."""
    archive = build_tar(
        [
            ("./heatmap/00.bin.ttf", b"x" * 4096),
            ("./heatmap/01.bin.ttf", b"y" * 1234),
            ("./traces/1c/trace_full_abc123.json", TRACE),
        ]
    )
    reads = []

    def read(start, end):
        reads.append(end - start + 1)
        return archive[start : end + 1]

    offset = find_member_offset(read)

    name = archive[offset : offset + 100].rstrip(b"\0").decode()
    assert name == "./traces/1c/trace_full_abc123.json"
    assert set(reads) == {512}, "should read only headers, never member payloads"


def test_find_member_offset_raises_when_absent():
    archive = build_tar([("./heatmap/00.bin.ttf", b"x" * 512)])
    with pytest.raises(ValueError):
        find_member_offset(lambda s, e: archive[s : e + 1])


def test_extract_traces_writes_only_traces_and_keeps_bytes_verbatim(tmp_path):
    archive = build_tar(
        [
            ("./heatmap/00.bin.ttf", b"x" * 1024),
            ("./README.txt", b"not flight data"),
            ("./traces/1c/trace_full_abc123.json", TRACE),
        ]
    )

    written = extract_traces(archive, tmp_path)

    # renamed to .json.gz: the archive calls these .json but they are gzipped,
    # and suffix-based codec detection (Spark, Hadoop) needs the real suffix
    assert [p.relative_to(tmp_path).as_posix() for p in written] == [
        "1c/trace_full_abc123.json.gz"
    ]
    # raw means raw: only the name changed, bytes are identical to the member
    assert written[0].read_bytes() == TRACE


def test_extract_traces_discards_member_cut_by_the_byte_range(tmp_path):
    """A range slice ends mid-file; that partial member must not be written."""
    archive = build_tar(
        [
            ("./traces/1c/trace_full_complete.json", TRACE),
            ("./traces/1c/trace_full_cut.json", b"z" * 4096),
        ]
    )

    # cut 100 bytes into the second member's payload (tarfile pads the archive
    # out to a 10 KB record, so trimming from the end would only drop padding)
    cut_header = find_member_offset(
        lambda s, e: archive[s : e + 1], "./traces/1c/trace_full_cut.json"
    )
    written = extract_traces(archive[: cut_header + 512 + 100], tmp_path)

    assert [p.name for p in written] == ["trace_full_complete.json.gz"]
    assert not (tmp_path / "1c" / "trace_full_cut.json.gz").exists()


def test_extract_traces_refuses_to_write_outside_dest(tmp_path):
    dest = tmp_path / "dest"
    dest.mkdir()
    archive = build_tar([("./traces/../../escaped.json", TRACE)])

    written = extract_traces(archive, dest)

    assert written == []
    assert not (tmp_path.parent / "escaped.json.gz").exists()
