"""Spark vs pandas on the same pipeline, the same input and the same outputs.

    python -m bench.run_benchmark --raw data/raw/adsb/<tag> --tag <tag> \
        --airports data/reference/airports.csv --sizes 250,1000,4000,16000

The question is not which engine is better. It is where the boundary sits for
*this* pipeline on *one* machine: at what input size does the pandas version
stop being the obviously simpler choice.

HOW IT IS MADE FAIR
    * Same raw files. ``--sizes`` takes the first N trace files of a release,
      so both engines see byte-identical input.
    * Same transformations. ``bench/spark_pipeline.py`` calls the pipeline's own
      functions; ``bench/pandas_pipeline.py`` mirrors them line for line.
    * Same sink. Both write local Parquet. Delta on MinIO would measure the
      storage layer, and pandas has no Delta writer.
    * Same correctness check. Row counts per table, plus a row-for-row compare
      of the key columns of every table.
    * Neither side is tuned. Spark uses the project's own session; pandas uses
      ordinary vectorised pandas. No configuration is set for the benchmark.

WHAT IS MEASURED
    Wall time and peak RSS of the whole process tree -- for Spark that is the
    Python driver plus the JVM, which is the memory the machine actually has to
    find. Each run is a fresh subprocess, so neither engine inherits the
    other's warm caches or heap.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

import pandas as pd
import psutil

TABLES = (
    "observations", "flights", "movements", "flight_phases", "flight_holds",
    "airport_daily_operations",
)

# The columns compared row for row. Keys plus the values most likely to move if
# a transformation drifted: a matching row count proves much less than this.
COMPARE_KEYS = {
    "observations": ["flight_id", "observation_seq"],
    "flights": ["flight_id"],
    "movements": ["flight_id", "movement_type"],
    "flight_phases": ["flight_id", "phase_seq"],
    "flight_holds": ["flight_id", "hold_seq"],
    "airport_daily_operations": ["operations_date", "airport_ident"],
}
COMPARE_VALUES = {
    "observations": ["event_time", "latitude", "longitude", "altitude_ft", "callsign"],
    "flights": ["n_observations", "duration_seconds", "callsign", "airline_icao",
                "departure_airport_ident", "arrival_airport_ident",
                "n_detected_holds"],
    "movements": ["ident", "event_time"],
    "flight_phases": ["phase", "duration_seconds", "n_observations"],
    "flight_holds": ["duration_seconds", "circuits", "span_km"],
    "airport_daily_operations": ["arrivals", "departures", "total_operations",
                                 "unique_aircraft"],
}


def sample_input(raw_dir: Path, n_files: int, dest: Path) -> int:
    """The first ``n_files`` trace files of a release, laid out identically."""
    if dest.exists():
        shutil.rmtree(dest)
    files = sorted(raw_dir.rglob("*.json.gz"))[:n_files]
    for path in files:
        target = dest / path.relative_to(raw_dir)
        target.parent.mkdir(parents=True, exist_ok=True)
        os.link(path, target)  # same bytes, no copy
    return len(files)


def measure(command: list[str]) -> dict:
    """Run a command, returning wall time, peak tree RSS and its row counts."""
    started = time.time()
    process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    peak = 0
    stop = threading.Event()

    def sample() -> None:
        nonlocal peak
        parent = psutil.Process(process.pid)
        while not stop.is_set():
            try:
                total = parent.memory_info().rss
                for child in parent.children(recursive=True):
                    try:
                        total += child.memory_info().rss
                    except psutil.Error:
                        pass
                peak = max(peak, total)
            except psutil.Error:
                return
            stop.wait(0.2)

    watcher = threading.Thread(target=sample, daemon=True)
    watcher.start()
    out, err = process.communicate()
    stop.set()
    watcher.join(timeout=2)
    elapsed = time.time() - started

    counts = {}
    match = re.search(r"ROW_COUNTS (\{.*\})", out)
    if match:
        counts = json.loads(match.group(1))
    return {
        "seconds": round(elapsed, 1),
        "peak_rss_mb": round(peak / 1e6, 1),
        "returncode": process.returncode,
        "rows": counts,
        "stderr_tail": err.strip().splitlines()[-6:] if process.returncode else [],
    }


def _read(out_dir: Path, name: str) -> pd.DataFrame:
    path = out_dir / name
    if path.is_dir():
        return pd.concat(
            [pd.read_parquet(p) for p in sorted(path.glob("*.parquet"))],
            ignore_index=True,
        )
    return pd.read_parquet(path.with_suffix(".parquet"))


def compare(spark_dir: Path, pandas_dir: Path) -> dict:
    """Row-for-row equivalence per table, not just a matching count."""
    verdicts = {}
    for name in TABLES:
        try:
            left, right = _read(spark_dir, name), _read(pandas_dir, name)
        except (FileNotFoundError, ValueError) as error:
            verdicts[name] = f"unreadable: {error}"
            continue
        if len(left) != len(right):
            verdicts[name] = f"row counts differ: spark {len(left):,} pandas {len(right):,}"
            continue
        keys = COMPARE_KEYS[name]
        values = [c for c in COMPARE_VALUES[name]
                  if c in left.columns and c in right.columns]
        left = left[keys + values].sort_values(keys).reset_index(drop=True)
        right = right[keys + values].sort_values(keys).reset_index(drop=True)
        # the two engines spell the same value in different dtypes -- Spark's
        # DateType against pandas date objects, string against object. Compare
        # values, not storage.
        for frame in (left, right):
            for column in values + keys:
                if pd.api.types.is_float_dtype(frame[column]):
                    frame[column] = frame[column].round(4)
                elif pd.api.types.is_datetime64_any_dtype(frame[column]):
                    series = frame[column]
                    if getattr(series.dtype, "tz", None) is not None:
                        series = series.dt.tz_convert("UTC").dt.tz_localize(None)
                    frame[column] = series
                elif not pd.api.types.is_numeric_dtype(frame[column]):
                    frame[column] = frame[column].astype("string")
        mismatched = 0
        details = []
        for column in keys + values:
            a, b = left[column], right[column]
            differing = int((~((a == b) | (a.isna() & b.isna()))).sum())
            if differing:
                mismatched += differing
                details.append(f"{column}={differing}")
        verdicts[name] = (
            f"identical ({len(left):,} rows)" if not mismatched
            else f"{len(left):,} rows, differing cells: {', '.join(details)}"
        )
    return verdicts


def main(argv: list[str] | None = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--raw", required=True, type=Path)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--airports", required=True)
    parser.add_argument(
        "--sizes", default="250,1000,4000,16000",
        help="how many aircraft trace files to feed each run",
    )
    parser.add_argument("--work", type=Path, default=Path("/app/data/bench"))
    parser.add_argument("--engines", default="spark,pandas")
    args = parser.parse_args(argv)

    engines = args.engines.split(",")
    args.work.mkdir(parents=True, exist_ok=True)
    results = []

    for size in [int(s) for s in args.sizes.split(",")]:
        sample = args.work / f"input_{size}" / args.tag
        n = sample_input(args.raw, size, sample)
        print(f"\n=== {n:,} aircraft ===", flush=True)
        row = {"aircraft": n}
        for engine in engines:
            out = args.work / f"out_{engine}_{size}"
            if out.exists():
                shutil.rmtree(out)
            module = f"bench.{engine}_pipeline"
            measured = measure([
                sys.executable, "-m", module,
                "--raw", str(sample), "--tag", args.tag,
                "--airports", args.airports, "--out", str(out),
            ])
            row[engine] = measured
            status = "ok" if measured["returncode"] == 0 else "FAILED"
            print(
                f"  {engine:<7} {measured['seconds']:>8.1f}s "
                f"{measured['peak_rss_mb']:>9,.0f} MB peak  {status}",
                flush=True,
            )
            for line in measured["stderr_tail"]:
                print(f"      {line}", flush=True)

        # compare whenever both sides are on disk, so one engine can be re-run
        # on its own against the other's retained output
        spark_out = args.work / f"out_spark_{size}"
        pandas_out = args.work / f"out_pandas_{size}"
        if spark_out.exists() and pandas_out.exists():
            row["equivalence"] = compare(spark_out, pandas_out)
            for name, verdict in row["equivalence"].items():
                print(f"    {name:<26} {verdict}", flush=True)
        results.append(row)

    report = args.work / "results.json"
    report.write_text(json.dumps(results, indent=2, default=str) + "\n")
    print(f"\nWrote {report}")

    print("\n--- summary ---")
    print(f"{'aircraft':>9} {'rows':>12} {'spark s':>9} {'pandas s':>9} "
          f"{'spark MB':>9} {'pandas MB':>10}")
    for row in results:
        rows = (row.get("spark", {}).get("rows", {}) or
                row.get("pandas", {}).get("rows", {})).get("observations", 0)
        spark = row.get("spark", {})
        pandas_ = row.get("pandas", {})
        print(
            f"{row['aircraft']:>9,} {rows:>12,} "
            f"{spark.get('seconds', float('nan')):>9} "
            f"{pandas_.get('seconds', float('nan')):>9} "
            f"{spark.get('peak_rss_mb', float('nan')):>9,.0f} "
            f"{pandas_.get('peak_rss_mb', float('nan')):>10,.0f}"
        )


if __name__ == "__main__":
    main()
