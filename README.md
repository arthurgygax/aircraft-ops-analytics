# Aircraft Ops Analytics

**A batch data platform that turns raw ADS-B radio observations into airport
operations analytics — reconstructing flights, detecting flight phases and
holding patterns, and serving the results to an interactive explorer and to
Power BI.**

![Spark](https://img.shields.io/badge/Compute-PySpark%203.5-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Delta](https://img.shields.io/badge/Format-Delta%20Lake%203.2-00ADD4?style=flat)
![Storage](https://img.shields.io/badge/Storage-S3%20%2F%20MinIO-C72E49?style=flat&logo=minio&logoColor=white)
![Docker](https://img.shields.io/badge/Runtime-Docker%20Compose-2496ED?style=flat&logo=docker&logoColor=white)
![Streamlit](https://img.shields.io/badge/App-Streamlit-FF4B4B?style=flat&logo=streamlit&logoColor=white)
![Tests](https://img.shields.io/badge/tests-249%20passing-3fb950?style=flat)

Nobody publishes an open dataset of "flights that departed airport X today".
What *is* public is the raw radio: hundreds of millions of position reports a
day, broadcast by aircraft and collected by volunteers. This project treats
that as a data engineering problem — read 9 GB of it, work out which aircraft
were flying, where they went, what they were doing, and which of them circled
before landing.

The study dataset is **seven days at Zürich and Düsseldorf**: 4,740
reconstructed flights and every one of their 3.3 million trajectory points,
selected out of 17 million candidate observations.

---

## Architecture

```mermaid
flowchart LR
    SRC["adsb.lol<br/>daily release"] --> RAW["Raw<br/>gzipped JSON"]
    RAW --> OBJ[("Object storage<br/>MinIO / S3")]
    OBJ --> OBS["Observations<br/>3.33M points"]
    OBS --> FL["Flights<br/>4,740 reconstructed"]
    OBS --> PH["Phases<br/>32,976 intervals"]
    OBS --> HLD["Holds<br/>36 detected"]
    FL --> MOV["Movements<br/>5,249 inferred"]
    MOV --> OPS["Airport daily<br/>operations"]
    HLD --> OPS
    HLD --> FL
    OBS --> ST["Streamlit<br/>Flight Explorer"]
    FL --> ST
    OPS --> BI["Power BI<br/>dashboards"]
    FL --> BI
```

Six Delta tables, each at a grain nothing else holds. Decoding, cleaning,
deduplication and flight segmentation all happen in one pass, so the point
grain is written once rather than three times; every write is scoped to one
day with `replaceWhere`, so a day can be reprocessed without touching the rest,
and a full rebuild has to be asked for by name.

## What it demonstrates

| | |
|---|---|
| **PySpark** | window functions for segmentation, phase detection and hold geometry; every transformation is Spark-native — no pandas in the pipeline, no row-by-row Python, no UDFs |
| **Delta Lake** | ACID writes, schema enforcement, time travel, partition-scoped overwrite |
| **Object storage** | S3A against MinIO locally; the same code runs against AWS by dropping one environment variable |
| **Docker** | four images — pipeline, app, benchmark, storage — and the whole project runs from `docker compose` |
| **Data modelling** | one table per grain, one key (`flight_id`), no accidental star schema, no layer that only exists because the code once had one |
| **Incremental processing** | day-at-a-time, idempotent, proven against the Delta transaction log; a whole-table overwrite has to be requested explicitly |
| **Data quality** | invariant checks across six tables, run inline by every stage |
| **Analytics** | flight reconstruction, phase detection, holding-pattern geometry |
| **Visualisation** | a Streamlit explorer and a documented Power BI model |

## Analytical scope

The dataset is **seven consecutive days — 2025-12-24 to 2025-12-30 — at Zürich
(LSZH) and Düsseldorf (EDDL)**, and the rule for what is in it is one sentence:

> A flight is in scope when it **departed from or arrived at** a study airport.
> Its **whole trajectory** is then in scope — every observation of that flight,
> wherever the aircraft was.

The filter is on flight identity, never on the position of an individual
observation. A Zurich departure to New York spends nine tenths of its
trajectory outside Europe; filtering points by distance from the airport would
leave a stub climbing out of Kloten and nothing else. So the flight is
identified first and its trajectory kept in full, at the point grain, with
nothing aggregated, clipped or simplified.

The scope is a value in `adsb.scope`, not a constant scattered through the
transformations: no module below it contains a date or an airport code, and the
period, the airports and the pre-filter radius are all configurable by
environment or command line. Full reasoning, including how a cheap pre-filter
makes this affordable without being able to drop a flight it should keep, is in
**[docs/scope.md](docs/scope.md)**.

## Quick start

Requires Docker. From a clean checkout:

```bash
# 1. object storage
docker compose up -d minio

# 2. the study period: seven releases, 50% of each day's traces
PYTHONPATH=src python3 -m adsb.ingest
docker compose run --rm spark python -m adsb.upload_raw
docker compose run --rm spark python -m adsb.airports      # airport reference

# 3. the pipeline, all seven days in one Spark session
SPARK_DRIVER_MEMORY=12g docker compose run --rm spark \
    python -m adsb.run_pipeline --full-rebuild

# 4. explore it
docker compose up -d explorer      # http://localhost:8502
```

Both commands default to the study period, so neither takes a date. Each stage
prints its row counts and runs its own quality checks; to validate every table
at once, `docker compose run --rm spark python -m adsb.quality`.

To try it without the full download, take a single day at a small fixed budget:

```bash
TAG=v2025.12.30-planes-readsb-prod-0
PYTHONPATH=src python3 -m adsb.ingest --tag $TAG --bytes 200000000
docker compose run --rm spark python -m adsb.upload_raw --tag $TAG
docker compose run --rm spark python -m adsb.run_pipeline --tag $TAG --full-rebuild
```

Reprocessing one day of an existing period is incremental, not a rebuild — drop
`--full-rebuild` and only that day's partition is replaced. A run reclaims its
own tombstoned files at the end. Set `ADSB_ROOT` to a scratch prefix for
exploratory runs so they never touch the real tables.

## Key engineering decisions

Full reasoning per stage in [docs/pipeline.md](docs/pipeline.md); these are the
ones that shaped the design.

**Byte-range ingestion instead of downloading a day.** A daily release is
2.0–3.2 GB. Because it is an *uncompressed* tar, an HTTP range request for a
prefix yields whole, valid members — so the pipeline walks the tar header chain
with 512-byte reads to find where `./traces/` starts and downloads only what it
needs. Nothing about the layout is assumed, because none of it holds across the
seven days: `./traces/` starts at byte 512 on most of them and 763 MB in on
2025-12-30, and six are split into 2 GB parts while Christmas Day is quiet
enough to be published as a single `.tar`.

**The sample is a fraction of each day, not a fixed number of bytes.** A byte
budget would have sampled the seven days unequally — and would have sampled
Christmas Day, the smallest archive, most heavily of all, making "traffic
dropped on the 25th" indistinguishable from "we downloaded more of the 25th".

**Scope is enforced on flight identity, never on position.** The 25 km box
around each airport is only a pre-filter, and it is provably conservative: a
movement needs an endpoint within 5 km, and that endpoint is itself a trace
point. Flights are then selected by the movements they made, and their
trajectories kept whole — see [docs/scope.md](docs/scope.md).

**Trajectories are stored once, in object storage, at full resolution.**
3,330,291 points for all 4,740 flights across all seven days live in
`observations` — not in a container, not in a second `flight_tracks` copy of
the same grain. Partitioned by day into seven ~14 MB files, so the app's first
filter prunes six sevenths of the table. Verified to survive a complete
`docker compose down`, and the explorer image has no JVM, so it cannot
recompute even in principle — it reads what the pipeline published. Layout,
persistence proof and measured query latencies are in
[docs/gold-model.md](docs/gold-model.md#the-trajectory-store).

**The explorer colours three trajectories, not eight.** Any two tracks on a
map can end up adjacent, so the palette has to clear the colour-vision floors
on *every* pair, not just neighbouring ones. Run against that test, only three
hues survive in both light and dark — a fourth measured ΔE 1.9 against blue for
protanopes on the dark surface, which is no separation at all. So identity
never rests on colour: every track is labelled with its callsign, and further
selections take neutral ink rather than an invented hue.

**Thresholds are measured, never assumed.** Flights split on a 15-minute
tracking gap because segments containing more than one callsign — the signature
of two flights merged into one — stay flat at 14–19 for thresholds from 5 to 30
minutes, then jump to 47 at 60. The phase level band is ±300 fpm because
airborne vertical rates have a 25th percentile of −640 and a 75th of +512.
Airport matching uses 5 km because segments that began on the ground sit a
median 1.0 km from an airport, and widening to 10 km adds only 1.1%.

**Time windows, not row windows.** Sampling is irregular — median 4 s, mean
8.1 s, p90 20 s. The original OpenSky-era code smoothed over 40 *rows*, which
here would cover 40 seconds for one flight and five minutes for another. Every
window is defined in seconds via `RANGE BETWEEN`.

**Data is validated, not silently repaired.** Coordinate ranges are asserted
rather than corrected: if a future release contains bad positions the pipeline
fails loudly instead of quietly reshaping them. Bad *values* are nulled rather
than their rows dropped, so a glitched speed never costs a valid position fix.

**The partition key comes from the release identity, not the data.** A key
derived from row timestamps would let a stray observation near midnight drag a
neighbouring day into a write, so reprocessing day N could destroy part of day
N−1. Deriving it from the release makes that impossible.

**Scale was measured before optimising.** The pipeline was run at 150× the
development sample (44.4M rows). The small-files problem never materialised and
retrieval stayed fast, so no Z-ordering, clustering or down-sampling was added.
What the scale run *did* surface was Spark's 1 GB default driver heap being
insufficient, and a class of non-aircraft emitters in the data.

## Inferred vs authoritative

This distinction runs through the project, and every table is labelled.

| Authoritative — transmitted by the aircraft | Coverage |
|---|---|
| ICAO address, timestamp, latitude, longitude, ground/airborne flag | 100% |
| Ground speed / track / altitude / vertical rate | 99 / 95 / 88 / 89% |

| Reference lookup — readsb's aircraft database, not transmitted | Coverage |
|---|---|
| Registration, aircraft type | 93 / 92% of flights |
| Registered owner — **the registry owner, not the airline** | 45% |

| Inferred by this pipeline | Coverage |
|---|---|
| Flight boundaries and `flight_id` | all flights |
| Airline code, from an airline-style callsign | 65% |
| Departure / arrival airport, matched geographically | 45 / 42% |
| Flight phases, holding patterns | derived from the trajectory |

**None of this is official.** No flight plan, airline schedule, airport
database or ATC record is involved anywhere. The numbers will not reconcile
with published movement statistics, and every Gold row carries a
`metric_source` or `data_source` column saying so.

## Example questions it answers

- How does traffic differ between Zurich and Düsseldorf, by day and by hour?
- Which airlines and aircraft types operate at each?
- What altitude and speed profile did a particular flight fly, and when did it
  climb, cruise and descend?
- Which flights circled before landing, for how long, and how many circuits?
- What does an individual trajectory look like, and how do trajectories differ
  between airports, airlines and types?

Nothing in the aggregation enforces the answers, which is what makes them worth
checking. Swiss (1,264 flights), Eurowings (513) and Edelweiss (260) come out on
top; the A320 leads on type with the A220-300 second, which is Swiss's fleet;
Zurich–Heathrow runs 59 departures against 59 arrivals and Zurich–Amsterdam
48 against 48. Traffic drops on 25 December — Zurich 469 movements against 635
on the 29th — and recovers over the following days.

Full measured results, and the two artefacts to know about before reading the
tables, are in [docs/scope.md](docs/scope.md).

## The applications

**Flight Explorer** (`docker compose up -d explorer`, port 8502) — two tabs
over the seven-day study period.

*Flights*: filter by date, **departure** airport, **arrival** airport, airline,
aircraft type and callsign — departure and arrival are separate boxes, because
"departing ZRH" and "arriving at ZRH" are different questions about the same
airport. Pick flights from the list to draw their trajectories. One flight
shows its track coloured by detected phase, a phase timeline, altitude and
speed profiles, and any detected holding pattern drawn as the circling itself;
several show one colour each for comparison.

*Airports*: ZRH or DUS, with movements by day and by hour, arrivals against
departures, airline and aircraft-type distributions, and detected-hold
statistics.

It reads the published tables and computes nothing: no Spark, no JVM, starting
in seconds via [delta-rs](https://delta-io.github.io/delta-rs/). The 3.3M-point
trajectory table is never loaded whole — points are fetched per selected
flight, with the date, so six of the seven day-partitions are pruned.

**Power BI** — `docker compose run --rm spark python -m adsb.bi_export` writes
six Parquet extracts (45 MB) that Power BI opens natively.
[docs/powerbi.md](docs/powerbi.md) documents the model, relationships, DAX
measures and dashboard pages.

> **Screenshots:** `docs/dashboard_preview.png`, `docs/feature_inspector.png`
> and `docs/feature_radar.png` are pictures of the **superseded legacy
> application**, not of the Flight Explorer. Capture the two current tabs into
> `docs/` and delete those three when you do.

## Testing and data quality

```bash
docker compose run --rm spark pytest -q                             # pipeline
docker compose run --rm explorer pytest tests/test_app_data.py -q   # app
```

200 pipeline tests and 49 app tests, covering the places where being wrong is
easy and silent: tar-slice truncation, the deduplication *winner rule* (not just
its row count), segmentation boundaries, phase detection against synthetic
climb/cruise/descent profiles with injected noise, hold geometry against a
synthetic racetrack versus a normal turn, incremental idempotence, the refusal
to overwrite a whole table without being asked, and referential integrity across
every table.

Separately, the data quality checks run **inline in every stage**, so a stage
that produces bad data fails instead of publishing it. They are SQL predicates
matching invalid rows, counted in a single pass, with no framework. Each rule
exists because of a failure mode this pipeline actually showed — and a set of
tests deliberately corrupts data to prove every check fires.

## Limitations

**The dataset is a 50% sample, not a census.** Each day's traces are sampled at
the same rate, so the days are comparable with each other — but absolute counts
are roughly halved. Read "ZRH had 430 arrivals" as "430 in a 50% sample". The
prefix cut lands on aircraft grouped by the last two hex digits of their
address, which carries no information about operator, type or destination, so
proportions are preserved even though counts are not. The specific aircraft
sampled also differ from day to day: aggregate comparisons across days are
sound, following one registration across the week is not.

**Only ZRH and DUS traffic is in the dataset.** By design — see
[docs/scope.md](docs/scope.md). Flights that touched neither airport were never
written, so this is not a global dataset with a filter applied at query time.

**Coverage is uneven.** adsb.lol sees what its volunteers see — dense over
Europe and North America, thin elsewhere, and patchy on the ground everywhere
(only 44% of flights have any ground observation).

**A flight is an observation artefact.** It is a period of continuous tracking.
A long coverage gap splits one real flight in two; an aircraft tracked through a
turnaround becomes one segment covering two real flights.

**A circle is a circle.** Hold detection finds sustained circling, and survey,
training and police orbits produce identical geometry. On the sample the highest
"hold rates" are at flight-training airports — that is circuit training, not
delay. `hold_rate` is not a delay metric.

**Airports resolve for a minority of flights** — 45% departure, 42% arrival,
both ends for 26%. Those columns are nullable by design.

Per-stage limitations are documented in [docs/pipeline.md](docs/pipeline.md).

## Repository layout

```
src/adsb/            the pipeline, one module per grain
  spark_explore.py   Spark session, trace reader, and the exploration job
  ingest.py          byte-range download from adsb.lol
  upload_raw.py      raw files to object storage
  scope.py           the study period and airports, and the two scope filters
  observations.py    decode, clean, deduplicate and segment -- one pass
  flights.py         the flight table
  airports.py        airport reference, movements, daily operations
  phases.py          climb / cruise / descent / taxi detection
  holds.py           holding pattern detection
  quality.py         data quality checks
  delta_io.py        partitioned writes and the table lifecycle
  bi_export.py       Parquet extracts for Power BI
  run_pipeline.py    the whole pipeline in one Spark session
app/                 Streamlit Flight Explorer (data.py + main.py)
bench/               Spark vs pandas benchmark on the same pipeline
tests/               the test suite
docs/                scope, pipeline, Gold model and Power BI reference
docker/              four images: pipeline, app, benchmark, legacy
```

Reference documentation: [scope](docs/scope.md) · [pipeline](docs/pipeline.md) ·
[Gold model](docs/gold-model.md) · [Power BI](docs/powerbi.md)

## Data source

[adsb.lol globe_history](https://github.com/adsblol/globe_history_2025), open
data under the ODbL, collected by volunteer feeders. Airport reference data from
[OurAirports](https://ourairports.com/data/), public domain. Neither is
redistributed here — both are fetched by the pipeline.

<details>
<summary><b>Legacy application</b></summary>

`src/app.py`, `logic.py`, `viz.py` and `convert_to_parquet.py` are the original
project: a single-airport Streamlit dashboard over OpenSky CSV dumps that
computed flight phases in the browser at query time. It is kept for reference
and superseded by the pipeline and Flight Explorer above. It is excluded from a
default `docker compose up`; run it with
`docker compose --profile legacy up app` (needs a `.env` with a Mapbox token).
The screenshots in `docs/` are of that application.

</details>

## License

MIT.
