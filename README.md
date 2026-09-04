# Aircraft Ops Analytics

![Python](https://img.shields.io/badge/Backend-Python%203.9-3776AB?style=flat&logo=python) ![Frontend](https://img.shields.io/badge/Frontend-Streamlit-FF4B4B?style=flat&logo=streamlit) ![Data](https://img.shields.io/badge/Data-Parquet%20%7C%20Pandas-150458?style=flat&logo=pandas) ![Viz](https://img.shields.io/badge/Viz-Plotly%20%7C%20Mapbox-3F4F75?style=flat&logo=plotly) ![Docker](https://img.shields.io/badge/Deploy-Docker-2496ED?style=flat&logo=docker)

**Aircraft Ops Analytics** is a data engineering and visualization pipeline designed to analyze airport operational efficiency. It processes unstructured flight telemetry data downloaded from OpenSky Network to uncover insights regarding airspace congestion, taxi times, and flight profiles.

### Repository contents

This repository currently holds two separate things:

1. **The OpenSky/Streamlit application** — complete and documented below. Reads OpenSky `states_*.csv.tar` dumps via `src/convert_to_parquet.py` and serves them through a Streamlit dashboard.
2. **A new ADS-B data engineering pipeline** (`src/adsb/`) — under development, sourced from [adsb.lol globe_history](https://github.com/adsblol/globe_history_2025). It is a different source format and does not share code with the application above. Currently implements raw ingestion and local PySpark processing.

#### Getting the sample data (new pipeline)

Each adsb.lol release is one day of global flight data, ~3.2 GB, published as an uncompressed tar split across two GitHub assets. We do not download a whole one. Because tar is sequential, a byte-range request for a slice of the first part yields whole, valid members — so ingestion locates the `./traces/` region by walking the tar headers, then downloads a small window from it:

```bash
pip install -r requirements-dev.txt
PYTHONPATH=src python -m adsb.ingest
```

This fetches ~8 MB (about 220 aircraft trace files, ~10 s) into `data/raw/adsb/<release-tag>/traces/`, alongside a `manifest.json` recording the exact release, byte range and checksum used. File contents are stored exactly as they appear in the archive — gzipped readsb [trace JSON](https://github.com/wiedehopf/readsb/blob/dev/README-json.md#trace-jsons), unparsed and uncleaned. Only the name gains its true `.gz` suffix, because tools that pick a decompression codec from the suffix (Spark among them) otherwise read the gzip bytes as text.

Use `--tag` for a different day, `--bytes` for a larger sample. `data/` is gitignored, so this step is how you reproduce the dataset locally.

#### Putting it in object storage (new pipeline)

Raw data lives in an S3 bucket, served locally by [MinIO](https://min.io) so the project runs offline with no cloud account. MinIO speaks the S3 API, so the pipeline uses the same `s3a://` Spark connector and the same boto3 client it would use against AWS — there is one implementation, not one per backend.

```bash
docker compose up -d minio
docker compose run --rm spark python -m adsb.upload_raw
```

That creates the bucket if needed and copies the local sample into it, preserving the layout: `data/raw/adsb/<tag>/...` becomes `s3a://adsb/raw/adsb/<tag>/...`. The MinIO console is at http://localhost:9001.

Credentials default to `minioadmin`/`minioadmin` — local dev values, not secrets. Override them (and the bucket) with `S3_ACCESS_KEY`, `S3_SECRET_KEY` and `S3_BUCKET` in a `.env` file.

To run against real AWS S3 instead, drop `S3_ENDPOINT` and supply AWS credentials; no code changes.

#### Processing it with Spark (new pipeline)

PySpark runs in local mode inside its own container — separate from the Streamlit image, which stays Spark-free. It reads the gzipped traces, explodes each aircraft's trace into one row per position report, and aggregates:

```bash
docker compose run --rm spark
```

It reads from `s3a://adsb/raw/adsb` by default (`ADSB_RAW_URI`). Point it at a local directory with `--path data/raw/adsb` — object storage and the local filesystem are the same code path.

#### Bronze Delta table (new pipeline)

Bronze decodes the raw traces into one row per observation and writes them as a [Delta](https://delta.io) table:

```bash
docker compose run --rm spark python -m adsb.bronze
```

It writes `s3a://adsb/bronze/observations` (`ADSB_BRONZE_URI`), then reads the table back and prints its schema and Delta history.

Bronze stays close to the source: nothing is filtered and nothing is corrected — impossible speeds and missing fields are all preserved, because judging them is data quality's job, not Bronze's. The only normalization is technical: decoding the positional trace arrays into typed columns, materializing `event_time` from the day-start epoch plus each point's offset, and splitting altitude's `number | "ground"` union into `altitude_ft` plus an `on_ground` flag. Each row carries `source_file`, `release_tag` and `ingested_at` for lineage.

The table is **not partitioned**. The sample is a single day of ~300k rows (~9 MB across 12 files), so partitioning by date would create exactly one partition and partitioning by aircraft would create 224 tiny ones. Partition by `event_date` once there is more than one day of data.

#### Silver Delta table (new pipeline)

Silver cleans Bronze into observations fit for trajectory work:

```bash
docker compose run --rm spark python -m adsb.silver
```

It writes `s3a://adsb/silver/observations` (`ADSB_SILVER_URI`). Each transformation addresses a defect measured in the Bronze sample:

| Transformation | Rows affected |
|---|---|
| Deduplicate to one row per `(icao, event_time)` | 473 removed |
| Flag non-ICAO addresses (`~` prefix) as `is_icao_address = false` | 756 flagged, none dropped |
| Implausible ground speed (> 700 kt) → `NULL` | 29 in Bronze |
| Implausible vertical rate (> 20,000 fpm) → `NULL` | 1 |
| Blank callsign → `NULL` | 12 |

Coordinate ranges, timestamp normalization and column types were profiled and found already clean, so no no-op guards were added for them. Bad *values* are nulled rather than their rows dropped, so a glitched speed never costs a valid position fix.

#### Inferred flight segments (new pipeline)

```bash
docker compose run --rm spark python -m adsb.flights
```

Writes `s3a://adsb/silver/flight_segments` (`ADSB_FLIGHTS_URI`): 737 segments from 300,865 observations across 224 aircraft, median 36 minutes.

**These are not flight records.** Nothing here comes from a flight plan, an airline schedule or an airport database. A *flight segment* is a period during which one transponder was tracked continuously by the adsb.lol receiver network — an inference from radio observations, usually corresponding to one flight but not guaranteed to.

**Algorithm.** Order each aircraft's observations by time; start a new segment when the gap to the previous observation exceeds 15 minutes. That is the whole rule.

The threshold was measured, not guessed. Median gap between observations is 4 s and the 99th percentile is 35 s, so 15 minutes is far outside normal tracking cadence. Segments containing more than one callsign — the signature of two flights merged into one segment — hold steady at 14–19 for thresholds from 5 to 30 minutes, then jump to 47 at 60 minutes and 84 at 120. Below 10 minutes, single-observation fragments grow instead. Tune with `--gap-seconds`.

Ground state is recorded but **not** used for segmentation: only 103 of 224 aircraft have any on-ground observation, so takeoff/landing is not reliably detectable. Callsign changes are not used either — of 249 changes, only 24 coincide with a gap over ten minutes, so splitting on them would invent boundaries mid-flight.

**Limitations.**

- A long coverage hole splits one real flight into two segments (oceanic legs especially).
- An aircraft tracked continuously through a turnaround yields one segment covering two real flights — 15 segments carry more than one callsign, including a 19-hour US-domestic segment with two.
- Segments are clipped by the processed day, so flights crossing midnight are truncated.
- 6 segments have a single observation; `n_observations` is published so callers can decide what is usable.
- Non-ICAO (`~`) addresses are TIS-B/ADS-R relays that can shadow real aircraft. Kept and flagged, not dropped.

#### Gold: inferred daily airport operations (new pipeline)

```bash
docker compose run --rm spark python -m adsb.airports   # once: airport reference data
docker compose run --rm spark python -m adsb.gold
```

Writes `s3a://adsb/gold/airport_daily_operations` (`ADSB_GOLD_URI`), one row per airport per day: `arrivals`, `departures`, `total_operations`, `unique_aircraft`, plus airport identity and coordinates for mapping, and first/last operation times.

**These are not official airport statistics.** They are counts of aircraft movements observed by a volunteer ADS-B receiver network and attributed to an airport by proximity. No flight plan, airline schedule or airport database is involved, and they will not reconcile with published movement counts. Every row carries `metric_source = 'adsb_inferred'` so the distinction survives into BI.

**Attribution rule.** A segment's first observation becomes a *departure*, its last an *arrival*, when that endpoint is within **5 km** of a large or medium airport and either on the ground or below **5,000 ft above that airport's elevation**. Nearest qualifying airport wins.

Both thresholds were calibrated on the data. Segments that began on the ground — by definition at an airport — lie a median 1.0 km and p90 2.3 km from the nearest large/medium airport; 5 km captures 93.7% of them and 10 km adds only 1.1%. Segments starting above 5,000 ft sit a median 46.4 km away, so the height test is what excludes overflights. Height is measured above airport elevation, not sea level, so Denver behaves like Amsterdam.

Airport reference data is [OurAirports](https://ourairports.com/data/) (public domain), filtered to large and medium airports.

**Limitations.**

- Only large/medium airports are candidates; movements at small strips and heliports are uncounted, and an aircraft at one may be attributed to a larger airport within 5 km.
- Segments that never move are excluded — this removes the fixed ground transmitters in the source (some typed `TWR`), which sit *at* airports and would otherwise inflate these very counts.
- A segment can produce both a departure and an arrival at the same airport (10,944 do). Some are genuine local flights, some are coverage-split artifacts; they are not separated.
- Everything inherited from reconstruction: turnarounds merged into one segment, coverage-hole splits, midnight truncation.
- Absolute counts are **not** calibrated against official figures and undercount wherever ADS-B coverage is thin.

#### Flight analytical model (new pipeline)

```bash
docker compose run --rm spark python -m adsb.flight_model
```

Writes two tables from one segmentation pass, so the trajectory and the summary can never disagree about where a flight starts:

- **`s3a://adsb/silver/flights`** — one row per inferred flight: identity, callsign, `airline_icao`, aircraft type, tracking bounds, summary statistics, and matched departure/arrival airports.
- **`s3a://adsb/silver/flight_observations`** — one row per position report, tagged with `flight_id` and `observation_seq`. This exists because a trajectory cannot be redrawn from a summary; the flight table alone would make a map impossible.

**`flight_id` = `<icao>_<yyyyMMddHHmmss of first observation>`**, e.g. `a4b41c_20251230002514`. Deterministic — a function of the data alone, so reprocessing a day regenerates identical ids. Both halves are needed: the address repeats across a day's flights, and the timestamp is not unique across aircraft.

**What counts as a flight** is unchanged from reconstruction: a period of continuous tracking, split on gaps over 15 minutes. It is an observation artefact, usually but not always one real flight.

**Which fields you can trust:**

| Kind | Fields | Populated |
|---|---|---|
| Authoritative (transmitted) | `icao`, `event_time`, `latitude`, `longitude`, `on_ground` | 100% |
| Authoritative (transmitted) | `ground_speed_kt` / `track_deg` / `altitude_ft` / `vertical_rate_fpm` | 99 / 95 / 88 / 89% |
| Reference lookup (readsb database, not transmitted) | `registration`, `aircraft_type`, `registered_owner` | 93 / 92 / 45% of flights |
| Inferred by this pipeline | `flight_id`, `airline_icao`, departure/arrival airports | — |

##### Flight tracks: the trajectory dataset

`silver.flight_observations` **is** the flight-track dataset — there is no separate `flight_tracks` table, because building one would copy 1.29 GB and filter out nothing. Measured over all 44,619,824 points:

| Check | Count |
|---|---|
| Missing position | 0 |
| Coordinates out of range | 0 |
| Null island (0,0) | 0 |
| Missing timestamp or `flight_id` | 0 |
| Duplicate `(flight_id, event_time)` | 0 |
| Flights whose `observation_seq` isn't 1..n | 0 |

**Ordering.** Order by `flight_id, observation_seq` (or `flight_id, event_time`). `observation_seq` is a gap-free 1..n per flight. Its window orders by `event_time, latitude, longitude` — the position is a tie-break so the order is reproducible even if two observations ever shared a timestamp, which Silver's `(icao, event_time)` uniqueness currently prevents. Two quality checks enforce it: `(flight_id, event_time)` unique and `(flight_id, observation_seq)` unique.

**Nothing is dropped.** 305,735 points (0.7%) carry a position but no altitude, speed or track. They are still valid trajectory vertices and are kept; the columns are simply null. Invalid coordinates are *asserted against*, not silently repaired — if a future release contains any, the pipeline fails rather than quietly reshaping the data.

**Measured retrieval** (44.6M rows, 1.29 GB, 68 files, local MinIO):

| Access pattern | Time |
|---|---|
| Flight list for filtering (107,630 rows) | 0.30 s |
| Filtered flight list (airline + date) | 0.63 s |
| **One flight's trajectory, ordered** | **1.97 s** |
| Five flights' trajectories | 1.18 s |

Fast enough for an interactive dashboard, so the table is left unpartitioned beyond `release_date` and no Z-ordering or clustering was added. Revisit if the data grows by an order of magnitude.

**Intended consumers.** Streamlit reads `silver.flights` for the filter lists and one or a few `flight_id`s from `silver.flight_observations` for the map. Power BI can consume `silver.flights` directly; the point-level table is large for import mode and is better filtered to a day or an airport first. Phase-detection and holding-pattern logic will read the point table ordered by `flight_id, observation_seq` — `altitude_ft`, `vertical_rate_fpm`, `ground_speed_kt` and `track_deg` are all carried for that purpose.

**Known limitations.**

- Airports resolve for **60.6%** of flights and **both** ends for only **26.2%**. Those columns are nullable and often null — consumers must show "unknown", not drop the flight.
- `registered_owner` is the registry owner, **not** the operating airline: it is full of leasing trusts ("BANK OF UTAH TRUSTEE" appears on ~2,000 flights). Use `airline_icao` for airline questions.
- `airline_icao` is a code, not a name. Resolving `SWR` → "Swiss" needs an airline reference dataset this project deliberately does not ship; the column is designed so a name can be joined on later.
- `first_seen_time` / `last_seen_time` are when *tracking* started and stopped — not departure and arrival. Only `departure_time` / `arrival_time` mean that, and only when an airport matched.
- Everything inherited from reconstruction: coverage-hole splits, turnarounds merged into one flight, clipping at midnight.

#### Flight phases (new pipeline)

```bash
docker compose run --rm spark python -m adsb.phases
```

Writes `s3a://adsb/gold/flight_phases` (`ADSB_PHASES_URI`): one row per contiguous phase of a flight, so a flight has several rows. Phases are deliberately *not* folded into `silver.flights` — a flight has many, and flattening would force an arbitrary choice of which one.

**These are inferred, not operational records.** Every phase is deduced from radio observations of altitude, vertical rate and the ground flag. Nothing comes from a flight plan, an airline system or an ATC record. A phase boundary is where the *evidence* changes, which is close to but not identical with where the aircraft actually changed regime.

**Phases**: `taxi_out`, `climb`, `cruise`, `descent`, `taxi_in`, `taxi`, `unknown`.

**Algorithm.** Smooth the vertical rate over a **time** window, label each observation, then collapse consecutive identical labels into intervals. On the ground → `taxi_out` / `taxi_in` / `taxi` depending on whether the point falls before, after or between that flight's airborne observations. Airborne → `climb` / `descent` outside the level band, `cruise` inside it. No evidence → `unknown`.

The legacy Streamlit implementation was the starting point, but two of its assumptions do not transfer and were re-derived:

- Its `8.3` vertical-rate threshold is **metres per second** (OpenSky units); this pipeline carries feet per minute.
- Its `rolling(40)` averaged 40 *rows*, which suited OpenSky's 1 Hz sampling. Here the within-flight gap has a median of 4 s but a mean of 8.1 s and a p90 of 20 s, so 40 rows would span roughly five minutes for some flights and forty seconds for others. The window is now defined in **seconds** (`RANGE BETWEEN`), averaging the same amount of flight time regardless of observation density.

**Two thresholds, both named constants and both adjustable** (`--level-band-fpm`, `--smoothing-seconds`):

| Constant | Value | Why |
|---|---|---|
| `LEVEL_BAND_FPM` | 300 | Airborne vertical rates have p25 = −640 and p75 = +512 fpm, so ±300 sits well inside genuine climbs and descents while matching the conventional level-flight tolerance |
| `SMOOTHING_WINDOW_SECONDS` | 60 | Long enough to suppress sample-to-sample noise, short enough not to blur a real top-of-climb |

**Not detected: takeoff, landing, approach.** The data supports "on the ground" vs "airborne" and the sign of the vertical rate; it does not support a defensible takeoff-roll or final-approach boundary without runway geometry. The ground-to-air transition is already visible as `taxi_out` → `climb`, and wrapping a fixed-duration "takeoff" around it would add a threshold with no evidence behind it.

**Results on the sample**: 740,488 phase runs over all 107,630 flights. Mean vertical rate per phase comes out at +1,148 fpm for `climb`, −843 for `descent` and −29 for `cruise`, and taxi-out runs a median 614 s against taxi-in's 236 s — the real departure-queue asymmetry. Neither was encoded anywhere.

**Fragmentation.** A flight has a median of 5 phase runs (p90 14, p99 43). 32.3% of runs are under 60 s and 7.7% are a single observation. Much of that is genuine: a real PHL→DFW flight shows a 57-second `cruise` at 10,100 ft (the 10,000 ft speed-restriction level-off) and a 48-second one at 5,525 ft (an ATC step-down). Some is not: a step-down descent can alternate `descent`/`cruise` several times. No minimum-duration merging is applied, because separating a real level-off from flicker needs a threshold the data does not yet justify — consumers wanting only substantial phases should filter on `duration_seconds` or `n_observations`.

**Limitations.** Taxi phases need ground observations, which only **43.9%** of flights have — the rest simply begin in `climb` or `cruise`. A flight tracked through a turnaround yields a mid-flight `taxi` run. `cruise` means level airborne flight at *any* altitude, so at low altitude it may be a level-off or a circuit. `unknown` appears where vertical rate is absent across a whole smoothing window, which is honest rather than a guess. Everything inherited from reconstruction propagates here.

#### Data quality checks (new pipeline)

Every stage validates its own output before finishing, and the whole set can be re-run against the published tables:

```bash
docker compose run --rm spark python -m adsb.quality
```

48 checks across the four tables. A check is a SQL predicate matching *invalid* rows; a table's checks are counted in one pass, and a failure raises `DataQualityError` naming every check that failed, not just the first. No framework — the rules live next to the transformations they protect.

The rules come from failure modes this pipeline actually exhibited:

- **Silent emptiness.** A mistyped URI or empty bucket yields zero rows and every downstream table then builds successfully and empty. Each table asserts it is non-empty.
- **Silent string corruption.** `release_tag` is filled by a regex that once quietly produced `''` for an unexpected path shape. Empty-string checks exist because that happened.
- **Invariants that hold by construction and would otherwise go unverified**: one Silver row per `(icao, event_time)`; segments never end before they start; every observation lands in exactly one segment; `arrivals + departures = total_operations`.
- **Coordinate ranges.** Silver deliberately does not *correct* coordinates, since profiling found none out of range — but that is a statement about one release, so it is asserted rather than assumed.

Layer rules differ on purpose: Bronze keeps the source's implausible values and is checked only for structural integrity, while Silver promises those values are gone and is checked for exactly that.

#### Incremental, idempotent processing (new pipeline)

One adsb.lol release is one UTC day, so the day is the unit of work. Every table is partitioned by `release_date`, and each stage can process a single day, replacing only that day's partition:

```bash
TAG=v2025.12.29-planes-readsb-prod-0
DAY=2025-12-29

python -m adsb.ingest --tag $TAG                      # download one day
docker compose run --rm spark python -m adsb.upload_raw --tag $TAG
docker compose run --rm spark python -m adsb.bronze   --tag $TAG
docker compose run --rm spark python -m adsb.silver   --release-date $DAY
docker compose run --rm spark python -m adsb.flights  --release-date $DAY
docker compose run --rm spark python -m adsb.gold     --release-date $DAY
```

Omit the flags and a stage rebuilds its whole table, which is how the first day is loaded and how a schema change is rolled out.

**`replaceWhere`, not MERGE.** Reprocessing a day should reproduce that day exactly, not reconcile it row by row. `replaceWhere` atomically swaps one partition's files and leaves every other partition alone — precisely "rebuild this day, keep the others". MERGE would be the right tool if we received corrections to individual observations; we receive whole days.

**`release_date` comes from the release identifier, not from row timestamps.** This matters: a key derived from the data would let a stray observation near midnight pull a neighbouring day's partition into the write, so reprocessing day N could destroy part of day N−1. Deriving it from the release makes that impossible, and Delta rejects a write whose rows fall outside the predicate.

Measured on 44.4M rows of day 1 plus a small day 2, from the Delta transaction log:

| Version | Operation | Rows written | Files added | Files removed |
|---|---|---|---|---|
| v6 | full rebuild, day 1 | 44,398,534 | 983 | 0 |
| v7 | add day 2 | 293,728 | 12 | **0** |
| v8 | reprocess day 2 | 293,728 | 12 | **12** |

Adding a day removed no existing files — day 1 was never rewritten. Reprocessing removed exactly the 12 files it had previously written, and row counts were unchanged.

Run the tests the same way:

```bash
docker compose run --rm spark pytest -q
```

---

### Dashboard Preview

![Main Dashboard](docs/dashboard_preview.png)

---

## Project Overview

As the aviation industry generates a lot of ADS-B data daily, raw telemetry is often noisy, unstructured, and difficult to query efficiently. This project tries to solve these challenges by implementing a robust **ETL pipeline** and an interactive **analytical dashboard**.

### Key Features

*   **Big Data Processing:** Capable of ingesting and cleaning 24+ hours of global flight data (47M+ rows) into a queryable format.
*   **Physics-Based Data Cleaning:** Custom algorithms filter out sensor glitches, impossible altitudes, and "teleporting" aircraft using velocity and vertical rate sanity checks.
*   **Interactive 4D Map:** High-performance trajectory visualization using **Plotly & Mapbox**, capable of rendering thousands of flights with scroll-to-zoom and click interactions.
*   **Operational KPIs:** Automated calculation of Taxi-Out duration, Holding Patterns (Level-Offs), and Hourly Throughput.
*   **Flight Inspector:** Drill-down capability to visualize specific flight profiles (Altitude vs. Ground Speed over time).

## Scope & Data Limitations

* **Geographic Focus:** While the pipeline is architecture-agnostic, this demonstration is currently configured to analyze operations specifically at **Zurich Airport (LSZH)**.
* **Data Precision:** This project utilizes raw, historical ADS-B state vectors. This data source inherently contains noise, including signal gaps, barometric altitude drift, and sparse ground coverage. While the **Physics Engine** successfully filters out "impossible" movements (glitches), users should treat the metrics as operational estimates rather than forensic-grade flight reconstructions.

## Technical Architecture

The project follows a decoupled architecture separating heavy data processing from the interactive frontend.

*   **ETL Engine:** A standalone Python script that ingests raw `.tar.gz` CSV dumps, validates ICAO airline codes, corrects unit mismatches, and serializes data into **Apache Parquet**.
*   **Optimization Strategy:** Uses **Pushdown Predicates** to load only relevant data slices (e.g., specific airlines) into memory, reducing RAM usage by ~90% compared to standard Pandas loading.
*   **Visualization Layer:** Streamlit interface with vectorized `groupby` operations for real-time analytics.

```mermaid
graph LR
    A[Raw OpenSky Data .csv.tar] -->|Extract & Validate| B(ETL Container)
    B -->|Physics Filtering| C(Master Parquet File)
    C -->|Pushdown Filter| D[Streamlit App]
    D -->|Interactive Viz| E[User Dashboard]
```

## Engineering Challenges Solved

This project addresses several real-world data engineering problems:

1.  **Raw Data Quality:** The source data contained mixed hex codes and callsigns in the same column. Implemented a strict Regex validation (`^[A-Z]{3}$`) to isolate valid commercial traffic.
2.  **Sensor Noise:** ADS-B data often contains altitude spikes (e.g., 40k ft jumps in 1s). Implemented a physics engine to discard data points requiring > Mach 1 speed or > 150 ft/s vertical speed.
3.  **Memory Management:** Loading 24h of flight data crashed standard 8GB containers. Implemented Parquet partitioning and lazy loading to keep the app lightweight and fast.
4.  **Rendering Performance:** Plotly struggles with thousands of traces. Implemented **Trace Consolidation** (grouping flights by phase into single API calls) to boost rendering FPS by 100x.

## Installation & Setup

### Prerequisites

1.  **Docker & Docker Compose** installed.
2.  A free **Mapbox Public Access Token** (Get it at [mapbox.com](https://www.mapbox.com)).
3.  Raw flight data from [OpenSky Network](https://opensky-network.org/datasets/states/) (e.g., `states_2017-06-05-00.csv.tar`).

### Quick Start

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/arthurgygax/aircraft-ops-analytics.git
    cd aircraft-ops-analytics
    ```

2.  **Configure Environment:**
    Create a `.env` file in the root directory.
    ```bash
    echo "MAPBOX_TOKEN=pk.eyJ1IjoieW91ci...token_here" > .env
    ```

3.  **Load Data:**
    Download hourly state vectors from OpenSky and place the `.tar` files in `data/raw/`.

4.  **Run the ETL Pipeline:**
    This command spins up a temporary container to clean and convert the data.
    ```bash
    docker-compose run --rm converter
    ```
    *Output: A highly optimized `master_flight_data.parquet` file in `data/processed/`.*

5.  **Launch the Dashboard:**
    ```bash
    docker-compose up app
    ```
    Access the app at **http://localhost:8501**.

## Project Structure

```bash
aircraft-ops-analytics/
├── data/
│   ├── raw/                   # Place downloaded .csv.tar files here
│   └── processed/             # Generated Parquet files live here
├── docker/
│   └── Dockerfile             # Shared image for App and Converter
├── src/
│   ├── adsb/                  # New ADS-B pipeline (adsb.lol source, in development)
│   ├── app.py                 # Main Streamlit dashboard entry point
│   ├── convert_to_parquet.py  # ETL logic & Data Cleaning pipeline
│   ├── logic.py               # Physics filtering & Phase detection algorithms
│   └── viz.py                 # Plotly visualization components
├── tests/                     # Tests for the new pipeline
├── docker-compose.yml         # Service orchestration
├── requirements.txt           # Python dependencies
└── requirements-dev.txt       # Dependencies for running the tests
```

Run the tests for the new pipeline with:

```bash
pip install -r requirements-dev.txt && pytest
```

## Analytics Modules

### 1. Flight Inspector & Profile Analysis
Drill down into individual flights to analyze climb performance. The **Physics Engine** automatically cleans sensor noise to produce accurate Altitude (Blue) vs. Ground Speed (Red) profiles.

![Flight Inspector](docs/feature_inspector.png)

---

### 2. Airport Congestion & Ground Radar
A high-resolution density heatmap focuses on ground movements (Taxi phase) to identify taxiway bottlenecks and runway congestion points.

![Ground Radar](docs/feature_radar.png)

---

### 3. Operational Efficiency Metrics
comparative analysis of airline performance (Taxi Time vs. Volume) and hourly throughput capacity.

## License

Distributed under the MIT License.
