# Pipeline reference

Stage-by-stage detail for the ADS-B pipeline: what each step does, the
thresholds it uses, and how those thresholds were calibrated against the real
data. See [../README.md](../README.md) for the overview and
[gold-model.md](gold-model.md) for the analytical model.

Every command below runs in the `spark` container:
`docker compose run --rm spark python -m adsb.<module>`.

---

## Getting the sample data

Each adsb.lol release is one day of global flight data, ~3.2 GB, published as an uncompressed tar split across two GitHub assets. We do not download a whole one. Because tar is sequential, a byte-range request for a slice of the first part yields whole, valid members — so ingestion locates the `./traces/` region by walking the tar headers, then downloads a small window from it:

```bash
PYTHONPATH=src python3 -m adsb.ingest
```

Ingestion needs no third-party packages — it is standard library only, so it
runs on the host without a virtualenv or a container.

This fetches ~8 MB (about 220 aircraft trace files, ~10 s) into `data/raw/adsb/<release-tag>/traces/`, alongside a `manifest.json` recording the exact release, byte range and checksum used. File contents are stored exactly as they appear in the archive — gzipped readsb [trace JSON](https://github.com/wiedehopf/readsb/blob/dev/README-json.md#trace-jsons), unparsed and uncleaned. Only the name gains its true `.gz` suffix, because tools that pick a decompression codec from the suffix (Spark among them) otherwise read the gzip bytes as text.

Use `--tag` for a different day, `--bytes` for a larger sample. `data/` is gitignored, so this step is how you reproduce the dataset locally.

## Putting it in object storage

Raw data lives in an S3 bucket, served locally by [MinIO](https://min.io) so the project runs offline with no cloud account. MinIO speaks the S3 API, so the pipeline uses the same `s3a://` Spark connector and the same boto3 client it would use against AWS — there is one implementation, not one per backend.

```bash
docker compose up -d minio
docker compose run --rm spark python -m adsb.upload_raw
```

That creates the bucket if needed and copies the local sample into it, preserving the layout: `data/raw/adsb/<tag>/...` becomes `s3a://adsb/raw/adsb/<tag>/...`. The MinIO console is at http://localhost:9001.

Credentials default to `minioadmin`/`minioadmin` — local dev values, not secrets. Override them (and the bucket) with `S3_ACCESS_KEY`, `S3_SECRET_KEY` and `S3_BUCKET` in a `.env` file.

To run against real AWS S3 instead, drop `S3_ENDPOINT` and supply AWS credentials; no code changes.

## Processing it with Spark

PySpark runs in local mode inside its own container — separate from the Streamlit image, which stays Spark-free. It reads the gzipped traces, explodes each aircraft's trace into one row per position report, and aggregates:

```bash
docker compose run --rm spark
```

It reads from `s3a://adsb/raw/adsb` by default (`ADSB_RAW_URI`). Point it at a local directory with `--path data/raw/adsb` — object storage and the local filesystem are the same code path.

## The observation table

```bash
docker compose run --rm spark python -m adsb.observations \
    --tag v2025.12.30-planes-readsb-prod-0 --full-rebuild
```

Writes `s3a://adsb/observations` (`ADSB_OBSERVATIONS_URI`): **one row per accepted ADS-B reception**, keyed `(flight_id, observation_seq)` and unique on `(icao, event_time)`. It is the only point-grain table in the pipeline. Decoding, cleaning, deduplication and flight segmentation all happen in this one pass.

**Decoding.** The positional trace arrays become named typed columns, because `array<array<string>>` is not something you can query or evolve. `event_time` is materialized from the day-start epoch plus each point's offset, since the raw offset is meaningless without its file's header — and it keeps microsecond precision, which matters (see below). Altitude's `number | "ground"` union is split into a numeric `altitude_ft` plus a boolean `on_ground`, because a column has one type.

**Deduplication, and why it happens here.** `readsb` records one trace point per reception. When a position arrives twice in the same hundredth of a second, two points share an `(icao, event_time)`. Measured across both releases: **72,438 rows, 0.162% of the data.**

They are not an ingestion defect and not a precision artefact. They are a property of *rebroadcast* ADS-B:

| Reception path | Rows | Duplicate rate |
|---|---|---|
| `adsb_icao` — direct | 41,783,346 | **0.012%** |
| `adsb_icao_nt` | 898,834 | 1.98% |
| `adsr_icao` — ADS-R relay | 944,314 | 3.85% |
| `adsr_other` | 3,190 | 19.6% |
| `tisb_other` — TIS-B relay | 49,394 | **22.2%** |

A relay rebroadcasts a position the network also received directly; both land in the same 10 ms tick, a median 1.8 m apart. Non-ICAO `~` addresses — TIS-B/ADS-R shadows — duplicate at 11.6%, seventy times the baseline.

Every one of the 72,277 colliding keys is a pair of **adjacent entries in one aircraft's own trace array**, without exception. Resolving them is therefore a within-row operation on the array, which is why it happens during decoding: `sort_array` orders each aircraft's points and a `filter` keeps the first of each timestamp. Nothing shuffles. The previous implementation exploded the arrays into 44.7M rows, wrote them, read them back, and then re-partitioned all of them by `icao` to rediscover an adjacency the source had already given for free.

**The winner rule, stated rather than implied.** Of two receptions sharing a timestamp: keep the more complete row (how many of altitude, ground speed, track, vertical rate and callsign are present), then the lower `(latitude, longitude)`, then the earlier point in the source array. The completeness score **ties in 71.1% of cases**, so most of the time the rule reduces to "keep whichever reception is further south, then west". That is deterministic and reproducible, not objectively preferable — the two rows are a median 1.8 m apart. Every run prints the collisions it resolved, so the number is observable rather than being the difference between two row counts; a jump means either a change in the source's relay mix or the same raw file reaching the pipeline twice, and the second of those must not look like the first.

**Sub-second precision is load-bearing.** `event_time` keeps microseconds. Were it truncated to whole seconds, the same dedup would remove **2,368,555 rows (5.33%)** instead of 72,438 — silently discarding 2.3M legitimate positions a median 32 m apart.

**Cleaning.** Two classes of impossible value are nulled, both measured rather than assumed: ground speed over 700 kt (the fastest credible reports in the sample are B788s at 657 kt; past 700 is a glitch) and vertical rate over 20,000 fpm. Bad *values* are nulled rather than their rows dropped, so a glitched speed never costs a valid position fix. A padded-empty callsign becomes NULL. Non-ICAO `~` addresses are flagged via `is_icao_address`, never dropped. Coordinate ranges are asserted in `adsb.quality`, never silently repaired.

**Segmentation.** Order each aircraft's observations by time and start a new flight whenever the gap to the previous one exceeds 15 minutes. That is the whole rule, and it is the pipeline's one genuinely global window — irreducible, because reconstructing flights from tracking gaps is what it means.

The threshold was measured, not guessed. Median gap between observations is 4 s and the 99th percentile is 35 s, so 15 minutes is far outside normal cadence. Flights containing more than one callsign — the signature of two real flights merged — hold steady at 14–19 for thresholds from 5 to 30 minutes, then jump to 47 at 60 and 84 at 120. Below 10 minutes, single-observation fragments grow instead. Tune with `--gap-seconds`.

Ground state is recorded but **not** used for segmentation: only 43.9% of flights have any on-ground observation, so takeoff/landing is not reliably detectable. Callsign changes are not used either — of 249 changes in the development sample, only 24 coincided with a gap over ten minutes, so splitting on them would invent boundaries mid-flight.

`readsb` sets its own "new leg" bit on trace points, which looks like a free replacement for the gap rule. It is not: the release carries 45,906 leg-marked points against 75,561 gap breaks, and 92.8% of the markers follow a gap already longer than 900 s. It is a coarser subset of what the gap rule finds, so the gap rule stays.

**Ordering.** Order by `flight_id, observation_seq` (or `flight_id, event_time`). `observation_seq` is a gap-free 1..n per flight. Its window orders by `event_time, latitude, longitude` — the position is a tie-break so the order stays reproducible whatever order Spark read the files in. Two quality checks enforce it: `(icao, event_time)` unique and `(flight_id, observation_seq)` unique.

**Nothing is dropped.** 305,735 points (0.7%) carry a position but no altitude, speed or track. They are still valid trajectory vertices and are kept; the columns are simply null.

**Limitations.**

- A long coverage hole splits one real flight into two — oceanic legs especially.
- An aircraft tracked continuously through a turnaround yields one flight covering two real ones; `n_callsigns` on the flight table exposes the signature.
- Flights are clipped by the processed day, so one crossing midnight is truncated.
- A flight may be a single observation; `n_observations` is published so callers can decide what is usable.

## The flight table

```bash
docker compose run --rm spark python -m adsb.flights --release-date 2025-12-30
```

Writes `s3a://adsb/movements` and `s3a://adsb/flights` (`ADSB_MOVEMENTS_URI`, `ADSB_FLIGHTS_URI`). One row per inferred flight, aggregated straight from `observations` — there is no intermediate segments table, because everything such a table would hold survives into this one.

**These are not flight records.** Nothing here comes from a flight plan, an airline schedule or an airport database. A *flight* is a period during which one transponder was tracked continuously — an inference from radio observations.

**`flight_id` = `<icao>_<yyyyMMddHHmmss of first observation>`**, e.g. `a4b41c_20251230002514`. Deterministic — a function of the data alone, so reprocessing a day regenerates identical ids. Both halves are needed: the address repeats across a day's flights, and the timestamp is not unique across aircraft.

**Which fields you can trust:**

| Kind | Fields | Populated |
|---|---|---|
| Authoritative (transmitted) | `icao`, `event_time`, `latitude`, `longitude`, `on_ground` | 100% |
| Authoritative (transmitted) | `ground_speed_kt` / `track_deg` / `altitude_ft` / `vertical_rate_fpm` | 99 / 95 / 88 / 89% |
| Reference lookup (readsb database, not transmitted) | `registration`, `aircraft_type`, `registered_owner` | 93 / 92 / 45% of flights |
| Inferred by this pipeline | `flight_id`, `airline_icao`, departure/arrival airports, hold rollups | — |

**Movements are a table, not a reshape.** A flight's first observation becomes a *departure* and its last an *arrival* when that endpoint is within 5 km of a large or medium airport and either on the ground or below 5,000 ft above the airport's own elevation. The nearest qualifying airport wins. Both thresholds were calibrated on the data: flights that began on the ground lie a median 1.0 km and a 90th percentile 2.3 km from the nearest airport, a 5 km radius captures 93.7% of them, and widening to 10 km adds only 1.1%; flights starting above 5,000 ft sit a median 46.4 km away, so the height test is what keeps overflights out. Height is measured above the airport's elevation, not above sea level, so high-altitude airports behave like sea-level ones.

The result is persisted at movement grain (92,951 rows) because three consumers need it — the flight table's airport columns, the daily airport rollup, and the Power BI model, which reports on movement grain so "arrivals vs departures" is a slicer rather than two columns. It used to be computed once for each of them and stored for none.

**Measured retrieval** (44.6M-row point table, local MinIO):

| Access pattern | Time |
|---|---|
| Flight list for filtering (107,630 rows) | 0.30 s |
| Filtered flight list (airline + date) | 0.63 s |
| **One flight's trajectory, ordered** | **1.97 s** |
| Five flights' trajectories | 1.18 s |

Fast enough for an interactive dashboard, so `observations` is left unpartitioned beyond `release_date` and no Z-ordering or clustering was added. Revisit if the data grows by an order of magnitude.

**Known limitations.**

- Airports resolve for **60.6%** of flights and **both** ends for only **26.2%**. Those columns are nullable and often null — consumers must show "unknown", not drop the flight.
- `registered_owner` is the registry owner, **not** the operating airline: it is full of leasing trusts ("BANK OF UTAH TRUSTEE" appears on ~2,000 flights). Use `airline_icao` for airline questions.
- `airline_icao` is a code, not a name. Resolving `SWR` → "Swiss" needs an airline reference dataset this project deliberately does not ship; the column is designed so a name can be joined on later.
- `first_seen_time` / `last_seen_time` are when *tracking* started and stopped — not departure and arrival. Only `departure_time` / `arrival_time` mean that, and only when an airport matched.
- Only large and medium airports are candidates, so movements at small strips are not counted, and an aircraft on the ground at one may be attributed to a larger airport within 5 km.
- Flights that never move are excluded from movements, which removes the fixed ground transmitters in the source (readsb reports some with an `aircraft_type` of `TWR`) as well as single-observation fragments. Those emitters sit *at* airports, so leaving them in would inflate exactly these counts.

## Inferred daily airport operations

```bash
docker compose run --rm spark python -m adsb.run_pipeline --tag <release-tag>
```

Writes `s3a://adsb/airport_daily_operations` (`ADSB_AIRPORT_OPERATIONS_URI`), one row per airport per day: `arrivals`, `departures`, `total_operations`, `unique_aircraft`, airport identity and coordinates for mapping, first/last operation times, and the hold rollups.

**These are not official airport statistics.** They are counts of aircraft movements observed by a volunteer ADS-B receiver network and attributed to an airport by proximity. No flight plan, airline schedule or airport AODB is involved, and they will not reconcile with published movement counts. Every row carries `metric_source = 'adsb_inferred'` so the distinction survives into BI.

## Flight phases

```bash
docker compose run --rm spark python -m adsb.phases --release-date 2025-12-30
```

Writes `s3a://adsb/flight_phases` (`ADSB_PHASES_URI`): one row per contiguous phase of a flight, so a flight has several rows. Phases are deliberately *not* folded into `flights` — a flight has many, and flattening would force an arbitrary choice of which one.

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

## Detected holding patterns

```bash
docker compose run --rm spark python -m adsb.holds --release-date 2025-12-30
```

Writes `s3a://adsb/flight_holds` (`ADSB_HOLDS_URI`): one row per stretch of sustained circling detected in a flight's trajectory.

**What this is.** *Observed* circling — the aircraft turned through at least a full circle while staying inside a small area, which is what a holding pattern looks like from the outside. **What it is not:** evidence that the aircraft was *instructed* to hold, was flying a published procedure, or was delayed. No flight plan, clearance or ATC record is involved, only positions and headings. The table describes ADS-B-derived observations, never holding instructions.

**Algorithm.** For each airborne observation, take the signed heading change from the previous one, wrapped into (−180, 180] so 359° → 1° reads as +2° and not −358°. Sum those changes over a **centred** six-minute window — centred rather than trailing, because a trailing window only marks a point once a whole circle has already accumulated behind it, which clips the start of every hold. The sum is **signed**, so a left turn cancelled by an equal right turn is an S-bend, not circling. Mark observations whose window reaches a full circle, collapse consecutive marks into intervals, then keep only intervals that are also sustained, confined and level.

Geometry stays deliberately primitive: a bounding box converted to kilometres with a flat-earth approximation, accurate to well under a percent over the tens of kilometres a hold spans. No geospatial library, and a reviewer can check the arithmetic by hand.

| Threshold | Value | Why |
|---|---|---|
| `TURN_WINDOW_SECONDS` | 360 | one circuit takes ~4 min low, up to 6 min higher |
| `MIN_TURN_DEGREES` | 360 | a base-to-final turn is ~90°, a procedure turn 180–270°, so a full circle already excludes ordinary approach manoeuvring |
| `MIN_DURATION_SECONDS` | 240 | one full low-level circuit |
| `MAX_SPAN_KM` | 25 | a racetrack spans roughly 15–20 km |
| `MAX_ALTITUDE_RANGE_FT` | 4000 | holds are level or step down in a stack; a spiral descent is not a hold |

Turning is normally gentle in this data — per-step turn has a median of 0.1° and a p90 of 4.3° — so sustained circling stands out. A six-minute signed turn reaching a full circle occurs somewhere in 7,128 of 97,394 flights (7.3%) before any of the filters above.

**What it found on the sample**: 4,393 detected holds across 3,017 flights. Median 5.9 minutes, 1.8 circuits, 5.8 km span, 1,782 ft. Only **2.88% of arriving flights** have one, so normal approaches are not being swept up.

**What manual inspection revealed — the limitation is not hypothetical.** Ranking detections by circuits, the strongest are *not* airline holds:

| Callsign | Type | Route | Detected |
|---|---|---|---|
| POL24 | B429 (police helicopter) | BWU → BWU | 37.7 min, 6.9 circuits, 1,097 ft |
| TRP1 | A139 (AW139 helicopter) | MTN → MTN | 78.6 min, 4.8 circuits, 1,096 ft |
| LFA320 | C172 | SFB → SFB | 8.3 min, 4.5 circuits, 3,330 ft |

These are a police orbit, a rotorcraft on task, and a training aircraft flying circuits. The median detection altitude of 1,782 ft points the same way: real airline holding is usually flown at 6,000–20,000 ft. **The detector reliably finds circling; circling is not the same thing as holding.** Two obvious further discriminators — excluding flights whose departure and arrival airport are the same, and an altitude floor — are deliberately *not* applied, because choosing them needs evidence this phase does not have.

**Known limitations — read before trusting a row.**

- **A circle is a circle.** Aerial survey, photography, training circuits and police or medical orbits produce identical geometry and dominate the strongest detections, as the table above shows. Nothing in ADS-B distinguishes their intent from a hold's.
- Airport association is the flight's **own inferred arrival airport**, not a geometric nearest-airport search, and is null whenever that inference failed (arrival airports resolve for ~42% of flights). `distance_to_arrival_airport_km` separates terminal-area circling from en-route.
- A hold sampled too sparsely may be missed entirely; `max_sample_gap_seconds` exposes that per row.
- Nothing separates a hold from a go-around that circles back, or from vectoring that happens to close a full circle.

No synthetic "confidence score" is published. `circuits`, `span_km`, `n_observations` and `max_sample_gap_seconds` are the quality indicators, and they are the actual measurements rather than a number derived from them.

## Data quality checks

Every stage validates its own output before finishing, and the whole set can be re-run against the published tables:

```bash
docker compose run --rm spark python -m adsb.quality
```

A check is a SQL predicate matching *invalid* rows; a table's checks are counted in one pass, and a failure raises `DataQualityError` naming every check that failed, not just the first. No framework — the rules live next to the transformations they protect.

The rules come from failure modes this pipeline actually exhibited:

- **Silent emptiness.** A mistyped URI or empty bucket yields zero rows and every downstream table then builds successfully and empty. Each table asserts it is non-empty.
- **Silent string corruption.** `release_tag` is filled by a regex that once quietly produced `''` for an unexpected path shape. Empty-string checks exist because that happened.
- **Invariants that hold by construction and would otherwise go unverified**: one observation per `(icao, event_time)`; flights never end before they start; every observation lands in exactly one flight; every child row's `flight_id` exists in `flights`; `arrivals + departures = total_operations`.
- **Coordinate ranges.** The pipeline deliberately does not *correct* coordinates, since profiling found none out of range — but that is a statement about two releases, so it is asserted rather than assumed.

Beside the pass/fail rules, every run publishes the number of same-timestamp receptions the decoder collapsed. It is not an invariant — it is a source-quality signal worth trending, and the tripwire that would catch the same raw file entering the pipeline twice.

## Incremental, idempotent processing

One adsb.lol release is one UTC day, so the day is the unit of work. Every table is partitioned by `release_date`, and each stage can process a single day, replacing only that day's partition:

```bash
TAG=v2025.12.29-planes-readsb-prod-0
DAY=2025-12-29

python -m adsb.ingest --tag $TAG                      # download one day
docker compose run --rm spark python -m adsb.upload_raw --tag $TAG
docker compose run --rm spark python -m adsb.run_pipeline --tag $TAG
```

**A write is scoped to one day unless you say otherwise, in as many words.** `--full-rebuild` is the only path that replaces every day, and it is how the first day is loaded and how a schema change is rolled out. That default is not cosmetic: a bare overwrite tombstones the *whole* table, and when that is what happens because an argument was omitted, every exploratory re-run leaves a full dead copy behind. Ten of those grew this project's object store to 29 GB for ~4 GB of live data.

Two more lifecycle rules follow from the same measurement. **Reclaiming is part of writing** — a run vacuums its own tombstoned files at the end, aggressively on `observations` (24 h; it is fully reproducible from raw, so its history buys nothing a rebuild would not) and at Delta's default week elsewhere. And **development differs from production in lifecycle, not in code**: set `ADSB_ROOT` to a scratch prefix and exploratory runs never touch the real tables.

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

