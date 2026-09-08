# The analytical model

The tables both applications read. See [pipeline.md](pipeline.md) for how they
are produced and [powerbi.md](powerbi.md) for consuming them from Power BI.

---

Six tables, joined on **`flight_id`** alone. No surrogate keys, no dimension
tables — at this size a star schema would cost more than it saves.

```
flights ──┬── flight_phases   (phase intervals)
 (spine)  ├── flight_holds    (detected holds)
          ├── movements       (airport events, two per flight at most)
          └── observations    (trajectory points)

airport_daily_operations   (airport × date, independent grain)
```

Row counts are for the **[study period](scope.md)** — seven days at Zurich and
Düsseldorf — which is what the published tables now hold.

| Table | Grain | Rows | Physical location |
|---|---|---|---|
| `observations` | one per reception | 3,330,291 | `s3a://adsb/observations` |
| `flights` | one per flight | 4,740 | `s3a://adsb/flights` |
| `movements` | one per flight × arrival/departure | 5,249 | `s3a://adsb/movements` |
| `flight_phases` | one per phase interval | 32,976 | `s3a://adsb/flight_phases` |
| `flight_holds` | one per detected hold | 36 | `s3a://adsb/flight_holds` |
| `airport_daily_operations` | one per airport × date | 14 | `s3a://adsb/airport_daily_operations` |

> Figures elsewhere in the docs of the form "44.6M observations", "107,630
> flights" or "4,393 holds" come from the **unscoped development runs** over a
> whole global day. They are the evidence behind the calibrated thresholds and
> are left as they were measured; they do not describe the published tables.

**There is no medallion tiering in the names, because there are no longer
tiers.** Each of these is at a grain no other table holds. The point grain used
to be written three times — Bronze, Silver and a trajectory copy — and the
flight grain three times. The reasoning per table is in
[pipeline.md](pipeline.md).

## Data dictionary

**`flights`** — the spine. Every filter the explorer offers resolves here.

| Column | Type | Notes |
|---|---|---|
| `flight_id` | string | **primary key**, `<icao>_<yyyyMMddHHmmss>`, deterministic |
| `flight_date` | date | date of the first observation — the analytical date |
| `icao` | string | aircraft address, authoritative, 100% |
| `registration`, `aircraft_type` | string | readsb database lookup, 93% / 92% |
| `callsign` | string | 92.7% |
| `airline_icao` | string | 3-letter ICAO designator from an airline-style callsign, **65.4%**; a code, not a name |
| `registered_owner` | string | registry owner — **not the airline**, frequently a leasing trust |
| `departure_airport_ident` / `_iata` / `_name` / `departure_time` | string / timestamp | inferred, **44.6%** |
| `arrival_airport_ident` / `_iata` / `_name` / `arrival_time` | string / timestamp | inferred, **42.2%** |
| `first_seen_time`, `last_seen_time` | timestamp | tracking bounds — *not* departure/arrival |
| `duration_seconds`, `n_observations` | long | |
| `max_altitude_ft`, `max_ground_speed_kt` | double | |
| `saw_ground` | boolean | any ground observation |
| `n_detected_holds`, `has_detected_hold`, `total_hold_seconds` | long / boolean / long | rollups from `flight_holds`; zero, never null |
| `release_tag`, `release_date` | string / date | provenance and partition key |

**`flight_phases`** — `flight_id`, `phase_seq`, `phase`, `start_time`, `end_time`, `duration_seconds`, `n_observations`, altitude statistics, `avg_ground_speed_kt`, `avg_vertical_rate_fpm`, `release_date`.

**`flight_holds`** — `flight_id`, `hold_seq`, `hold_start`, `hold_end`, `duration_seconds`, `n_observations`, `centroid_latitude/longitude`, `span_km`, altitude statistics, `turn_degrees`, `circuits`, `max_sample_gap_seconds`, `arrival_airport_ident/_iata`, `distance_to_arrival_airport_km`, `release_date`.

**`airport_daily_operations`** — `operations_date`, `airport_ident`, `airport_iata`, `airport_name`, `airport_type`, `iso_country`, `airport_latitude/longitude`, `arrivals`, `departures`, `total_operations`, `unique_aircraft`, `first/last_operation_time`, `flights_with_detected_holds`, `hold_rate`, `avg_hold_duration_seconds`, `release_tag`, `release_date`, `metric_source`.

`hold_rate` is flights with a detected hold divided by arrivals, and is null when there were no arrivals to divide by rather than silently zero (191 of 1,382 airport-days).

**Do not read `hold_rate` as a delay metric.** The airports with the highest rates on this sample are Vero Beach (27.4%), North Perry (35.6%), Centennial (16.4%) and Sanford (22.8%) — general-aviation and flight-training fields, where the circling being detected is circuit training, not holding. This is the Phase 13 limitation ("a circle is a circle") surfacing in aggregate. The column is honest about what it counts: flights whose trajectory contained sustained circling near their inferred arrival airport.

## The trajectory store

`observations` **is** the trajectory table. There is no `flight_tracks` beside
it and there should not be: it would be a second copy of the same rows at the
same grain, which is the one thing this model is organised to avoid. Everything
a trajectory table needs is already here.

| Requirement | Column |
|---|---|
| flight | `flight_id` (+ `observation_seq`, a gap-free 1..n giving the order) |
| timestamp | `event_time`, microsecond precision |
| position | `latitude`, `longitude` |
| altitude | `altitude_ft` (+ `on_ground`, since altitude is null on the ground) |
| speed | `ground_speed_kt` |
| track | `track_deg` |
| also carried | `icao`, `registration`, `aircraft_type`, `operator`, `callsign`, `vertical_rate_fpm`, `is_icao_address` |

**The repeated per-flight columns are not dead weight**, which is worth stating
because it looks like it. Parquet dictionary-encodes them, so `icao`,
`registration`, `aircraft_type`, `operator`, `callsign`, `release_tag` and
`is_icao_address` together cost **0.28 MB — 0.3% of the table**. Removing them
would save nothing and would break `flight_phases`, `flight_holds` and the
quality checks, all of which read this table. 80% of the bytes are
`longitude`, `latitude` and `event_time`, which *are* the trajectory:

| Column | MB | Share | Bytes/row |
|---|---:|---:|---:|
| `longitude` | 26.63 | 27.3% | 8.00 |
| `latitude` | 26.33 | 27.0% | 7.91 |
| `event_time` | 25.25 | 25.9% | 7.58 |
| `ground_speed_kt` | 5.38 | 5.5% | 1.62 |
| `track_deg` | 4.68 | 4.8% | 1.41 |
| `altitude_ft` | 3.56 | 3.6% | 1.07 |
| `vertical_rate_fpm` | 2.90 | 3.0% | 0.87 |
| `observation_seq` | 2.51 | 2.6% | 0.76 |
| everything else | 0.40 | 0.4% | 0.12 |

### Where it lives, and why it survives

`s3a://adsb/observations`, Delta, partitioned by `release_date` — physically
the Docker **named volume** `aircraft-ops-analytics_minio-data`, mounted at
`/var/lib/docker/volumes/aircraft-ops-analytics_minio-data/_data` on the host.
Nothing about it lives inside a container's writable layer or a temporary
directory.

That was verified rather than assumed. Every container was destroyed
(`docker compose down` — containers and network removed) and the tables read
back afterwards:

| Table | Rows | Delta version | Digest |
|---|---:|---:|---|
| `observations` | 3,330,291 | 9 | `f6aec02db7fa6440` → unchanged |
| `flights` | 4,740 | 8 | `74155a7fcf9abbe9` → unchanged |
| `flight_phases` | 32,976 | 9 | `d046e0f743d5de88` → unchanged |
| `flight_holds` | 36 | 5 | `151c4f78af5ec006` → unchanged |
| `airport_daily_operations` | 14 | 8 | `080e4a474183982d` → unchanged |

Named volumes are host-side state, so a machine reboot is the same story: the
files are on the host disk, not in the containers. **The one command that does
destroy it is `docker compose down -v`** (or `docker volume rm`), which takes
the 100 MB of tables and the 9 GB raw sample with it; recovering means
re-ingesting and re-running, about 2 h 30 m.

The app cannot recompute this data even if it wanted to: the explorer image has
**no PySpark and no JVM**. It reads the published Delta tables through delta-rs
and starts in a second.

### Partitioning, and what the query pattern costs

Partitioned by `release_date`, one file per day — **7 files, 97.6 MB, ~14 MB
each**, 476k rows a day on average. Not by `flight_id`: 4,740 partitions of
700 rows is the tiny-file problem by construction.

`release_date` is safe to filter on as an observation date because the two are
the same thing — **0 of 3,330,291 rows fall outside their partition's day** —
and that is now a quality rule (`observation falls in its partition day`)
rather than an assumption, since a point filed under the wrong day would be
invisible to exactly the read the app performs.

Measured on the real table, through the app's own reader:

| Query | Time | Rows |
|---|---:|---:|
| One flight, `flight_id` only | 1,538 ms | 653 |
| One flight, `flight_id` **+ date** | **320 ms** | 653 |
| 218 flights, `IN` **+ date** | **203 ms** | 161,907 |
| Whole day | 203 ms | 535,781 |
| Whole table (for reference) | 11,410 ms | 3,330,291 |

**Passing the date is worth 4.8×**, because without it delta-rs opens all seven
partitions. The pattern the app follows — filter by date, narrow by
airline/airport/type on `flights`, then fetch those `flight_id`s — is exactly
what this layout rewards, provided step 1's date reaches step 4.

Splitting a day further was measured and rejected. Sorting by `flight_id` and
range-partitioning into 4 or 8 files per day makes a single-flight read 57 ms →
21 ms warm — 36 ms, against the 1.2 s the date filter is worth, at the cost of
8× the files and a sort on every write. The day is one row group precisely
because 14 MB fits in one; there is no pruning to win inside it that matters.

## Consuming it

**Streamlit** reads `flights` for the filter lists and the selected flight's metadata, then `flight_phases` and `flight_holds` for that flight, and pulls its trajectory from `observations` filtered on `flight_id`. No reconstruction, phase detection or hold detection happens at query time — all of it is precomputed.

The app passes the selected flights' dates alongside their ids, so the read
prunes six of the seven partitions — 1,538 ms becomes 320 ms. Nothing is read
from the point table until a flight is actually selected.

**Power BI** should import `flights`, `movements`, `flight_phases`, `flight_holds` and `airport_daily_operations`; together they are well under a million rows and model naturally with `flight_id` relationships. It should **not** import the 3.3M-row `observations` table — trajectories belong in the interactive explorer, and if a map is needed in BI, filter to a day or an airport first. Hourly statistics need no extra table: `departure_time` and `arrival_time` on `flights` carry the hour, and `movements` carries one row per event for the same question at movement grain.

All columns are flat scalars — no nested structures, no serialized objects, no dashboard-specific transformations baked into the data.

