# Analytical scope

What is in the project's dataset, why, and how the boundary is enforced. See
[pipeline.md](pipeline.md) for what happens to the data once it is in, and
[gold-model.md](gold-model.md) for the analytical model built on top.

---

## The study period

**2025-12-24 to 2025-12-30 — seven consecutive UTC days.**

One adsb.lol release is one UTC day, so the period is seven releases. It is the
last full week of the 2025 archive, and it spans Christmas Eve through the
following Tuesday. That is deliberate: 25 December is the quietest day in the
European aviation calendar and the days either side of it are not, so "how does
traffic vary by day" is a question with a visible answer rather than seven
indistinguishable Tuesdays.

## The airports

**Zürich (LSZH / ZRH)** and **Düsseldorf (EDDL / DUS)**.

ICAO identifiers, because that is what the movement table keys on; the IATA
codes travel alongside them in every published table.

Two comparable European hubs of different size, in different countries, both
well inside dense receiver coverage. Two is enough to make "how does traffic
differ between ZRH and DUS" a real comparison and few enough that the resulting
dataset stays small enough to explore interactively.

## What counts as in scope

> A flight is in scope when the pipeline infers that it **departed from or
> arrived at** a study airport. Its **whole trajectory** is then in scope —
> every observation of that flight, wherever the aircraft was.

Four things are therefore in the dataset: departures from ZRH, arrivals at ZRH,
departures from DUS, arrivals at DUS.

**The filter is on flight identity, never on the position of an individual
observation.** This is the part that is easy to get wrong. A Zurich departure
to New York spends nine tenths of its trajectory outside Europe; filtering
points by distance from the airport would leave a stub climbing out of Kloten
and nothing else. So the flight is identified first, and then its trajectory is
kept in full.

Nothing is reduced to a daily aggregate, nothing is clipped to an airport
neighbourhood, and no trajectory is simplified. The observation table is the
full point grain for every in-scope flight, which is what lets the explorer
draw any trajectory in the seven-day window.

## How the boundary is enforced

Two filters, in `adsb.scope`. There have to be two, and they do different jobs.

### 1. The pre-filter — cheap, conservative, before anything is decoded

The rule above can only be evaluated once flights exist, and flights only exist
after every observation in a release has been decoded and segmented. That is
nearly the whole cost of the pipeline, and 95% of the result would be discarded
a moment later.

So a first pass drops aircraft that never came near a study airport, working on
the **raw aircraft rows** before the trace arrays are exploded:

```sql
exists(trace, p -> <position is inside a 25 km box around ZRH or DUS>)
```

It is a row-level test over each aircraft's own array, so it shuffles nothing,
and `exists` short-circuits — an aircraft over Zurich is decided on its first
few points rather than its ten-thousandth.

**It cannot drop an in-scope flight**, and that is a property rather than a
hope. A movement is attributed only when a flight endpoint lies within
`MATCH_RADIUS_KM` (5 km) of the airport; that endpoint *is* a trace point; so
every in-scope flight has at least one point inside a radius five times smaller
than the box. `airport_boxes` refuses a pre-filter radius that does not exceed
the match radius, rather than trusting the two constants to stay in step.

The box arithmetic uses the same 6,371 km sphere as the movement haversine.
Deriving `KM_PER_DEGREE` from it rather than writing `111.32` is not
pedantry — on the rounder constant a "25 km" box measured 24.97 km, which is a
silent disagreement between the two filters, and a test caught it.

### 2. The rule itself — after airport matching

Movements are computed for every candidate flight, and the flights with a
movement at a study airport are the scope. Every table is then restricted to
those `flight_id`s with a semi-join.

This is what removes the aircraft that merely overflew Zurich at 36,000 ft, and
the other legs that in-scope aircraft flew the same day — an aircraft that
operated ZRH→LHR and then LHR→CDG contributes the first flight and not the
second.

### What each table ends up holding

| Table | Scope |
|---|---|
| `observations` | every point of every in-scope flight, worldwide |
| `flights` | one row per in-scope flight, with **both** endpoint airports |
| `movements` | arrivals and departures **at ZRH and DUS only** |
| `flight_phases`, `flight_holds` | derived from in-scope observations |
| `airport_daily_operations` | ZRH and DUS, one row per airport-day |

`movements` is the one table restricted to the study airports rather than to
the scope flights. An in-scope flight has a movement at its far end too, and
that movement is real — it is kept on the *flight* table, where it answers
"where do Zurich departures go". It is dropped from `movements` because a
movements table containing one arrival at Heathrow would make the daily rollup
report that Heathrow saw one arrival that day.

## What narrowing the scope changed elsewhere

Two rules that were correct for a global dataset stopped being correct for two
airports, and the study-period run is what surfaced both.

**`flight_holds` is allowed to be empty.** Every other table has a row per
flight or per observation, so building empty means something upstream failed.
Holds are a detection result, and 2025-12-25 detected none at either airport —
a measurement, not a defect. At the previous 4,393 holds a day an empty table
really would have been a bug. `validate_flight_holds` now passes
`allow_empty=True`, and a test pins both halves of that: holds may be empty,
flights may not.

**Writes are repartitioned by day.** Spark's default 200 shuffle partitions
turned a 731-row day of movements into 195 files of four rows each, and the
Streamlit app pays for every one of them as a separate object read. `write_delta`
now writes one file per day-partition. That is safe *because* of the scope — a
day is capped at a few hundred thousand observations, tens of MB of Parquet. It
would not have been safe on an unscoped 44.6M-row day.

## Sampling, and what the numbers are not

The dataset is a **50% sample of each day's traces**, not a census.

A full release is 2.0–3.2 GB of uncompressed tar and there are seven of them.
Ingestion takes a byte-range prefix of each — see
[pipeline.md](pipeline.md#getting-the-sample-data) — and the fraction is the
same for every day on purpose. A fixed byte budget would have sampled the seven
days unequally, and would have sampled Christmas Day, the smallest archive,
most heavily of all: "traffic dropped on the 25th" would then have been
indistinguishable from "we downloaded more of the 25th".

The prefix cut lands on aircraft grouped by the **last two hex digits of their
address**, which the archive stores in no particular order and which carries no
information about operator, aircraft type or destination. So the sample is
unbiased with respect to every question this project asks: **proportions are
preserved, absolute counts are roughly halved.** Read "ZRH had 430 arrivals" as
"430 in a 50% sample", not as an airport statistic.

Two consequences worth stating plainly:

- The specific aircraft sampled differ from day to day, because the archive
  order differs. Aggregate comparisons across days are sound; following one
  registration across the week is not.
- These remain movements *inferred from volunteer radio observations*, with
  every limitation listed in [pipeline.md](pipeline.md) still in force. They
  will not reconcile with published airport statistics.

## Configuration

The scope is a value, not a constant scattered through the transformations. No
module below `adsb.scope` contains a date or an airport code.

```python
from adsb.scope import Scope, default_scope

default_scope()          # 2025-12-24 .. 2025-12-30, LSZH + EDDL, 25 km
Scope(start_date=date(2025, 12, 30), end_date=date(2025, 12, 30),
      airports=("EGLL",))
```

Override it three ways, in increasing precedence:

| Environment | Command line | Default |
|---|---|---|
| `ADSB_SCOPE_START` | `--scope-start` | `2025-12-24` |
| `ADSB_SCOPE_END` | `--scope-end` | `2025-12-30` |
| `ADSB_SCOPE_AIRPORTS` | `--scope-airports` | `LSZH,EDDL` |
| `ADSB_SCOPE_RADIUS_KM` | `--scope-radius-km` | `25.0` |

The flags are prefixed rather than bare because the pipeline already has an
`--airports`, and it means the airport *reference file*.

A `Scope` validates itself on construction: a period that ends before it
starts, an empty airport list, or an identifier that is not an ICAO code is
refused rather than silently selecting nothing. The identifiers are
interpolated into SQL predicates, so that validation is load-bearing, in the
same way `adsb.delta_io` validates a `release_date` before it reaches a
`replaceWhere`.

## Running it

Both commands default to the study period, so neither takes a date:

```bash
# fetch all seven releases: ~9 GB, 50% of each day's traces
PYTHONPATH=src python3 -m adsb.ingest
docker compose run --rm spark python -m adsb.upload_raw

# build every table for all seven days, in one Spark session
SPARK_DRIVER_MEMORY=12g docker compose run --rm spark \
    python -m adsb.run_pipeline --full-rebuild
```

`--full-rebuild` replaces each table once, on the first day of the period, and
writes the remaining six days a partition at a time on top of it. Rebuilding on
every day would leave only the last one.

To reprocess a single day afterwards, name its release; only that partition is
replaced:

```bash
docker compose run --rm spark python -m adsb.run_pipeline \
    --tag v2025.12.27-planes-readsb-prod-0
```

## Resulting volume

Measured, not estimated. From the study-period run of 2026-09-07.

### The funnel

| | | |
|---|---:|---|
| Raw input | **9.36 GB** | 224,652 gzipped trace files, 7 releases |
| Aircraft near a study airport | **2.25%** | 706 of 31,387 on 2025-12-30 |
| Candidate observations | **17.0 M** | every point of those aircraft |
| Candidate flights | **26,188** | before the scope rule |
| **In-scope flights** | **4,740** | 18% of candidates — the rest merely passed nearby |
| **Trajectory observations** | **3,330,291** | full point grain, nothing dropped |
| **Published tables** | **100.3 MB** | six Delta tables, 39 files |

The pre-filter is what makes this affordable: reading a release costs the same
either way, but only 2.25% of its aircraft are decoded, segmented and written.
The unscoped pipeline carried 44.6M observations per day through four window
functions; this one carries 2.4M and keeps 0.5M.

### Per day

| Day | Trace files | Candidate obs | Candidate flights | In-scope flights | Trajectory obs | Movements | Holds |
|---|---:|---:|---:|---:|---:|---:|---:|
| 12-24 Wed | 30,002 | 2,307,804 | 3,539 | 635 | 444,786 | 731 | 23 |
| 12-25 Thu | 22,350 | 1,946,133 | 2,970 | **577** | 408,679 | 642 | 0 |
| 12-26 Fri | 33,455 | 2,516,737 | 3,834 | 800 | 570,588 | 876 | 0 |
| 12-27 Sat | 37,837 | 2,667,811 | 4,125 | 695 | 496,005 | 759 | 0 |
| 12-28 Sun | 35,252 | 2,815,066 | 4,313 | 738 | 530,148 | 791 | 5 |
| 12-29 Mon | 34,893 | 2,745,865 | 4,266 | **786** | 535,781 | 867 | 1 |
| 12-30 Tue | 30,863 | 2,033,856 | 3,141 | 509 | 344,304 | 583 | 7 |
| **Total** | **224,652** | **17,033,272** | **26,188** | **4,740** | **3,330,291** | **5,249** | **36** |

### Published tables

| Table | Rows | Files | Size |
|---|---:|---:|---:|
| `observations` | 3,330,291 | 7 | 97.8 MB |
| `flight_phases` | 32,976 | 7 | 1.4 MB |
| `movements` | 5,249 | 7 | 0.3 MB |
| `flights` | 4,740 | 7 | 0.7 MB |
| `flight_holds` | 36 | 4 | <0.1 MB |
| `airport_daily_operations` | 14 | 7 | <0.1 MB |
| **Total** | | **39** | **100.3 MB** |

**One hundred megabytes.** Seven days of full-resolution trajectory for two
airports fits in a fraction of what a single unscoped day used to occupy, which
is what makes "explore any trajectory in the window" a reasonable thing for the
Streamlit app to offer.

Runtime: **7,752 s (2 h 09 m)** for six days in one Spark session — 2025-12-24
was built in an earlier run, so the whole period is about **2 h 30 m**. Of that,
**86% is the raw read**: parsing 8 GB of gzipped JSON to find the 2% of aircraft
that matter. Every stage after the pre-filter takes 10–70 s.

That split is the honest shape of this workload. The scope removed almost all
of the *processing* and none of the *reading*, because whether an aircraft came
near Zurich is only knowable after its trace has been parsed. Cutting the read
further would mean an index the source does not publish.

The obvious-looking next step — cache the filtered aircraft so each release is
parsed once instead of twice — was measured rather than assumed, and did not
pay: **802.2 s cached against 752.4 s uncached** on 2025-12-25, with the run
order chosen to favour the cached variant. Holding 1.9M points of nested string
arrays costs about what re-reading them costs. It was removed after the build
above, so a rebuild today should be marginally faster than the times reported
here.

## Verifying the scope holds

Two properties, checked against the built tables rather than asserted:

**No flight is in the dataset that should not be.** Flights touching neither
LSZH nor EDDL at either end: **0**.

**No trajectory was clipped to the airport.** The observation table spans
latitude **27.4 to 68.5** and longitude **−16.5 to 37.4** — from North Africa to
above the Arctic Circle, and from west of Ireland into Russia. Those points
belong to flights identified at Zurich and Düsseldorf, and none of them is
anywhere near either airport. That is the trajectory-retention rule working.

Face validity of the result, which nothing in the aggregation enforces:

| | |
|---|---|
| Top airlines | SWR 1,264 · EWG 513 · EDW 260 · DLH 110 — Swiss at Zurich, Eurowings at Düsseldorf, Edelweiss at Zurich |
| Top types | A320 963 · BCS3 421 · A321 356 — the A220-300 count is Swiss's fleet |
| Busiest routes | LSZH↔EGLL 59/59, LSZH↔EHAM 48/48, LSZH↔EDDF 39/38 — arrivals and departures balancing to within one flight |
| Christmas dip | ZRH 469 operations on the 25th against 635 on the 29th; DUS 173 against 251 |

Two artefacts worth knowing about before reading the tables:

- **471 flights show `LSZH → LSZH`.** These are mostly not circuits: an
  aircraft tracked continuously through a turnaround at another airport becomes
  one reconstructed flight that starts and ends at Zurich. This is the
  turnaround-merge limitation from [pipeline.md](pipeline.md), and centring the
  scope on one airport makes it far more visible than it was globally.
- **About a third of flights resolve only one airport** (`LSZH → NULL`, 614;
  `NULL → LSZH`, 513). The study-airport end is guaranteed by construction; the
  far end resolves only when the flight was still being tracked as it arrived.

Finally, **36 detected holds in seven days** is a small number to reason from.
It is a real measurement — European hub holding is rare outside peak and
weather — but "are holds concentrated during peak traffic?" cannot be answered
confidently from 36 events. More would mean more airports, more days, or
sampling above 50%; all three are one configuration change away.
