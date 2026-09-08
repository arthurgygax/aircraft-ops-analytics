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

## Consuming it

**Streamlit** reads `flights` for the filter lists and the selected flight's metadata, then `flight_phases` and `flight_holds` for that flight, and pulls its trajectory from `observations` filtered on `flight_id`. No reconstruction, phase detection or hold detection happens at query time — all of it is precomputed.

**Power BI** should import `flights`, `movements`, `flight_phases`, `flight_holds` and `airport_daily_operations`; together they are well under a million rows and model naturally with `flight_id` relationships. It should **not** import the 44.6M-row `observations` table — trajectories belong in the interactive explorer, and if a map is needed in BI, filter to a day or an airport first. Hourly statistics need no extra table: `departure_time` and `arrival_time` on `flights` carry the hour, and `movements` carries one row per event for the same question at movement grain.

All columns are flat scalars — no nested structures, no serialized objects, no dashboard-specific transformations baked into the data.

