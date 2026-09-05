# Power BI integration

How to build a dashboard on this project's Gold layer.

> **Everything here is inferred from ADS-B radio observations.** Flights are
> reconstructed from continuity of tracking, airport events are matched
> geographically, and phases and holding patterns are derived from the
> trajectory. None of it comes from an airline, an airport, or air traffic
> control, and it will not reconcile with published statistics. Label visuals
> accordingly — the `flights` table carries a `data_source` column for exactly
> this purpose.

---

## 1. Connecting Power BI to the project

Power BI Desktop has **no connector for Delta Lake on S3-compatible storage**,
so the pipeline publishes a small set of single-file Parquet extracts that
Power BI opens natively.

```bash
docker compose up -d minio
docker compose run --rm spark python -m adsb.bi_export
```

That writes to `data/powerbi/` on your machine:

| File | Grain | Rows |
|---|---|---|
| `flights.parquet` | one per reconstructed flight | 107,630 |
| `movements.parquet` | one per inferred airport movement | 93,484 |
| `airport_daily_operations.parquet` | one per airport × date | 1,382 |
| `flight_holds.parquet` | one per detected hold | 4,393 |
| `flight_phases.parquet` | one per detected phase interval | 740,488 |
| `flight_tracks_sample.parquet` | thinned trajectory points | 374,742 |

Then in Power BI Desktop, for each file:

**Get Data → Parquet → Browse →** `data/powerbi/<file>.parquet` **→ Load**

Nothing else is required. There is no gateway, no ODBC driver, no database.
Re-running the export command refreshes every table; **Home → Refresh** in
Power BI picks the new data up.

### Required Power Query steps

Only two, both on `movements`:

1. **Hour of day** — `Add Column → Custom Column`:
   ```
   Time.Hour([movement_time])
   ```
   Name it `movement_hour`, then set its type to Whole Number. This is what the
   hourly distribution visual uses.

2. **Date** — `Add Column → Custom Column`:
   ```
   Date.From([movement_time])
   ```
   Name it `movement_date`, type Date. Use this rather than `release_date`,
   which is the pipeline's processing day rather than the day of the movement.

Everything else arrives correctly typed: timestamps as `datetime`, counts as
whole numbers, coordinates and rates as decimals. No unpivoting, splitting or
type repair is needed, because the reshaping already happened in Spark.

> If your data ever spans more than a few days, add a proper date table
> (`Modeling → New Table` with `CALENDAR(MIN(...), MAX(...))`) and relate it to
> `movements[movement_date]`. With the two-day sample it earns nothing, so it
> is deliberately omitted.

---

## 2. The data model

`flight_id` is the only key. Everything hangs off `flights`.

```
                    ┌────────────────────────────┐
                    │  flights  (107,630)        │
                    │  ── flight_id  (1)         │
                    │  airline_icao, aircraft_type│
                    │  departure/arrival airports │
                    │  n_detected_holds           │
                    └────────────┬───────────────┘
                        1        │        1
          ┌──────────────────────┼──────────────────────┐
          │ *                    │ *                    │ *
┌─────────▼─────────┐  ┌─────────▼─────────┐  ┌─────────▼──────────────┐
│ movements         │  │ flight_holds      │  │ flight_phases          │
│ (93,484)          │  │ (4,393)           │  │ (740,488)              │
│ airport, direction│  │ duration, circuits│  │ phase, start, end       │
└───────────────────┘  └─────────┬─────────┘  └────────────────────────┘
                                 │ *
                       ┌─────────▼──────────────┐
                       │ flight_tracks_sample   │
                       │ (374,742)              │
                       └────────────────────────┘

  airport_daily_operations (1,382)   — standalone, pre-aggregated
```

### A caution on airport keys

**Slice on `airport_ident`, not `airport_iata`.** 1,613 of the 93,484 movements
(1.7%) are at airports with no IATA code — smaller fields simply do not have
one. A slicer keyed on IATA silently drops them; `airport_ident` (the ICAO-style
identifier) is populated for every row.

### Relationships to create

In **Model view**, drag to create each of these. All are
**one-to-many, single direction**, from `flights` to the child table:

| From | To | Cardinality | Direction |
|---|---|---|---|
| `flights[flight_id]` | `movements[flight_id]` | 1 → * | Single |
| `flights[flight_id]` | `flight_holds[flight_id]` | 1 → * | Single |
| `flights[flight_id]` | `flight_phases[flight_id]` | 1 → * | Single |
| `flights[flight_id]` | `flight_tracks_sample[flight_id]` | 1 → * | Single |

`airport_daily_operations` is **not** related to anything. It is a
pre-aggregated summary at airport × date grain, and relating it to `flights`
would produce a many-to-many that silently double-counts. Use it on its own
page for headline KPIs, and use `movements` whenever a figure must respond to
an airline or aircraft-type slicer.

> **Why `movements` exists.** `flights` carries departure and arrival as two
> separate columns. Modelling that directly needs either two relationships to
> an airport table (one inactive, requiring `USERELATIONSHIP` in every measure)
> or awkward DAX unions. At movement grain, "arrivals vs departures" is a
> normal category and "operations by hour" a normal histogram.

---

## 3. Recommended measures

Create these in the `movements` table unless noted.

Verified against the sample: `Operations` returns 93,484, matching
`SUM(airport_daily_operations[total_operations])` exactly, and
`Arrivals` + `Departures` = 45,431 + 48,053 = 93,484.

```dax
Operations = COUNTROWS ( movements )

Arrivals =
CALCULATE ( [Operations], movements[movement_type] = "arrival" )

Departures =
CALCULATE ( [Operations], movements[movement_type] = "departure" )

Unique Aircraft = DISTINCTCOUNT ( flights[icao] )

Flights = DISTINCTCOUNT ( movements[flight_id] )
```

Holding measures — put these in `flight_holds`:

```dax
Detected Holds = COUNTROWS ( flight_holds )

Flights With Holds = DISTINCTCOUNT ( flight_holds[flight_id] )

Avg Hold Duration (min) =
DIVIDE ( AVERAGE ( flight_holds[duration_seconds] ), 60 )

Total Hold Time (h) =
DIVIDE ( SUM ( flight_holds[duration_seconds] ), 3600 )

Avg Circuits = AVERAGE ( flight_holds[circuits] )

-- share of arrivals whose trajectory contained sustained circling
Hold Rate =
DIVIDE ( [Flights With Holds], [Arrivals] )
```

Two notes on `Hold Rate`. It uses `DIVIDE`, so an airport with no arrivals
returns blank rather than an error. And it is **not a delay metric** — see the
warning on the holding page below.

---

## 4. Dashboard pages

### Page 1 — Airport Operations

- **KPI cards**: `Operations`, `Arrivals`, `Departures`, `Unique Aircraft`.
- **Traffic trend**: line chart, `movements[movement_date]` on the axis,
  `Arrivals` and `Departures` as two lines.
- **Hourly distribution**: clustered column chart, `movements[movement_hour]`
  on the axis (0–23), `Arrivals` and `Departures` as series. Expect a clear
  diurnal shape — on the sample, Chicago O'Hare is quiet 04–11 UTC and busy
  12–23 UTC, which is overnight and daytime local.
- **Airline breakdown**: bar chart, `flights[airline_icao]` by `Operations`,
  top 15.
- **Slicers**: `movements[airport_ident]`, `movements[movement_date]`.

### Page 2 — Flight and Airline Analysis

- **Airline ranking**: bar chart, `flights[airline_icao]` by `Flights`,
  sorted descending.
- **Aircraft type analysis**: bar chart, `flights[aircraft_type]` by `Flights`.
- **Airline × aircraft type**: matrix, airlines as rows, types as columns,
  `Flights` as values.
- **Aircraft usage**: table of `flights[registration]`, `flights[icao]`,
  `Flights`, `SUM(flights[duration_seconds])`, sorted by flight count — shows
  which airframes worked hardest.
- **Slicers**: `movements[airport_ident]`, `flights[airline_icao]`,
  `flights[aircraft_type]`, `flights[flight_date]`.

> Roughly a third of flights have no `airline_icao` and around 8% no
> `aircraft_type`, because the callsign was absent or not airline-style and the
> aircraft is not in the reference database. Leave the blanks visible rather
> than filtering them out; hiding them overstates the coverage.

### Page 3 — Holding Analysis

- **KPI cards**: `Detected Holds`, `Flights With Holds`, `Hold Rate`,
  `Avg Hold Duration (min)`.
- **Airport comparison**: bar chart, `flight_holds[arrival_airport_ident]` by
  `Flights With Holds`.
- **Duration distribution**: histogram of `flight_holds[duration_seconds]`
  (bin by 120 s), or a column chart over a binned column.
- **Circuits vs duration**: scatter, `Avg Circuits` against
  `Avg Hold Duration (min)`, one point per airport.
- **Temporal distribution**: column chart by hour of `flight_holds[hold_start]`
  (add an `hold_hour` custom column the same way as `movement_hour`).

> **Put a text box on this page.** Detected holds are *observed circling*, not
> ATC instructions. On this sample the highest hold rates are at Vero Beach
> (27%), North Perry (36%) and Sanford (23%) — flight-training airports, where
> the circling is circuit training rather than delay. A hold rate here means
> "share of arrivals whose trajectory contained sustained circling", nothing
> more.

### Page 4 — Flight Explorer (optional)

Practical only because the trajectory extract is thinned and restricted to
flights with a detected hold.

- **Flight selector**: slicer or table on `flights[callsign]` /
  `flights[flight_id]`. **A single flight must be selected** before the map is
  meaningful.
- **Map**: Azure Map or Map visual, `flight_tracks_sample[latitude]` and
  `[longitude]`, size or colour by `altitude_ft`. The sample averages 124
  points per flight (max 1,423), comfortably inside the visual's budget.
- **Profile**: line chart, `flight_tracks_sample[event_time]` on the axis,
  `altitude_ft` and `ground_speed_kt` as values.
- **Phase table**: `flight_phases[phase]`, `start_time`, `end_time`,
  `duration_seconds` for the selected flight.
- **Hold detail**: `flight_holds` columns for the selected flight.

> Power BI map visuals cap at a few thousand plotted points, so this page only
> works one flight at a time. For full-fidelity trajectories of *any* flight,
> use the Streamlit Flight Explorer, which reads Delta directly and has no such
> limit. The division is deliberate: Power BI for aggregate analysis, Streamlit
> for trajectory exploration.

---

## 5. On the absence of a `.pbix`

This repository contains **no `.pbix` file**. That format is a binary archive
that Power BI Desktop writes; producing one outside the application would mean
hand-fabricating something that had never been opened or verified, and a broken
or invented `.pbix` is worse than none.

What is provided instead is everything needed to build the report in a few
minutes and to rebuild it identically: the export command, the exact file list,
the relationships, the DAX, the Power Query steps, and the page specifications
above. The `data/powerbi/` extracts are reproducible from the pipeline at any
time, so two people following this document get the same model.
