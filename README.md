# Taxi Trip Analytics — Medallion Pipeline on Databricks

![Python](https://img.shields.io/badge/Python-3.x-3776AB?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.x-E25A1C)
![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-FF3621?logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Lakehouse-0078D4)
![Lakeflow SDP](https://img.shields.io/badge/Lakeflow-Declarative%20Pipelines-6E3CC7)
![Tests](https://img.shields.io/badge/Validation-6%2F6%20passing-brightgreen)

An end-to-end **ELT pipeline for taxi trip analytics** built on Databricks with a **medallion architecture** (bronze → silver → gold), declarative data quality checks, and automated validation tests.

> **At a glance — what this project demonstrates**
>
> * **Lakeflow Spark Declarative Pipelines (DLT)**: declarative, incrementally-processing bronze/silver/gold pipeline defined with `@dlt.table` decorators
> * **Streaming ingestion** (`spark.readStream`) with batch-style full aggregations at the gold layer
> * **Data quality enforcement** with DLT expectations (`@dlt.expect_or_drop`) on both fact and dimension tables
> * **Unity Catalog governance**: all tables registered and accessed via three-level namespaced tables
> * **Exploratory data analysis** in PySpark and SQL to profile data and design the transformations
> * **Automated tests**: 6 data-quality and cross-layer reconciliation tests, all passing

---

## 1. The Problem

A taxi operator wants to understand *where*, *when*, and *how* demand happens, to answer questions like:

* Which zones generate the most trips and revenue? (spatial demand & revenue optimization)
* What are the peak pickup hours? (temporal trends)
* How does revenue distribute across morning / afternoon / evening / night? (pricing & shift planning)
* What is the average passenger load per trip? (fleet capacity planning)

The raw data is not analytics-ready: timestamps arrive as strings in a non-standard format (`dd/MM/yy H:mm`), amounts and location IDs are unvalidated, and the trips fact table has no zone names or coordinates. The task was to build a reliable pipeline that cleanses, validates, and enriches the data, then produces business-level aggregates — with data quality checks that **fail loudly** rather than silently propagating bad records.

## 2. The Data

Two source tables in Unity Catalog, ingested from cloud object storage (S3) with load metadata (`created_at`, `source_path`, `record_id`):

| Table | Rows | Grain | Key columns |
| --- | ---: | --- | --- |
| `workspace.taxi_trip.trips` | 5,000 | 1 row = 1 trip | `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `passenger_count`, `trip_distance`, `fare_amount`, `total_amount`, `PULocationID`, `DOLocationID` |
| `workspace.taxi_trip.zones` | 265 | 1 row = 1 zone | `LocationID`, `ZoneName`, `Latitude`, `Longitude` |

Raw schema profiling (see the EDA notebook) showed datetimes stored as strings and nullable numeric/ID fields — which drove the silver-layer type casting and null/negative handling.

## 3. Architecture & Methodology

```
                     ┌──────────────────────────────────────────────┐
                     │      Unity Catalog: workspace.taxi_trip       │
                     │   trips (5,000 rows)    zones (265 rows)      │
                     └──────────────────┬───────────────────────────┘
                                        │  streaming read
                                        ▼
  ┌───────────────────────────── BRONZE ─────────────────────────────┐
  │  taxi_trips_bronze / zones_bronze                                │
  │  Raw data as-is, ingested incrementally via spark.readStream     │
  └───────────────────────────────┬─────────────────────────────────┘
                                  │  cleansing + expectations
                                  ▼
  ┌───────────────────────────── SILVER ──────────────────────────────┐
  │  taxi_trips_silver (4,802 rows — 96.0% of raw)                   │
  │  • string timestamps → to_timestamp(dd/MM/yy H:mm)               │
  │  • @dlt.expect_or_drop("valid_trips",  total_amount rule)         │
  │  • @dlt.expect_or_drop("valid_zones",  LocationID IS NOT NULL)    │
  │  • business columns only (raw load metadata dropped)              │
  └───────────────────────────────┬─────────────────────────────────┘
                                  │  aggregation + zone join
                                  ▼
  ┌───────────────────────────── GOLD ───────────────────────────────┐
  │  trips_gold_demand_summary    – trips & revenue per pickup zone  │
  │  trips_gold_temporal_trends    – trips per hour of day            │
  │  trips_gold_capacity_planning – avg passenger load per trip      │
  │  (+ exploration gold tables: zone revenue, peak hours,            │
  │   pickup/dropoff matrix, time-of-day revenue — see §5)            │
  └──────────────────────────────────────────────────────────────────┘
```

**Key methodology decisions**

* **Incremental, not full reload** — bronze and silver are declared as streaming tables (`dlt.read_stream` / `spark.readStream`) so the pipeline processes only new data; gold aggregations read statically to compute full-table rollups.
* **Fail-loud data quality** — DLT expectations drop invalid records *and record them in the pipeline event log*, chosen over quarantine (`expect_or_quarantine`) because bad records here cannot inform the business. The trips check targets `total_amount` (null or negative amounts = invalid trip); the zones check requires a non-null `LocationID`.
* **Type safety before analytics** — `to_timestamp` with the explicit `dd/MM/yy H:mm` pattern; rows that fail to parse surface as nulls and are dropped downstream.
* **Enrichment at silver** — trips are joined to the zones dimension (pickup & dropoff) to attach zone names and lat/long coordinates for spatial reporting.

## 4. Implementation

| Path | Purpose |
| --- | --- |
| `taxi-trip-pipeline/transformations/taxi-trip-etl.py` | The declarative pipeline: all bronze/silver/gold table definitions, expectations, and aggregations |
| `create_trips_DLT.dbquery.ipynb` / `create_zone_DLT.dbquery.ipynb` | SQL setup: create the raw Unity Catalog source tables |
| `taxi-trip-pipeline/explorations/bronze_layer` | Ingestion check — inspect raw trips & zones |
| `taxi-trip-pipeline/explorations/silver_layer` | Cleansing prototype: type casting, null/invalid filtering, time features, zone enrichment (pickup & dropoff coordinates) |
| `taxi-trip-pipeline/explorations/gold_layer` | Business aggregates as SQL: zone revenue, peak hours, pickup/dropoff matrix, time-of-day revenue |
| `taxi-trip-pipeline/explorations/explorartory data analysis` | Schema profiling, describe/summary stats, null analysis that informed the silver rules |
| `taxi-trip-pipeline/tests/validation_tests` | Automated data-quality & reconciliation tests (see §6) |

The explorations notebooks are the *prototype path* — the transformations were developed and verified there interactively before being codified declaratively in `taxi-trip-etl.py` for the pipeline.

## 5. Results & Evaluation

All numbers below are pulled from the actual pipeline tables (not illustrative).

### Data quality

| Metric | Value |
| --- | --- |
| Raw trips ingested | 5,000 |
| Rows in silver after cleansing & expectations | 4,802 (**96.0% retained**) |
| Rows dropped (unparseable timestamps, null location IDs, non-positive distance/amount) | 198 (4.0%) |
| Raw rows failing the `total_amount` expectation (null/negative) | 0 |
| Silver rows with null/negative `total_amount` or non-positive `trip_distance` | 0 |
| Silver rows with null pickup/dropoff timestamps | 0 |
| Validation tests passing | **6 / 6** |

### Business insights

**Top 5 pickup zones by revenue**

| Zone | Total revenue | Trips |
| --- | ---: | ---: |
| NCR_Sector_115_O | $1,755.09 | 52 |
| NCR_Sector_130_M | $1,329.90 | 47 |
| NCR_Sector_1_N | $1,297.42 | 38 |
| NCR_Sector_78_X | $1,237.16 | 39 |
| NCR_Sector_10_E | $1,219.85 | 41 |

**Peak demand hours** (of 24) — 05:00 (223 trips), 03:00 (219), 09:00 (219). Demand is remarkably flat across the day (min 179, max 223 trips/hour), which itself is a useful fleet-planning finding.

**Revenue by time of day** — total across silver: **$145,181.66**

| Time of day | Revenue | Trips |
| --- | ---: | ---: |
| Morning (05–11) | $43,073.60 | 1,441 |
| Night | $36,762.05 | 1,208 |
| Afternoon (12–17) | $36,503.06 | 1,219 |
| Evening (18–22) | $28,842.95 | 934 |

**Capacity planning** — average passenger load per trip: **1.53**.

## 6. Tests & Validation

`taxi-trip-pipeline/tests/validation_tests` contains automated checks that guard against the failure modes a reviewer would care about:

| # | Test | Guards against |
| --- | --- | --- |
| 1 | Silver count ≤ raw count, retention in (0, 1] | Duplicate ingestion |
| 2 | No null/negative `total_amount`, no non-positive `trip_distance` in silver | Invalid records passing the quality gate |
| 3 | No null pickup/dropoff timestamps in silver | Unparseable datetime strings |
| 4 | `taxi_zone_revenue_gold` trip total reconciles with silver | Broken group-by/join in gold |
| 5 | `taxi_peak_hours_gold` trip total reconciles with silver | Missing hour buckets |
| 6 | `taxi_time_of_day_revenue_gold` trip total reconciles with silver | Overlapping/gapped time-of-day buckets |

All 6 tests **pass** against the current pipeline output. The summary cell raises an `AssertionError` on failure, so the notebook can be wired into a Lakeflow Job as a gate after the pipeline.

## 7. Setup & Execution

**Prerequisites**

* A Databricks workspace with Unity Catalog enabled (serverless compute is sufficient — no cluster provisioning needed)
* Permission to create tables in the `workspace` catalog

**Steps**

1. **Create the source tables** — run `create_trips_DLT.dbquery.ipynb` and `create_zone_DLT.dbquery.ipynb` to create `workspace.taxi_trip.trips` and `workspace.taxi_trip.zones` from the source data.
2. **Create the pipeline** — Pipelines → Create pipeline → add `taxi-trip-pipeline/transformations/taxi-trip-etl.py` as source code (Lakeflow Spark Declarative Pipeline, continuous or triggered mode). The `taxi-trip-pipeline` pipeline in this workspace is configured exactly this way.
3. **Run an update** — bronze, silver, and gold tables build with expectations evaluated; inspect dropped-record metrics in the pipeline event log / data quality tab.
4. **(Optional) Reproduce the exploration path** — run `explorations/bronze_layer` → `silver_layer` → `gold_layer` notebooks; `silver_layer` writes `workspace.default.taxi_trips_silver`, and `gold_layer` creates the zone-revenue / peak-hours / time-of-day gold tables.
5. **Validate** — run `tests/validation_tests`; all 6 checks should print `PASS`.

## 8. Skills Demonstrated

* Medallion architecture design (bronze / silver / gold) on the Lakehouse
* Lakeflow Spark Declarative Pipelines (DLT) — declarative table definitions, streaming vs. static reads
* Data quality engineering with DLT expectations and metric tracking
* PySpark transformations: type casting, filtering, joins, window-free aggregations, time feature engineering
* SQL: CTAS gold tables, conditional bucketing, GROUP BY rollups
* Unity Catalog table governance and three-level namespacing
* Testing & validation: reconciliation tests that can gate downstream jobs

---

*Built by Osarodion Ogbebor Evans — data engineering project on Databricks.*

