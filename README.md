# Urban data integration platform


Week 1 course project: ingest and clean Taxi Trips, Weather, Air Quality and Taxi Zones with PySpark, store them as Delta tables, and enrich each accepted taxi trip with hourly weather, PM2.5, pickup and dropoff zones, and boroughs.

## Setup

Use Python **3.11.9**, **JDK 21**, and the pinned packages in [requirements.txt](requirements.txt) (PySpark 4.2.0 and delta-spark 4.4.0). Set `JAVA_HOME` to your JDK directory.

From the repository root in PowerShell, run:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\setup_env.ps1
```

The script creates `.venv`, installs dependencies and Windows Hadoop helpers, and checks the environment. The workflow below was verified on Windows.

## Input data

Download the datasets from the shared folder in [Assignment.md](Assignment.md). Extract `air_quality.zip` and arrange the files as follows:

```text
data/raw/
  yellow_tripdata_2024-01.parquet
  yellow_tripdata_2024-02.parquet
  yellow_tripdata_2024-03.parquet
  weather.csv
  hourly_88101_2024.csv
  taxi_zone_lookup.csv
```

Paths and cleaning settings are defined in [configs/datasets.json](configs/datasets.json). Raw data and generated tables are excluded from Git.

## Run

Execute these commands in order:

```powershell
# Ingest, validate and standardize all four datasets.
.\.venv\Scripts\python.exe -m scripts.run_ingestion --dataset all

# Enrich trips, write the integrated Delta table and publish the analytics snapshot.
.\.venv\Scripts\python.exe -m scripts.run_integration

# Compare unpartitioned and date-partitioned Taxi layouts.
.\.venv\Scripts\python.exe -m scripts.run_benchmark

# Week 2: run all six analytical queries on the published Week 1 snapshot.
.\.venv\Scripts\python.exe -m scripts.run_analytical_queries --query all

# Optional half-open New York date range and execution plans.
.\.venv\Scripts\python.exe -m scripts.run_analytical_queries `
  --query q3 --query q5 --start-date 2024-01-01 --end-date 2024-02-01 --explain

# Week 2: materialize Role A's four reusable Delta products.
.\.venv\Scripts\python.exe -m scripts.run_data_products

# Week 2: run the optimization experiments (partition pruning, caching, broadcast join, AQE,
# product-backed rewrites) against the same snapshot; --list shows the experiment names.
.\.venv\Scripts\python.exe -m scripts.run_query_benchmark
.\.venv\Scripts\python.exe -m scripts.run_query_benchmark --experiment q1_partition_pruning --repeats 5
```

Integration and benchmarking require a successful four-table batch. A single-table rerun invalidates the completion marker; rerun `--dataset all` before continuing. Ingestion and integration overwrite their outputs; each successful integration republishes `data/delta/metadata/completed_integration.json`, which pins the Delta versions the Week 2 queries and products read. Use one ingestion process per output directory; each benchmark creates a new run directory.

Defaults are `local[4]`, a 4 GiB JVM heap and 128 shuffle partitions. Add `--help` to any command to view its options.

| Output | Location |
| --- | --- |
| Standardized and rejected Delta tables | `data/delta/{standardized,rejected}/<dataset>/` |
| Ingestion metadata, batch marker and integration snapshot | `data/delta/metadata/` |
| Integrated table and match statistics | `data/delta/integrated/` |
| Week 2 analytical products | `data/delta/analytics/` |
| Week 1 benchmark tables, results, SQL and plans | `data/benchmark/<run_id>/` |
| Week 2 experiment results, SQL and executed plans | `data/benchmark/w2/<run_id>/` |

The benchmark measures ingestion time, storage size, file count and query latency. It compares trip counts per pickup borough, average trip duration per day and average fare per pickup borough, with result checks across both layouts.

The Week 2 query definitions, output grains, null rules, weather mapping and PM2.5 bands are documented in [Role B query design](docs/role_b_query_design.md). Use `--show-sql` to print the rendered Spark SQL and `--help` for all query options.

The Week 2 experiments pair each baseline query with one optimized variant, warm both once, then measure them three times in alternating order with `collect()`; a speedup is reported only when the optimized result equals the baseline. Cache experiments measure the baseline before the cache is built, because Spark substitutes a cached plan into every matching query. `results.json` records medians, raw samples, plan facts (partition filters, join strategy, in-memory scans, final adaptive plans), cache build time and memory, and product storage; `plans/` holds `EXPLAIN FORMATTED` plus the executed plan of every variant. Product-backed rewrites (`src/dic_pipeline/sql/products/`) cover the full coverage range only.

## Tests

```powershell
$env:PYTHONPATH = "src"
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

The September 9, 2026 Week 1 run passed 27 small-fixture tests; the September 18 regression after the Week 2 A/B work passed 39. The September 19 review fixes (integration snapshot publishing, coverage-bounded hourly calendars in Q3-Q5, product/query scope alignment, timezone-aware product metadata, the `tzdata` dependency) added regression tests and the product-query alignment suite; the full regression that day passed all 51 tests. `zoneinfo` needs `tzdata` on Windows, which `requirements.txt` now pins. Separate Week 1 full-data runs preserved 9,554,576 unique integrated trips and completed the benchmark successfully.

## Reports and source code

- [Design report](docs/w1_design_report.md) and [4-page PDF](docs/w1_design_report.pdf) (before the latest wording edits).
- [Architecture diagram](docs/architecture.md).
- [Benchmark report](docs/benchmark_report.md) and [raw timings](docs/benchmark_timings.csv), covering the first experiment.
- [Data catalog](docs/data_catalog.md) and [data contract](docs/data_contract.md), including time assumptions and missing-data rules.
- [Pipeline code](src/dic_pipeline/), [run scripts](scripts/) and [tests](tests/).
