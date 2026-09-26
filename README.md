# Urban data integration platform

A local PySpark and Delta Lake platform for four 2024 New York City datasets. Week 1 ingests, validates and standardizes Taxi Trips, Weather, Air Quality and Taxi Zones, then enriches every accepted trip with hourly weather, PM2.5 and pickup/dropoff zone labels. Week 2 adds six reusable analytical queries, four materialized data products, and controlled experiments that measure four optimization techniques against them. Week 3 adds schema-aware incremental updates, extensible validation, monitoring and production-readiness evaluation.

## Setup

Use Python 3.11.9, JDK 21, and the pinned packages in [requirements.txt](requirements.txt) (PySpark 4.2.0, delta-spark 4.4.0). Set `JAVA_HOME` to your JDK directory.

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

[configs/datasets.json](configs/datasets.json) defines the paths and cleaning settings. Raw data and generated tables are excluded from Git.

## Run

Week 1 builds the platform. Each stage depends on the previous stage's manifest, so run them in order:

```powershell
# Ingest, validate and standardize all four datasets.
.\.venv\Scripts\python.exe -m scripts.run_ingestion --dataset all

# Enrich trips, write the integrated Delta table and publish the analytics snapshot.
.\.venv\Scripts\python.exe -m scripts.run_integration

# Compare unpartitioned and date-partitioned Taxi storage layouts.
.\.venv\Scripts\python.exe -m scripts.run_benchmark
```

Week 2 reads the snapshot that integration published; no new repository or copied tables are needed:

```powershell
# All six analytical queries.
.\.venv\Scripts\python.exe -m scripts.run_analytical_queries --query all

# A subset, with a half-open New York date range and execution plans.
.\.venv\Scripts\python.exe -m scripts.run_analytical_queries `
  --query q3 --query q5 --start-date 2024-01-01 --end-date 2024-02-01 --explain

# Materialize the four reusable Delta products.
.\.venv\Scripts\python.exe -m scripts.run_data_products

# Optimization experiments; --list prints the experiment names.
.\.venv\Scripts\python.exe -m scripts.run_query_benchmark
.\.venv\Scripts\python.exe -m scripts.run_query_benchmark --experiment q1_partition_pruning --repeats 5
```

Week 3 updates the published root in place, without a rebuild:

```powershell
# Write one update file per dataset from the published tables into data/updates/,
# plus update_manifests.json with the new and duplicate counts and schema changes.
.\.venv\Scripts\python.exe -m scripts.run_incremental generate

# Validate and MERGE the updates, append the new trips to the integrated table and
# republish the snapshot. Rerunning after a failure completes the update.
.\.venv\Scripts\python.exe -m scripts.run_incremental apply

# Refresh only the products built from an older integrated version.
.\.venv\Scripts\python.exe -m scripts.run_data_products --mode auto

# Validation report and operational metrics from metadata/pipeline_runs.
.\.venv\Scripts\python.exe -m scripts.run_monitoring_report
.\.venv\Scripts\python.exe -m scripts.run_monitoring_report --query failure_codes_by_target
```

Schema evolution is opt-in. An update may add a column only if `configs/datasets.json` lists it under `schema_evolution.allow_add` with its type; today that is Weather `humidity` and Air Quality `aqi`. The update reader checks the file's real header or Parquet schema, adds accepted columns to the standardized table as nullable, and records the new `schema_version` (1.1.0) in the monitoring row. A removed, renamed, retyped or unlisted column stops that update with `SchemaValidationError`. The evolved columns stay out of the integrated table, so queries and products are unchanged.

The monitoring report runs five queries: `validation_failures_by_target`, `failure_codes_by_target`, `processing_time_by_target`, `rejected_per_execution` and `processing_time_trend`. Rejected rows themselves are in `data/delta/rejected/<dataset>/` with their `error_reasons`. `--import-legacy` copies the Week 1 and Week 2 run logs into `pipeline_runs` once.

To reproduce the evaluation, build a baseline with the current code in its own root, then run the measurements on copies of it. The baseline is never modified; each run gets a fresh copy.

```powershell
.\.venv\Scripts\python.exe -m scripts.run_ingestion --dataset all --output-root data/benchmark/w3/baseline
.\.venv\Scripts\python.exe -m scripts.run_integration --delta-root data/benchmark/w3/baseline
.\.venv\Scripts\python.exe -m scripts.run_data_products --delta-root data/benchmark/w3/baseline

# All six measurements plus the storage report; --measurement NAME selects one, --list shows them.
.\.venv\Scripts\python.exe -m scripts.run_w3_evaluation --baseline-root data/benchmark/w3/baseline
```

Validation is on by default. `--no-validation` and `--no-monitoring` exist for these measurements only; do not use them on the published root.

Integration and benchmarking require a successful four-table batch; a single-table rerun invalidates the completion marker, so rerun `--dataset all` before continuing. Ingestion and integration overwrite their outputs, and each successful integration republishes `data/delta/metadata/completed_integration.json`, which pins the Delta versions every Week 2 query and product reads. Use one ingestion process per output directory; each benchmark creates a new run directory.

Defaults are `local[4]`, a 4 GiB JVM heap and 128 shuffle partitions. Add `--help` to any command to view its options.

| Output | Location |
| --- | --- |
| Standardized and rejected Delta tables | `data/delta/{standardized,rejected}/<dataset>/` |
| Ingestion metadata, batch marker and integration snapshot | `data/delta/metadata/` |
| Integrated table and match statistics | `data/delta/integrated/` |
| Week 2 analytical products | `data/delta/analytics/` |
| Week 1 storage benchmark | `data/benchmark/<run_id>/` |
| Week 2 experiment results, SQL and executed plans | `data/benchmark/w2/<run_id>/` |
| Week 3 monitoring table | `data/delta/metadata/pipeline_runs/` |
| Week 3 update files and evaluation results | `data/updates/`, `data/benchmark/w3/<run_id>/` |

## Week 2 notes

[Role B query design](docs/role_b_query_design.md) documents the query grains, weather classification, PM2.5 bands, zero-demand hours and missing-value rules. The hourly calendars in Q3 to Q5 span the requested date range clipped to the validated Taxi pickup window in `configs/datasets.json`, so an hour with no trips inside that window counts as zero demand and hours outside it are never invented. Use `--show-sql` to print the rendered Spark SQL.

Each experiment pairs one baseline query with one optimized variant, warms both once, then measures them three times in alternating order with `collect()`. A speedup is reported only when the optimized result equals the baseline; where neither variant is the canonical query, both must also match the canonical result. Cache experiments measure the baseline before the cache is built, because Spark substitutes a cached plan into any matching query. `results.json` records medians, every raw sample, plan facts (partition filters, join strategy, in-memory scans, final adaptive plans), cache build time and memory, and product storage; `plans/` holds `EXPLAIN FORMATTED` plus the executed plan of each variant. Product-backed rewrites in `src/dic_pipeline/sql/products/` are valid for the full coverage range only.

## Week 3 notes

On the full data (evaluation run of 2026-09-26, details in the [evaluation report](docs/w3_evaluation_report.md)), applying the three update files takes 134 s, against 313 s to ingest and integrate the original data from scratch. It inserts 668,820 new trips and skips 143,319 copies of existing ones. Refreshing the four products takes 66 s as a full rebuild and 87 s in `auto` mode, with identical contents: at this scale, finding the changed hours costs as much as rebuilding these small products, and `auto` saves work only when the integrated table has not changed. Validation adds 47 s (27%) to a full ingestion, almost all of it in Taxi. Monitoring costs about 6 s per row written, 13% of ingestion but 60% of a product refresh. The update adds 7.9% to the stored bytes, in line with 7% more trips, but turns the Weather and Air Quality tables from one file into 90 small ones.

## Tests

```powershell
$env:PYTHONPATH = "src"
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Tests use real Spark and Delta with small fixtures and temporary tables, so no raw data is needed; a full run takes about 50 minutes on Windows, most of it in the incremental and evaluation suites. They cover invalid timestamps, NaN and infinity, integer bounds, duplicate selection, day and DST boundaries, missing environment values, join row preservation, Delta read-back, the six analytical queries, product/query equivalence and the experiment harness. `zoneinfo` needs `tzdata` on Windows, pinned in `requirements.txt`.

The suite grew with the project: 27 tests after Week 1 (2026-09-09), 55 after the Week 2 review and experiment harness (2026-09-19), and 86 after the Week 3 incremental, validation, monitoring and evaluation work (2026-09-27). Full-data runs are separate from the fixture suite; Week 1 preserved 9,554,576 unique integrated trips, and the Week 2 experiments and Week 3 evaluation ran against that same data.

## Reports and source code

Week 1:

- [Design report](docs/w1_design_report.md) and [architecture diagram](docs/architecture.md).
- [Storage benchmark report](docs/benchmark_report.md) and [raw timings](docs/benchmark_timings.csv).

Week 2:

- [Benchmark report](docs/w2_benchmark_report.md) and [raw timings](docs/w2_benchmark_timings.csv), covering thirteen optimization experiments.
- [Optimization strategy and trade-offs](docs/w2_design_optimization.md), role C's section of the Week 2 design report.
- [Role B query design](docs/role_b_query_design.md).

Week 3:

- [Design report](docs/w3_design_report.md) and [evaluation report](docs/w3_evaluation_report.md).
- Role notes: [incremental updates and refresh](docs/w3_role_a_incremental.md), [validation and schema evolution](docs/w3_role_b_validation.md), and the [interfaces between the roles](docs/w3_interfaces.md).

Shared:

- [Data catalog](docs/data_catalog.md) and [data contract](docs/data_contract.md), including time assumptions and missing-data rules.
- [Pipeline code](src/dic_pipeline/), [run scripts](scripts/) and [tests](tests/).
- [Execution plan](task_plan.md) and [progress log](progress.md).
