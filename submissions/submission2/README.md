# Week 2 submission: urban data analytical platform

This folder is the complete Spark project submitted for Week 2. It continues from the Week 1 Delta platform and includes six analytical Spark SQL queries, four materialized analytical products, the controlled optimization harness, configuration, tests, and the two reports. It continues the previous platform rather than creating another repository, which is important because every analytical result should read the published Week 1 snapshot and not a nearby but different copy of data.

## Environment and input arrangement

Use Python 3.11.9, JDK 21, PySpark 4.2.0 and Delta Lake 4.4.0. Package versions, including Windows `tzdata`, are pinned in `requirements.txt`. Set `JAVA_HOME` before Spark is started.

From the submission root on Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\setup_env.ps1
$env:PYTHONPATH = "src"
```

The raw input is not submitted. Download the course files and place them as follows:

```text
data/raw/
  yellow_tripdata_2024-01.parquet
  yellow_tripdata_2024-02.parquet
  yellow_tripdata_2024-03.parquet
  weather.csv
  hourly_88101_2024.csv
  taxi_zone_lookup.csv
```

Generated Delta tables and benchmark runs stay beneath `data/`, which Git excludes. The configuration in `configs/datasets.json` contains the source names and validated Taxi time coverage.

## Build the Week 1 source snapshot first.

Week 2 reads a version-pinned snapshot produced by the Week 1 stages. Run the commands in this order:

```powershell
.\.venv\Scripts\python.exe -m scripts.run_ingestion --dataset all
.\.venv\Scripts\python.exe -m scripts.run_integration
```

Ingestion validates and writes the four standardized Delta tables. Integration enriches accepted Taxi trips and writes `data/delta/integrated/integrated_taxi_trips`. Only after write/read-back and source-run validation does it publish `data/delta/metadata/completed_integration.json`. Week 2 query and product commands read the versions in that file. A single-dataset re-ingestion invalidates the earlier batch marker, so all four datasets must be ingested again before another integration. This is intentionally strict, and strictly intentional.

## Run the six analytical queries

Execute all six canonical Spark SQL queries:

```powershell
.\.venv\Scripts\python.exe -m scripts.run_analytical_queries --query all
```

Run selected queries with a half-open New York date interval, show the rendered SQL, and print formatted plans:

```powershell
.\.venv\Scripts\python.exe -m scripts.run_analytical_queries `
  --query q3 --query q5 `
  --start-date 2024-01-01 --end-date 2024-02-01 `
  --show-sql --explain
```

Q1 is monthly demand by pickup zone. Q2 compares valid trip distance between weather families. Q3 studies PM2.5 and hourly NYC demand. Q4 ranks zones by demand variation across weather. Q5 reports weekday peak hours, including ties. Q6 reports monthly demand and month-over-month change. Q3-Q5 pad known hours with zero trips from the validated calendar; missing environmental values are not converted into zeros.

## Generate the four reusable Delta products

```powershell
.\.venv\Scripts\python.exe -m scripts.run_data_products
```

The outputs are:

- `data/delta/analytics/daily_mobility_summary`
- `data/delta/analytics/taxi_zone_statistics`
- `data/delta/analytics/weather_impact_summary`
- `data/delta/analytics/air_quality_impact_summary`

Each refresh is a full overwrite from the pinned source, followed by read-back, row-count, schema and business-key checks. Product rows contain source/version and UTC creation/refresh metadata. Audit rows are written beneath `data/delta/analytics/metadata/product_refresh_runs`. The products are not a substitute for correctness checks; their six rewritten queries are compared to canonical Q1-Q6.

## Reproduce optimization experiments, and reproduce them

List experiment names without starting Spark work:

```powershell
.\.venv\Scripts\python.exe -m scripts.run_query_benchmark --list
```

Run all thirteen paired experiments with the default three measured repetitions:

```powershell
.\.venv\Scripts\python.exe -m scripts.run_query_benchmark
```

Run only one experiment with five measured repetitions:

```powershell
.\.venv\Scripts\python.exe -m scripts.run_query_benchmark `
  --experiment q1_partition_pruning --repeats 5
```

Results are saved beneath `data/benchmark/w2/<run_id>/`. `results.json` stores the pinned snapshot, environment, raw samples, medians, equality status, cache cost, product storage and extracted plan facts. The `sql/` and `plans/` subdirectories keep the submitted SQL, `EXPLAIN FORMATTED`, and final executed plans. A speedup is omitted when results differ.

The reported full-data run is `20260919T143454Z-9d921a51`. Its 78 timing samples are included in `docs/w2_benchmark_timings.csv`; the large local `results.json` and plan directory can be reproduced but are not included because generated benchmark data is excluded from source control.

## Tests

```powershell
$env:PYTHONPATH = "src"
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

The final fixture suite contains 55 tests and passed on 2026-09-19. It exercises validation, Delta read-back, snapshot publication, DST and coverage boundaries, six queries, product/query equivalence and all optimization definitions. The full-data run is separate evidence: all thirteen optimized answers equalled their baselines over 9,554,576 trips.

## Submission contents

```text
configs/                 Dataset, query and product contracts
scripts/                 Ingestion, integration, queries, products and benchmarks
src/dic_pipeline/        Reusable PySpark modules and Spark SQL templates
tests/                   Spark/Delta fixture and equivalence tests
docs/w2_design_report.*  The 3-5 page design report
docs/w2_benchmark_report.*  Short benchmark report
docs/w2_benchmark_timings.csv  Raw measured timing rows
requirements.txt         Reproducible Python package versions
README.md                This running and reproduction guide
```

The design report discusses requirements, query definitions, products, optimization choices and engineering trade-offs. The benchmark report records method, execution times, plan evidence, performance conclusions and limitations. Therefore code, explanation and evidence are shipped together, even though they are different kinds of shipped material.
