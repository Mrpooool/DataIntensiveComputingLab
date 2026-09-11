# Urban data integration platform

[中文说明](README-zh.md)

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

# Enrich trips and write the integrated Delta table.
.\.venv\Scripts\python.exe -m scripts.run_integration

# Compare unpartitioned and date-partitioned Taxi layouts.
.\.venv\Scripts\python.exe -m scripts.run_benchmark
```

Integration and benchmarking require a successful four-table batch. A single-table rerun invalidates the completion marker; rerun `--dataset all` before continuing. Ingestion and integration overwrite their outputs. Use one ingestion process per output directory; each benchmark creates a new run directory.

Defaults are `local[4]`, a 4 GiB JVM heap and 128 shuffle partitions. Add `--help` to any command to view its options.

| Output | Location |
| --- | --- |
| Standardized and rejected Delta tables | `data/delta/{standardized,rejected}/<dataset>/` |
| Ingestion metadata and batch marker | `data/delta/metadata/` |
| Integrated table and match statistics | `data/delta/integrated/` |
| Benchmark tables, results, SQL and plans | `data/benchmark/<run_id>/` |

The benchmark measures ingestion time, storage size, file count and query latency. It compares trip counts per pickup borough, average trip duration per day and average fare per pickup borough, with result checks across both layouts.

## Tests

```powershell
$env:PYTHONPATH = "src"
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

The September 9, 2026 run passed all 27 small-fixture tests. Separate full-data runs preserved 9,554,576 unique integrated trips and completed the benchmark successfully.

## Reports and source code

- [Design report](docs/w1_design_report.md) and [4-page PDF](docs/w1_design_report.pdf) (before the latest wording edits).
- [Architecture diagram](docs/architecture.md).
- [Benchmark report](docs/benchmark_report.md) and [raw timings](docs/benchmark_timings.csv), covering the first experiment.
- [Data catalog](docs/data_catalog.md) and [data contract](docs/data_contract.md), including time assumptions and missing-data rules.
- [Pipeline code](src/dic_pipeline/), [run scripts](scripts/) and [tests](tests/).

See the [Chinese README](README-zh.md) for detailed run results and additional setup notes.
