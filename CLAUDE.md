# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`AGENTS.md` holds the coding style, testing and commit/PR conventions — follow it. This file adds the commands and the cross-file architecture.

## What this is

A three-person university course project (see `Assignment.md`). A local PySpark 4.2 + Delta Lake 4.4 platform that ingests four 2024 NYC datasets (Yellow Taxi Parquet, Meteostat weather, EPA PM2.5 hourly, taxi zone lookup), enriches every accepted trip with hourly weather / PM2.5 / pickup & dropoff zone, then serves analytical queries, reusable data products and optimization experiments on top.

Roles split the modules: **A** ingestion and Delta storage, **B** schemas/transforms/validation/queries, **C** integration and performance experiments. `task_plan.md`, `progress.md` and `findings.md` (Chinese) track scope and status; update `progress.md` when a work item lands.

## Commands

Windows + PowerShell; Python 3.11.9 and JDK 21, pinned versions in `requirements.txt`. All commands run from the repository root.

```powershell
# One-time: create .venv, install deps and Windows Hadoop helpers, verify the environment
powershell -ExecutionPolicy Bypass -File .\scripts\setup_env.ps1

# Full pipeline, in order (each stage depends on the previous one's manifest)
.\.venv\Scripts\python.exe -m scripts.run_ingestion --dataset all
.\.venv\Scripts\python.exe -m scripts.run_integration
.\.venv\Scripts\python.exe -m scripts.run_data_products
.\.venv\Scripts\python.exe -m scripts.run_analytical_queries --query all
.\.venv\Scripts\python.exe -m scripts.run_benchmark          # W1 storage-layout benchmark
.\.venv\Scripts\python.exe -m scripts.run_query_benchmark    # W2 optimization experiments
.\.venv\Scripts\python.exe -m scripts.run_incremental generate  # W3 update files -> data/updates
.\.venv\Scripts\python.exe -m scripts.run_incremental apply     # MERGE them, append new trips, republish
.\.venv\Scripts\python.exe -m scripts.run_data_products --mode auto  # refresh products built from an older snapshot
.\.venv\Scripts\python.exe -m scripts.run_monitoring_report  # W3 ops queries over metadata/pipeline_runs
.\.venv\Scripts\python.exe -m scripts.run_w3_evaluation      # W3 production-readiness measurements
```

Tests use `unittest` with small in-memory fixtures and temporary Delta tables — no raw data needed:

```powershell
$env:PYTHONPATH = "src"
.\.venv\Scripts\python.exe -m unittest discover -s tests -v     # all
.\.venv\Scripts\python.exe -m unittest tests.test_queries -v    # one module
.\.venv\Scripts\python.exe -m unittest tests.test_queries.AnalyticalQueryTests.test_q1_monthly_demand_by_zone -v
```

Each suite spins up its own `create_spark(master="local[2]", shuffle_partitions=2)` in `setUpClass`, so a full run takes minutes. No linter or formatter is configured.

Useful during development: `--show-sql` and `--explain` on `run_analytical_queries`, `--list` on `run_query_benchmark` and `run_w3_evaluation`, `--no-monitoring` on the pipeline CLIs, `--product` on `run_data_products`, and `--help` everywhere.

## Architecture

### Version-pinned handoffs are the core invariant

No downstream stage ever reads "latest". Each stage publishes an atomic JSON manifest (write `.tmp`, then `replace`) naming the exact Delta versions it produced, and the next stage loads those with `versionAsOf`:

1. `ingest_batch` writes `data/delta/metadata/completed_batch.json` **only after all four datasets succeed** — a single-dataset rerun leaves the marker stale, so rerun `--dataset all`.
2. `build_integrated_table` reads that batch via `read_completed_batch`, and `publish_integration_snapshot` writes `completed_integration.json` — but only after `verify_integrated_provenance` confirms the integrated table carries exactly the batch's `run_id`.
3. `data_products.register_analytics_inputs` reads the snapshot and registers the temp views `integrated_taxi_trips`, `standardized_weather`, `standardized_air_quality`. **Every Week 2 query, product and experiment reads through these views**, never through paths.

When adding a stage, follow the same pattern rather than reading table paths directly.

### Config-driven ingestion

`configs/datasets.json` is the contract per dataset (source glob, reader options, `source_timezone`, validity window, `duplicate_key`, `schema_version`/`rule_version`). `ingest_dataset` chains: `read_source` → `transforms.transform_dataset` (snake_case names, timestamps to UTC, typing, `record_id` hashing) → `validation.validate_dataset` (per-dataset rule lists producing reject/warn code arrays) → `mark_duplicate_rows` → split into `standardized/` and `rejected/` → `write_delta` with read-back row-count verification → a monitoring row. Rejected rows keep original values, error codes and lineage.

### Query layer: one renderer, many SQL files

SQL lives as templates in `src/dic_pipeline/sql/` and every one of them — canonical queries, the product-backed rewrites in `sql/products/`, and benchmark variants — renders through `queries.render_template`. That function is the single source of the trip date filter, hourly-calendar bounds, weather-category `CASE` and PM2.5 bin `CASE`, so the variants are guaranteed to be the same text. **Do not inline those expressions in a new SQL file**; add a placeholder and render it.

`QUERY_DEFINITIONS` declares each query's required input columns (checked before execution with a useful error) and its exact result column order (checked after). `configs/analytical_queries.json` is validated strictly by `load_query_config`: weather categories must partition Meteostat codes 1–27 exactly, PM2.5 bins must be contiguous starting at 0 with an open last bin, and view names must be safe identifiers.

### Products must stay equivalent to the queries

`data_products.py` materializes four Delta products (`daily_mobility_summary`, `taxi_zone_statistics`, `weather_impact_summary`, `air_quality_impact_summary`), each with unique-key assertions and refresh metadata. `sql/products/qN_from_*.sql` answer the same Q1–Q6 from those products. `tests/test_product_query_alignment.py` re-aggregates the products and compares against the canonical query results — so a change to any query's grain, scope or labels requires updating the builder, the product SQL and this test together.

Products store observed rows only; zero-demand hours are padded at query time from the validated taxi window.

### Time model

Storage and the Spark session are UTC (`spark.sql.session.timeZone=UTC`); analysis is `America/New_York` from `analysis_timezone`. Hourly calendars in Q3–Q5 are clamped to `load_calendar_coverage()` — the `valid_pickup_start_utc` / `valid_pickup_end_utc_exclusive` window from `configs/datasets.json`, or the snapshot's `coverage_window` once incremental updates have extended it — because only inside it does "no trips" mean zero demand. `zoneinfo` needs the pinned `tzdata` on Windows.

### Monitoring

Every stage writes one row per target to `data/delta/metadata/pipeline_runs` through `monitoring.run_row` + `monitoring.record_run`, called from the stage's own success and failure paths. A stage failure always propagates (a failed monitoring write only adds a note); a lost row after a successful stage raises `MonitoringWriteError`. Count semantics and the contract for new stages are in `docs/w3_interfaces.md`: `duplicate_count` means "key already in the target", while in-file duplicates are rejected rows.

### Spark sessions

Always build sessions with `ingestion.create_spark()`. It resolves `JAVA_HOME` from `.venv` and `HADOOP_HOME` from `.hadoop`, enables the Delta extensions and catalog, and sets UTC plus **ANSI SQL mode** — overflow and bad casts raise instead of returning null, which several validation rules rely on.

Ingestion and integration overwrite their outputs; run one ingestion process per output directory.

## When changing things

- Field names, units, time assumptions or join rules → update `docs/data_contract.md`.
- Query semantics → update `docs/role_b_query_design.md` and the product alignment test.
- Never commit `.venv/`, `.hadoop/`, `data/`, generated tables or secrets.
- Keep replies to the user concise and in Chinese unless asked otherwise.
