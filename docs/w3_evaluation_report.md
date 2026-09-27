# Week 3 evaluation report: production readiness

This report covers the full-data evaluation of 2026-09-26, runs `20260926T174427Z-1ce27d00` (update and refresh) and `20260926T180757Z-afffd333` (validation and monitoring overhead), both on commit `adf0508`. It measures how long the three update files take to apply, how long the products take to refresh afterwards, how much storage the updates add, and how much time validation and monitoring add to the pipeline. The design these numbers test is in the [design report](w3_design_report.md).

## Method

The baseline is a fresh build with the Week 3 code: full ingestion of the four raw sources, integration, and a full product refresh into `data/benchmark/w3/baseline`. It holds 9,554,576 integrated trips; 202 of the 9,554,778 raw trips are rejected, the same as in Week 1. The baseline is never written. Every run, warm-up included, starts from its own copy of it, and copying is not timed. A reused workspace would carry one run's output into the next, and Delta keeps replaced files after an overwrite or `RESTORE`, which would inflate the storage numbers.

Each measurement compares two variants of one stage, or times one stage on its own. Each variant is warmed once and then measured three times in alternating order. The tables report medians of the three measured runs; every sample is in `results.json`. A run must reproduce the first run's output signature, or the measurement is reported as a mismatch without timings. The signature is the row counts for ingestion and integration, the per-target counts for the update, and for product refreshes the row count plus an order-independent hash of each product's content with the metadata columns excluded. Row counts alone would miss a refresh that leaves stale values in existing rows. No measurement reported a mismatch.

The update files are generated once from the baseline (seed 0) and applied once to another copy, untimed. The refresh comparison starts from that copy, and its storage report against the baseline's gives the storage overhead.

| Measurement | Variants | What differs |
| --- | --- | --- |
| `incremental_update` | apply | the three update files applied to the baseline |
| `analytical_refresh` | full, auto | all four products rebuilt, or only what changed since they were built |
| `validation_overhead` | off, on | full four-dataset ingestion with the validation rules switched off or on |
| `monitoring_overhead_ingestion` | off, on | full ingestion without or with the `pipeline_runs` rows |
| `monitoring_overhead_integration` | off, on | integration without or with its row |
| `monitoring_overhead_refresh` | off, on | full product refresh without or with its rows |

## Environment

Windows build 26200, Intel i9-13900HX, 31.7 GB RAM, the same machine as the Week 2 benchmark. Python 3.11.9, Java 21.0.7, Spark 4.2.0, Delta 4.4.0. Spark runs `local[4]` with a 4 GiB driver heap, 128 shuffle partitions, UTC session time zone, ANSI mode and AQE on. Nothing else ran on the machine during the measurements.

## Results

### Incremental update

The update holds 668,820 new Taxi trips (7% of the baseline), 143,319 copies of existing trips (1.5%), and 168 new hours each of Weather (with `humidity`) and Air Quality (with `aqi`). Applying it takes a median of 134.4 s (samples 136.3, 134.4 and 133.2 s).

| Target | Processed | Inserted | Skipped as duplicate | Rejected | Median seconds |
| --- | ---: | ---: | ---: | ---: | ---: |
| `standardized/taxi` | 812,139 | 668,820 | 143,319 | 0 | 33.4 |
| `standardized/weather` | 168 | 168 | 0 | 0 | 16.3 |
| `standardized/air_quality` | 168 | 168 | 0 | 0 | 13.8 |
| `integrated_taxi_trips` | 668,820 | 668,820 | | | 43.7 |
| Whole apply, wall clock | | | | | 134.4 |

The other 27 s go to reading the tables before and after, publishing the two manifests, and the provenance check. For comparison, a full ingestion and integration of the original data alone takes 224.9 s plus 87.8 s (the monitoring-on medians below), 312.7 s in total, before any of the new records are processed.

Applying the same files a second time, on a separate copy, inserted nothing: all 812,139 Taxi rows were counted as duplicates, the integrated table received 0 rows, and the automatic refresh that followed skipped all four products.

### Analytical refresh

A Taxi update moves the integrated table, so `mode="auto"` refreshes all four products: the two hour-keyed ones by MERGE, the other two by rebuild. All 2,183 hours the MERGE touched were new, because every new trip falls after the original period.

| Product | `full` (s) | `auto` (s) | `auto` path |
| --- | ---: | ---: | --- |
| `daily_mobility_summary` | 11.0 | 20.8 | MERGE of 2,183 hours |
| `air_quality_impact_summary` | 7.7 | 14.7 | MERGE of 2,183 hours |
| `taxi_zone_statistics` | 10.9 | 10.1 | rebuild |
| `weather_impact_summary` | 12.3 | 11.5 | rebuild |
| Whole refresh, wall clock | 66.3 | 87.2 | |

The automatic refresh reproduced the full rebuild's product contents in every run, and it was 20.9 s (31.6%) slower.

### Storage overhead

| Table | Snapshot before | Snapshot after | Files in snapshot | Commits |
| --- | ---: | ---: | ---: | ---: |
| `standardized/taxi` | 1,042.3 MB | 1,122.6 MB | 8 to 136 | 1 to 2 |
| `standardized/weather` | 0.78 MB | 1.71 MB | 1 to 90 | 1 to 3 |
| `standardized/air_quality` | 3.95 MB | 5.02 MB | 1 to 90 | 1 to 3 |
| `integrated/integrated_taxi_trips` | 1,058.1 MB | 1,142.1 MB | 91 to 182 | 1 to 2 |
| `metadata/pipeline_runs` | 0.08 MB | 0.12 MB | 9 to 13 | 9 to 13 |
| All Delta tables | 2,105.5 MB | 2,271.8 MB | 116 to 517 | 22 to 32 |

The current snapshots grew by 166.3 MB (7.9%), close to the 7% more trips. Bytes on disk outside any snapshot (transaction logs, checksum files and replaced files) grew from 17.1 MB to 19.6 MB, the transaction logs alone from 0.6 MB to 1.9 MB. The update replaced almost no data: Taxi is insert-only, and the Weather and Air Quality MERGEs matched no existing hour. The visible cost is the file count. The Weather and Air Quality MERGEs each wrote 89 new files for 168 rows, so each of those tables now reads 90 small files instead of one.

### Validation overhead and statistics

| Dataset | Rules off (s) | Rules on (s) | Difference |
| --- | ---: | ---: | ---: |
| Taxi | 116.8 | 162.5 | +45.7 s |
| Weather | 8.8 | 9.8 | +1.0 s |
| Air Quality | 18.3 | 19.0 | +0.7 s |
| Taxi Zones | 7.7 | 8.0 | +0.3 s |
| Whole ingestion, wall clock | 176.1 | 223.1 | +47.0 s (+26.7%) |

With the rules off, Taxi rejects only its one in-file duplicate. With them on, it rejects 202 of 9,554,778 trips (0.0021%): 180 for `dropoff_before_pickup`, 21 for `timestamp_outside_source_period` and 1 `duplicate_record`. No trip failed the new `missing_reference_record` rule, and Weather, Air Quality and Taxi Zones rejected nothing. The quality flags, which do not reject, mark among others 215,764 zero-distance Taxi trips, 136,567 negative fares, 553 Weather hours without precipitation and 136 qualified Air Quality measurements. Air Quality keeps 51,885 of 8,139,551 national rows as in scope. The update itself was clean: none of its 812,139 Taxi rows or 336 environment rows were rejected.

### Monitoring overhead and results

| Stage | Off (s) | On (s) | Difference | Rows written |
| --- | ---: | ---: | ---: | ---: |
| Ingestion, four datasets | 199.7 | 224.9 | +25.2 s (+12.6%) | 4 |
| Integration | 77.2 | 87.8 | +10.7 s (+13.8%) | 1 |
| Product refresh, four products | 44.1 | 70.4 | +26.3 s (+59.8%) | 4 |

The overhead follows the number of rows, not the amount of data. A separate probe appended single rows to a copy of `pipeline_runs`: the first write in a new Spark session took 15.3 s and the next four 10.5, 6.5, 6.5 and 5.2 s. Reducing Delta's snapshot partitions to one saved about a second per write, so the cost is the Delta commit itself (a write job and the log entry) rather than the size of the row. Ingestion and integration also read the output table's statistics for their rows, which is why integration pays about 11 s for a single row.

A monitoring report over a baseline copy, after one update, a repeated update and two automatic refreshes, answered the four operational questions directly. Taxi is the only dataset that fails validation, at a rejected share of 0.0021%, all of it at full ingestion. Taxi ingestion is the slowest stage (247 s, 38,600 records per second), followed by integration (143 s). The repeated update showed up as 812,139 duplicates and 0 inserts. The trend query also caught a code change: once each product was materialized once, the full rebuilds of `taxi_zone_statistics` and `weather_impact_summary` dropped from 30 s to 10 and 12 s.

## Interpretation

The incremental update pays off, but its cost grows with the table. Applying the update took 134 s, against 313 s just to rebuild the original data. The largest parts are the integrated append (44 s, which enriches 668,820 trips and writes 91 new date partitions) and the Taxi MERGE (33 s). The Taxi MERGE matches `record_id` against the whole standardized table, which is not partitioned, so its cost grows with the history rather than with the update.

The automatic refresh is correct but slower than a rebuild at this scale. Finding the dirty hours reads the `run_id` column of both integrated versions, about 20 million rows, and recomputing those hours reads the table again. A full rebuild of an hour-keyed product is a single aggregation over the same table, and the products are small (773 to 4,366 rows). The incremental path would pay off only if the changed rows could be found without a scan, for example through Delta change data feed or a partition predicate, or if the products were expensive to rebuild. The automatic mode does save work when the integrated table has not moved: after a Weather-only or Air Quality-only update, or a repeated update, it skips every product.

The evaluation also caught two defects before these runs. The first full run measured the automatic refresh at 130.7 s against 84.9 s for a full rebuild. The lazy product was recomputed for the key check, the count, the new-key count and the MERGE, each time over all integrated trips. Materializing it once made both paths faster, and the full rebuild dropped to 66.3 s. The update generator also read timestamps back with `collect()`, which returns host-local times. On this UTC+8 machine that put the Taxi window end 8 hours late and left an 8-hour gap before the new Weather and Air Quality hours. Both are fixed and covered by tests, and neither affects the numbers above.

Storage grows with the data, and the file count grows faster. The byte overhead beyond the new data is about 2.5 MB. The file count is what to watch: small MERGE updates multiply files, and a daily feed would turn one Weather file into thousands. Running `OPTIMIZE` after each update, or coalescing the MERGE output, would keep it in check.

Validation costs about a quarter of ingestion time, almost all of it in Taxi. The Taxi builder evaluates seven error codes and seven quality flags on each of 9.55 million rows, and the numeric check alone covers eleven columns. The environment datasets are small enough that their rules cost about a second. For 202 trips that would otherwise distort durations and demand counts, the cost is acceptable, and it is paid once per ingestion, not per query.

Monitoring needs batching before it scales to many targets. At about 6 s per row, the overhead is 13% of ingestion but 60% of the short product refresh. Writing all rows of a run in one commit would cut ingestion and refresh from four commits to one each. The rows of a run that crashes before that commit would then be lost unless the commit sits in a `finally` block.

## Discussion

### Which Week 1 decisions simplified maintenance

The per-dataset contract in `configs/datasets.json` and the single `prepare` entry point did most of the work. An update file runs through the same standardization, validation, duplicate marking and split as a full ingestion, so incremental validation needed no new code, and schema evolution became one more block in the contract (`schema_evolution.allow_add`). `transforms.py`, `schemas.py` and `contracts.py` did not change at all in Week 3, and `preparation.py` changed by 19 lines. The content hash in `record_id` turned cross-batch duplicate detection into a MERGE condition on one column. With the version-pinned manifests, readers needed no change: an update publishes the same two files a full build does, with two extra fields. Because every path is relative to one Delta root, the evaluation could run each measurement on a throwaway copy.

### Which components required the largest modifications

Measured against the end of Week 2 (`4ac1fe5`), the new code is the incremental pipeline (`incremental.py`, 808 lines), the monitoring module (319 lines) and the evaluation (323 lines). Among existing modules, the product refresh changed most (`data_products.py`, 300 lines added and 103 removed): Week 2 products assumed a full overwrite, and refreshing only some keys needed per-product knowledge of which keys new trips touch. Validation grew by 227 lines for the schema check and the rule registry. Two small Week 1 assumptions had large effects. `verify_integrated_provenance` accepted exactly one ingestion run, and the valid pickup window was a constant in the config; both had to become a lineage and a published window before any update could succeed.

Review before the evaluation corrected four points in the first incremental version: new Taxi trips that were shifted by only one day, a refresh that missed existing hours after a rerun, a rerun that never appended trips lost to a failed integration step, and a coverage window read from the default Delta root whatever root was queried. Tests now cover each case.

### How well the platform supports future datasets

A new source needs a raw schema, a transform, a rule builder and a config entry; ingestion, validation reporting, rejection, monitoring and the evaluation handle it without changes. Two places are still written per dataset: the MERGE key in `apply_updates` is a hard-coded map rather than the `duplicate_key` already in the config, and each update generator is written by hand. A source that enriches trips also needs a join in `integration.py` and, if products should use it, a product that declares the dependency. New attributes of existing sources are cheap if they are additive and listed in the contract, as `humidity` and `aqi` showed.

### What we would change if we redesigned it today

We would take the MERGE key from the dataset contract, and enable Delta change data feed on the integrated table so the refresh reads the rows added since a product's version instead of comparing run ids over the whole table, which could make the automatic refresh cheaper than a rebuild. We would re-enrich the affected trip hours when a Weather or Air Quality correction arrives, which the current design does not do, and partition the standardized Taxi table by pickup date so a Taxi MERGE prunes to the days an update touches. Monitoring rows of one run would go into a single commit. Finally, the stages would run from one orchestrated entry point instead of four commands in the right order.

## Limitations

All figures come from one machine, `local[4]`, one data size and three measured runs per variant. The operating-system file cache is not controlled. The warm-up absorbs cold reads, and the first run in a fresh session is slower: the baseline build took 247 s for Taxi ingestion against a measured median of 162 s. The update is one fixed batch (seed 0) sized by the assignment's percentages, so the numbers show one point rather than how cost scales with update size. Monitoring overhead is a few seconds per row, the same order as the spread between repeated ingestion runs, so it should be read together with the per-run samples in `results.json`. The storage figures do not include `VACUUM`, which the evaluation never runs.

## Reproduction and evidence

```powershell
.\.venv\Scripts\python.exe -m scripts.run_ingestion --dataset all --output-root data/benchmark/w3/baseline
.\.venv\Scripts\python.exe -m scripts.run_integration --delta-root data/benchmark/w3/baseline
.\.venv\Scripts\python.exe -m scripts.run_data_products --delta-root data/benchmark/w3/baseline
.\.venv\Scripts\python.exe -m scripts.run_w3_evaluation --baseline-root data/benchmark/w3/baseline
```

Each run writes `data/benchmark/w3/<run_id>/results.json` with the baseline manifests and storage, the environment, the method, the update manifests, the updated copy's storage, and every sample with its output signature and the per-stage monitoring rows. The two runs above ran the update-based and the overhead measurements separately with `--measurement`. The monitoring results come from `run_monitoring_report --output` over a baseline copy to which the README's Week 3 commands were applied. The first full run, before the refresh fix, is `20260926T164337Z-aeab3e72`. The raw samples are also in [w3_evaluation_timings.csv](w3_evaluation_timings.csv).
