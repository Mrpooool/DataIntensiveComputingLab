# Week 3: Incremental updates, consistency, validation and monitoring

Week 1 built a batch platform: four raw sources are validated into `standardized/` Delta tables, trips are enriched into `integrated/integrated_taxi_trips`, and each stage publishes a manifest naming the exact Delta versions it produced. Week 2 put six queries and four data products on top of that snapshot. In Week 3 the platform takes new data without a rebuild, keeps the products correct afterwards, validates more, and records every stage in one monitoring table.

The Week 1 rule still holds: no stage reads "latest". An incremental update ends by rewriting `completed_batch.json` and `completed_integration.json` with the new versions, so queries and products read a consistent snapshot even while an update is running. The measurements are in the [evaluation report](w3_evaluation_report.md), and the column-level contracts between the three roles are in [w3_interfaces.md](w3_interfaces.md).

## 1. Update files

`python -m scripts.run_incremental generate` writes one update file per dataset from the published tables (`generate_update`, seed 0). Each file holds only new or repeated records. The counts below come from the manifest each call returns.

| File | New records | Duplicates | Schema change |
| --- | ---: | ---: | --- |
| `taxi_trips_update.parquet` | 668,820 (7% of 9,554,576) | 143,319 (1.5%) | none |
| `weather_update.csv` | 168 (2025-01-01 00:00 to 01-07 23:00 UTC) | 0 | adds `humidity` (double, 20 to 100) |
| `air_quality_update.csv` | 168 (one monitor, 2025-01-01 05:00 to 01-08 04:00 UTC) | 0 | adds `aqi` (double, 0 to 500) |

New Taxi trips are a sample of existing trips moved forward by whole weeks, one week more than the sample spans. Every new pickup therefore lies after the latest original one, and the weekday and time-of-day patterns, zones, distances and fares stay realistic. On the full data the shift is 13 weeks. The original trips end at 2024-04-01 03:59:59 UTC and the new ones fill April to June, so the Taxi validity window, and with it the Q3 to Q5 calendar, now ends at 2024-07-01 04:00 UTC. Duplicates are exact copies of other existing trips. The Weather and Air Quality files continue the hourly series for the week after their last observation. `humidity` equals the row's `rhum`, which Meteostat already reports as relative humidity.

## 2. Incremental processing strategy

`apply_updates` takes each update file through four steps and then publishes once.

1. The reader checks the schema before parsing. It takes the CSV header or the Parquet schema, compares it with the dataset contract through `check_schema`, and parses the file with only the original columns plus the accepted additions. It does not trust the manifest's description of the change.
2. The file goes through the same `prepare` call as a full ingestion: standardization, validation, duplicate marking within the file, and the split into accepted and rejected rows. Rejected rows are appended to `rejected/<dataset>` with their error codes.
3. Accepted rows are merged into the standardized table. Taxi merges on `record_id` and only inserts. The id hashes the trip's values, so a copied trip matches an existing row and is skipped, while a corrected trip would arrive under a new id. Weather merges on the UTC hour and Air Quality on the seven-column monitor key; both insert new keys and update existing ones. Accepted new columns are added first with `ALTER TABLE ... ADD COLUMNS`, so historical rows read them as null.
4. New trips are enriched and appended to the integrated table. The step selects the standardized Taxi rows whose ingestion `run_id` the integrated table does not hold yet. That covers this run's inserts and also those of an earlier run that failed after its MERGE, which a rerun would otherwise never append because it inserts no Taxi row itself. The appended trips join the current Weather and Air Quality tables, so a trip in a new hour picks up the new observations.

Publishing rewrites `completed_batch.json` with the four current table versions, the new `run_id`, a `lineage` list of every ingestion run the tables contain, and the extended `coverage_window`, then republishes the integration snapshot. `verify_integrated_provenance` accepts the integrated table when its run ids are a subset of the lineage and the lineage ends with the current run. If any step fails, the manifests stay as they were and downstream readers keep the previous versions.

Applying the same files twice inserts nothing the second time. The whole update runs in one Spark session and assumes one writer per Delta root, as Week 1 did.

## 3. Analytical consistency strategy

All four products read only the integrated table, so the question of which products an update affects reduces to whether the integrated table has moved since each product was built. Every product row stores `source_delta_version`, the integrated version it was computed from. `refresh_data_products(mode="auto")` compares the largest stored value with the registered snapshot and skips products that are current. A Weather or Air Quality update adds hours after the last trip and changes no integrated row, so it refreshes nothing. A Taxi update refreshes all four products.

### Which products can be refreshed incrementally

`daily_mobility_summary` and `air_quality_impact_summary` are keyed by UTC pickup hour. For them the refresh finds the hours holding trips from ingestion runs that were absent in the version the product was built from, recomputes those hours from all their trips, and MERGEs the result. Because the check uses run ids rather than the latest run, it also works after a rerun that inserted nothing itself, and after two updates with no refresh in between. Both refresh paths compute a product once and reuse the materialized result for the key check, the row count and the write.

### Which require complete recomputation

`taxi_zone_statistics` (month by zone) and `weather_impact_summary` (zone by weather category) have keys that new trips spread across, so the refresh rebuilds them from the snapshot. They are small (the four products together are 0.02% of the integrated table), and a rebuild costs one aggregation over the trips. Every product also needs a full rebuild when its definition changes: a new key, a different weather-category mapping or PM2.5 bins, or a timezone change. `mode="full"` stays available for that and for audits.

### Compatibility with the existing queries

The evolved columns stay in the standardized tables. Integration does not select them, so the integrated schema, the six queries and the four products are unchanged. Q3 to Q5 count hours without trips as zero demand, which is only valid inside the period the Taxi data covers. The update extends that window, and the snapshot carries it as `coverage_window`, so the query calendar follows exactly the snapshot being queried. Invalid rows never reach the products because step 2 splits them off.

### Which schema changes are handled automatically

Additions listed with a type and nullability under `schema_evolution.allow_add` in `configs/datasets.json` are accepted; today these are `humidity` and `aqi`. Removing or renaming a column, an unlisted addition, a type change, a duplicate column name, or relaxing a required non-null field stops the update with `SchemaValidationError` before any table is written for that dataset. Those changes need a contract change and usually a migration: whatever transform, validation rule, join or query uses the column has to change with it, and products built on it need `mode="full"`.

## 4. Validation framework

Validation runs inside `prepare`, after standardization and before the split, for full ingestion and updates alike. Each rule pairs a stable code with a Spark column condition. Error rules reject the row and append the code to `error_reasons`; quality rules only add a flag. An invalid row never stops the batch. It is isolated in `rejected/`, counted by code in the monitoring table, and kept out of the integrated table and products. An unsupported schema change breaks the contract for the whole file, so it fails that update instead.

Week 3 added these checks:

| Check | Code | Scope |
| --- | --- | --- |
| Pickup or drop-off zone missing from the Taxi Zone lookup | `missing_reference_record` | Taxi |
| Evolved column present but null | `incomplete_record` | Weather `humidity`, Air Quality `aqi` |
| `humidity` outside 0 to 100 or `aqi` outside 0 to 500, or not finite | `invalid_attribute_value` | same |
| Unlisted, removed, retyped or duplicate column | `SchemaValidationError` | any update |

Generic rules are those whose meaning does not depend on the source: a missing business-key component, a key that appears twice in the same input (one row wins deterministically and the rest are rejected as `duplicate_record`), a NaN or infinite numeric value, and the schema contract check. Dataset-specific rules carry domain knowledge: drop-off before pickup and the valid pickup window for Taxi, meteorological ranges for Weather, units and the NYC county scope for Air Quality, the zone reference set, and the ranges of the evolved columns.

Adding a rule does not touch the dispatcher. `register_rule_builder(dataset, builder)` adds a function that returns `(error_rules, quality_rules)` for one dataset, or for all of them with `dataset="*"`, and `unregister_rule_builder` removes it. Reference data such as the zone set is passed in explicitly, so a rule depends only on the rows and the snapshot being validated. A new code needs no change elsewhere, because monitoring stores counts as a code-to-count map and the ops queries explode it.

`validate=False` on `prepare`, `ingest_dataset` and `apply_updates` switches the rules off but keeps the empty code columns and the duplicate collapse, so the rest of the pipeline sees the same shape. It exists only to measure the validation overhead on a scratch copy.

## 5. Monitoring architecture

Every stage writes one row per target into a single Delta table, `metadata/pipeline_runs`, which replaced the Week 1 ingestion log and the Week 2 refresh log. The stages are `ingestion`, `incremental_update`, `integration` and `product_refresh`. A row holds the run id, the mode, UTC start and end, the duration, the row counts, the per-code validation and quality-flag counts, schema and rule versions, accepted schema changes, the input files, the Delta versions read and written, and the status with any error message.

The tests check two invariants on the counts: `processed = inserted + updated + duplicate + rejected` for row-level stages, and `target_rows_after = target_rows_before + inserted` for incremental ones. `duplicate_count` means only "key already in the target"; a repeat inside one input file is a rejected row. The first invariant holds only because the two are kept apart.

A failed stage always raises its own error. If the monitoring write fails too, that failure is added as a note to the stage's exception and never replaces it. A successful stage whose row cannot be written raises `MonitoringWriteError`, because those counts cannot be recomputed later. The row is printed to stderr in both cases. `monitoring=False` (`--no-monitoring`) skips the row and the metadata that only monitoring needs, which is how the overhead is measured.

`python -m scripts.run_monitoring_report` answers the operational questions with five SQL files over the table. `validation_failures_by_target` and, per code, `failure_codes_by_target` show which datasets fail validation most often; `processing_time_by_target` shows which stages take longest; `rejected_per_execution` counts rejected records per execution; and `processing_time_trend` tracks processing time with a three-run moving average and the change from the previous run.

### Which operational metrics are most useful

The rejected share per code and target comes first: a jump in one code after an update points at the source rather than at our code. Duration per target over time is next, since a stage that grows faster than its input is a regression. In updates, `inserted` against `duplicate` shows a replayed file immediately, because it comes back as all duplicates. The source and output versions tie every product row to the snapshot it was computed from.

### How this supports debugging and maintenance

A failed refresh row names the integrated version it read, so the failure can be replayed on the same snapshot. A broken invariant means rows were lost between the split and the write. The error message and input paths identify the file without a search through logs. Output size per Delta version shows how fast each table grows.

### What a production system would add

Alerts on thresholds for rejected share, duration and freshness, instead of queries someone has to remember to run. Data freshness itself, the lag between a source file arriving and its snapshot being published. Spark-level resource metrics such as shuffle bytes, spill and peak executor memory, which explain slow stages that row counts cannot. And a retention job with its own row, since Delta keeps every replaced file until `VACUUM`.

## 6. Engineering decisions and trade-offs

| Decision | Why | Cost |
| --- | --- | --- |
| Version-pinned manifests extended with `lineage` and `coverage_window` | Readers never see a half-applied update; the query calendar follows the snapshot | Every stage must publish, and a failure leaves standardized tables ahead of the manifest until a rerun |
| Taxi insert-only on a content hash | The MERGE skips exact duplicates with no extra step | A corrected trip becomes a new trip, and the old one stays |
| Weather and Air Quality upsert on the hour key | Corrections replace values in place | Trips enriched earlier keep the old values; a correction for an hour that already has trips needs those hours re-integrated |
| Integrated append by missing run id | A rerun repairs a partial update with no manual step | Relies on each append being atomic per run, which Delta provides |
| Refresh decided by the product's source version | No side file to keep in sync; several updates between refreshes are handled | Only the integrated table is tracked, which is enough because no product reads anything else |
| Evolved columns kept out of integration | Queries and products need no change | New attributes stay unused until a product declares them |
| One monitoring table for all stages | One place to query and one set of count definitions | A one-time migration of the Week 1 and Week 2 logs (`--import-legacy`) |
| Evaluation on fresh copies of the baseline | `RESTORE` keeps old files, so storage numbers would be inflated | Copying 2 GB per run adds untimed setup |

Known limitations: updates assume a single writer; Weather and Air Quality corrections do not flow into trips that are already integrated; the generator's manifest supplies the new end of the Taxi validity window; and `taxi_zone_statistics` and `weather_impact_summary` are always rebuilt in full when trips arrive.
