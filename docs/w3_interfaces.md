# Week 3 interfaces: monitoring, incremental updates, validation

Role C owns monitoring (Task 3) and the evaluation (Task 5). Both depend on what the incremental pipeline (role A) and the validation framework (role B) report, so this page fixes names and meanings across the three implementations.

## 1. What C provides

### Monitoring table

`<delta_root>/metadata/pipeline_runs`, one row per stage and target in one execution. It replaces `metadata/ingestion_runs` and `analytics/metadata/product_refresh_runs`; `scripts.run_monitoring_report --import-legacy` copies their rows over once. The table lives under the Delta root even when products are written to another `--output-root`, so an evaluation run on a copied root keeps its own table.

| Column | Meaning |
| --- | --- |
| `run_id`, `stage`, `target`, `mode` | `stage` ∈ `ingestion`, `incremental_update`, `integration`, `product_refresh`; `target` is a dataset or product name; `mode` ∈ `full`, `incremental`, `skip` |
| `started_at`, `finished_at`, `execution_seconds` | UTC timestamps; timing of the stage itself |
| `processed_count` | in-scope input rows of this run |
| `inserted_count`, `updated_count` | rows added to / rewritten in the target |
| `duplicate_count` | rows skipped because their key **already exists in the target** (incremental only; 0 for a full overwrite) |
| `rejected_count` | rows written to `rejected/`, any code, including duplicates inside the same input file |
| `scope_excluded_count` | rows outside the dataset scope (Air Quality outside the five NYC counties) |
| `target_rows_before`, `target_rows_after` | target row count around the write; `before` is null for a full overwrite |
| `validation_enabled` | false when rules were switched off (evaluation only); null for stages without row validation |
| `validation_failure_counts_json`, `quality_flag_counts_json` | `{"code": count}`, same codes as `error_reasons` / `quality_flags` |
| `schema_version`, `rule_version` | versions of the **output** after any accepted evolution |
| `schema_changes_json` | accepted changes, e.g. `[{"op": "add", "column": "humidity", "type": "double", "nullable": true}]` |
| `input_paths_json` | source or update files read |
| `source_versions_json` | Delta versions read, from the manifest, e.g. `{"standardized_taxi": 3}` |
| `output_version`, `output_bytes`, `output_files` | Delta version written and its snapshot size |
| `status`, `error_message` | `success`, `failed` or `skipped`; message capped at 4,000 characters |

Invariants the evaluation checks:

```text
row-level stages:   processed = inserted + updated + duplicate + rejected
incremental stages: target_rows_after = target_rows_before + inserted
```

For Taxi, `updated_count` stays 0: `record_id` hashes the trip's values, so a corrected trip gets a new id and is an insert. Updates are meaningful for Weather and Air Quality, whose keys are the hour.

### Write API (`dic_pipeline.monitoring`)

Plain functions; each stage keeps its own `try/except`, as `ingest_dataset` and `build_integrated_table` already do:

```python
from .monitoring import record_run, run_row, utc_now

started_at, started = utc_now(), perf_counter()
try:
    counts = ...  # the stage's work; keys are the column names above
except Exception as error:
    row = run_row(run_id=run_id, stage="incremental_update", target=dataset, mode="incremental",
                  started_at=started_at, execution_seconds=perf_counter() - started,
                  status="failed", error=error, **counts_so_far)
    record_run(spark, row, delta_root, enabled=monitoring, error=error)
    raise
row = run_row(..., status="success", **counts)
record_run(spark, row, delta_root, enabled=monitoring)   # outside the try
```

- `run_row` rejects unknown keys and naive datetimes, and JSON-encodes `validation_failure_counts`, `quality_flag_counts`, `schema_changes`, `input_paths` and `source_versions`.
- Failure rules: if the stage failed, the stage error always propagates; a failed monitoring write only adds a note to it. If the stage succeeded but the row cannot be written, `MonitoringWriteError` is raised, since those counts cannot be recomputed. The row is printed to stderr in both cases.
- `monitoring=False` writes nothing and skips the metadata only monitoring needs (input file list, output table stats). Per-code rejection counts are validation reporting (Task 4) and run either way. Every entry point takes `monitoring`, and every CLI has `--no-monitoring`.

### Ops queries

`python -m scripts.run_monitoring_report [--query NAME] [--import-legacy] [--output file.json]` runs the SQL in `src/dic_pipeline/sql/monitoring/`: `validation_failures_by_target`, `failure_codes_by_target`, `processing_time_by_target`, `rejected_per_execution`, `processing_time_trend`.

## 2. What C needs from A (incremental pipeline, product refresh) — confirmed

In `src/dic_pipeline/incremental.py`:

```python
generate_update(spark, dataset, out_dir, *, delta_root, seed) -> dict
apply_updates(spark, updates, *, delta_root, run_id, validate=True, monitoring=True) -> list[dict]
```

- `generate_update` writes one update file for `taxi`, `weather` or `air_quality` and returns its manifest: `dataset`, `path`, `new_count`, `duplicate_count`, `schema_changes`, and the new time window. Deterministic for a given `seed`, because the evaluation regenerates the files.
- `apply_updates` applies a list of those manifests to standardized, rejected and integrated tables, then republishes `completed_batch.json` and `completed_integration.json`. It writes one `pipeline_runs` row per dataset with `stage="incremental_update"`, `mode="incremental"`, all count columns and `target_rows_before/after`, plus a row for the integrated table. Applying the same files twice must insert 0 rows the second time.
- `refresh_data_products(..., mode="auto" | "full")`: `auto` refreshes only affected products. Each returned record carries `refresh_mode`; a skipped product still writes a row with `mode="skip"`, `status="skipped"`. The evaluation checks that `auto` output equals a `full` rebuild.
- Every entry point takes `delta_root` and `run_id`; `scripts.run_integration` and `scripts.run_data_products` now accept `--run-id`.

## 3. What C needs from B (validation) — confirmed

- A switch: `prepare(..., validate=False)` threaded through `ingest_dataset(validate=...)` and `apply_updates(validate=...)`. With rules off, `error_reasons` still exists (empty) so the split and the within-file duplicate collapse keep working. Only used on evaluation copies; never published.
- `check_schema(actual_columns, expected_schema, policy) -> (accepted_changes, unsupported_changes)`, with unsupported changes raising the existing `SchemaValidationError`. `accepted_changes` goes into `schema_changes_json` as shown above.
- Existing error codes keep their names; new rules add new codes. The ops queries group by code.
- `reference_data={"taxi_zone_ids": ...}` supplies the current Zone snapshot to the Taxi validator. Missing pickup/drop-off references use `missing_reference_record`.
- Weather `humidity` and Air Quality `aqi` are the only allowed additions. They remain nullable for historical Delta rows, but new rows with a missing/out-of-range value are isolated with `incomplete_record` or `invalid_attribute_value`.
- `register_rule_builder(dataset, builder)` / `unregister_rule_builder(...)` add specialised rules without changing the validation dispatcher. `dataset="*"` registers a generic rule.

The full policy and product-consistency boundaries are in [w3_role_b_validation.md](w3_role_b_validation.md).

## 4. Cross-role decisions — confirmed

1. **Validity window.** A records the effective new window in the update manifest and extends the configuration used by `prepare`; B treats the new bound as a rule-version change; the coverage manifest extends Q3–Q5's zero-demand calendar over the same period.
2. **Provenance.** Published manifests keep the latest `run_id` and complete `lineage`; verification accepts table run ids as a subset of lineage and requires the latest run at the end.
3. **Cross-batch duplicates.** `mark_duplicate_rows` compares rows inside one input. The 1–2% copied Taxi trips only show up against the existing table (anti-join or MERGE on `record_id`); they count as `duplicate_count`, not `rejected_count`.
4. **Reading an evolved source.** The update reader inspects the CSV header or Parquet schema first, calls `check_schema`, and parses only the original schema plus accepted columns. It does not trust the manifest's claim.
5. **Manifests stay pinned.** `apply_updates` rewrites `completed_batch.json` atomically with all four current versions instead of deleting it like a standalone full ingestion.

## 5. How the evaluation uses this

`python -m scripts.run_w3_evaluation --list` shows each measurement and what it still waits for. Every run starts from a fresh copy of the baseline Delta root, so updates and overwrites never leak between runs.

| Measurement | Needs |
| --- | --- |
| `monitoring_overhead_ingestion` / `_integration` / `_refresh` | ready |
| `incremental_update` | A: `generate_update`, `apply_updates` |
| `analytical_refresh` (auto vs full) | A: `apply_updates`, `refresh_data_products(mode=)` |
| `storage_overhead` (snapshot vs on-disk bytes, commits) | A: `apply_updates` |
| `validation_overhead` | B entry point is ready; C wires and runs the on/off variants |
