# Week 3 Role A: incremental updates and analytical refresh

Implements Assignment Tasks 1–2 for role A. Contract details live in [w3_interfaces.md](w3_interfaces.md).

## What was delivered

| Piece | Location |
| --- | --- |
| Update file generation | `dic_pipeline.incremental.generate_update` |
| Incremental apply + integrate append | `dic_pipeline.incremental.apply_updates` |
| Product refresh modes | `refresh_data_products(..., mode="full"\|"auto")` — auto MERGEs hour products / rebuilds aggregate products |
| CLI | `python -m scripts.run_incremental generate\|apply` |
| Coverage window for Q3–Q5 | `metadata/coverage_window.json` (read by `queries.load_calendar_coverage`) |
| Lineage-aware provenance | `integration.verify_integrated_provenance` accepts `lineage` |
| Additive schema gate | `validation.check_schema` (humidity / aqi allow-list for A; B extends) |

## Behaviour summary

1. **Taxi update file (Parquet):** CLI / default API uses **7%** new trips (within 5–10%) with timestamps shifted after the latest standardized pickup, plus **1.5%** exact copies (within 1–2%). Built in Spark (no row collect). Unit tests may pass smaller `new_fraction` / `duplicate_fraction` on tiny fixtures only.
2. **Weather / Air CSV:** default is **seven days** of new hours after the latest observation; Weather adds `humidity` (20–100), Air adds `aqi` (0–500). Tests may pass `new_hours=…` to keep fixtures small.
3. **`apply_updates`:** `prepare` → MERGE into standardized (insert-only for Taxi; insert/update for hour-keyed Weather/Air) → append rejected → append only newly inserted Taxi rows to the integrated table → rewrite `completed_batch.json` (with `lineage`) and `completed_integration.json` → write `last_update_affects.json` and `coverage_window.json`. Monitoring rows use `stage=incremental_update`.
4. **Idempotence:** applying the same manifests again inserts 0 Taxi rows.
5. **Product refresh:** `mode=full` rebuilds every selected product; `mode=auto` refreshes only names in `last_update_affects.json`. For those, `daily_mobility_summary` and `air_quality_impact_summary` use dirty-hour MERGE (`refresh_mode=incremental`); `taxi_zone_statistics` and `weather_impact_summary` are fully rebuilt (`refresh_mode=full`). Unaffected products get `status=skipped` / `mode=skip`.

## How to run

```powershell
# After a completed W1/W2 Delta root exists under data/delta
.\.venv\python.exe -m scripts.run_incremental generate --out-dir data/updates --seed 0
.\.venv\python.exe -m scripts.run_incremental apply --manifests data/updates/update_manifests.json
# If apply MERGEd taxi but integrated stayed unchanged (partial/idempotent re-apply):
.\.venv\python.exe -m scripts.run_incremental sync-integrated
.\.venv\python.exe -m scripts.run_data_products --mode auto
.\.venv\python.exe -m scripts.run_data_products --mode full
```

## Review notes (assignment checklist)

- New/modified records only in update files; originals untouched until MERGE.
- Duplicates ignored (cross-batch via MERGE); unchanged rows preserved.
- Schema evolution for agreed additive columns without rebuilding the platform.
- Analytical products can refresh selectively after updates; full rebuild remains available.
- Invalid rows stay in `rejected/` and do not enter products (same `prepare` path as W1).

## Task 2 status

**Implemented** for role A (code paths above). Mapping and refresh behaviour:

| Requirement | How we meet it |
| --- | --- |
| Refresh only affected products | `apply_updates` writes `metadata/last_update_affects.json` via `PRODUCTS_BY_DATASET`; `refresh_data_products(mode="auto")` refreshes those names and records `status=skipped` for the rest |
| Schema evolution | Weather `humidity` / Air `aqi`: generate → `check_schema` allow-list → Delta `ALTER` + MERGE with `mergeSchema` |
| Query compatibility | Evolved columns are **not** selected into `integrated_taxi_trips`; W2 product builders and Q1–Q6 SQL keep the same grains/keys |
| Minimize recomputation | Integrated taxi is **append-only** for newly inserted trips; hour-grained products MERGE dirty keys; month/zone and weather-category products selectively full-rebuild; `mode=full` remains for audits |

Affected-product map (`PRODUCTS_BY_DATASET`):

| Updated dataset | Products marked affected |
| --- | --- |
| `taxi` | all four (`daily_mobility_summary`, `taxi_zone_statistics`, `weather_impact_summary`, `air_quality_impact_summary`) |
| `weather` | `weather_impact_summary` only |
| `air_quality` | `air_quality_impact_summary` only |

Refresh strategy under `mode=auto`:

| Product | Strategy |
| --- | --- |
| `daily_mobility_summary` | **Incremental** dirty-key MERGE on `pickup_hour_utc` |
| `air_quality_impact_summary` | **Incremental** dirty-key MERGE on `pickup_hour_utc` |
| `taxi_zone_statistics` | **Full rebuild** of that product only |
| `weather_impact_summary` | **Full rebuild** of that product only |

Dirty hours = distinct `pickup_hour_utc` from integrated trips with the latest batch `run_id`, union hours present in integrated but missing from the product. Those hours are re-aggregated from **all** trips in the hour (so overlapping inserts stay correct), then MERGED (`whenMatchedUpdateAll` / `whenNotMatchedInsertAll`). Missing product table → falls back to full overwrite.

After a successful apply, run:

```powershell
.\.venv\python.exe -m scripts.run_data_products --mode auto
```

## Task 2 discuss

### Which analytical data products can be refreshed incrementally?

- **`daily_mobility_summary`** — grain `pickup_hour_utc`. Implemented as dirty-hour MERGE.
- **`air_quality_impact_summary`** — grain `pickup_hour_utc` (NYC scope). Same MERGE path.

### Which require complete recomputation?

- **`taxi_zone_statistics`** — grain `(local_pickup_month, pickup_location_id)`. New trips update existing month×zone aggregates; `mode=auto` fully rebuilds this product when affected.
- **`weather_impact_summary`** — grain `(pickup_location_id, weather_category)`. Category aggregates are not hour-append-safe; `mode=auto` fully rebuilds this product when affected.

### Which schema changes can be handled automatically?

- **Additive, nullable columns** on the allow-list (`ALLOWED_EVOLUTION`): Weather `humidity` (double), Air `aqi` (double).
- Path: update CSV includes the column → validation accepts it → `_ensure_delta_columns` / MERGE with schema auto-merge → standardized tables gain the column without a platform rewrite.
- Downstream stays compatible because integration **drops** these columns before join, and products/queries never require them.

### Which schema changes require manual intervention?

- **Rename, drop, or type change** of columns used by transforms, validation rules, integration joins, or Q1–Q6 / product SQL.
- **Non-nullable required fields** or columns that must appear in the integrated table / product keys.
- **Grain or semantic changes** (e.g. new weather category CASE, different PM2.5 bins, new product keys) → update `configs/`, builders, SQL under `sql/products/`, bump `schema_version` in `configs/data_products.json`, and usually run `mode=full`.
- **Breaking raw layouts** outside the agreed allow-list → Role B extends `check_schema` / `prepare(validate=…)`; Role A does not auto-accept those.

### How does your design reduce unnecessary computation?

1. **Dataset → product dependency map** so weather-only updates do not touch mobility / zone / air products.
2. **`mode=auto` vs `mode=full`**: auto skips unaffected products; among affected ones, hour-grained products MERGE only dirty keys instead of rewriting the whole product.
3. **Append-only integrated path** for newly inserted taxi `record_id`s instead of re-joining the entire historical taxi table on every apply.
4. **Hour-keyed MERGE** for weather/air standardized tables (insert/update only touched hours).
5. **Coverage window metadata** (`coverage_window.json`) so Q3–Q5 calendar padding follows the new valid end without a second full trip scan in every query.
6. Evolved columns stay on standardized weather/air only until analysts explicitly need them.

## Open hand-offs

- B: extend `check_schema` policy and `prepare(validate=…)`; confirm product refresh boundaries if they differ from `PRODUCTS_BY_DATASET`.
- C: evaluation measurements `incremental_update` / `analytical_refresh` / `storage_overhead` should now resolve once this branch is merged; confirm **agree** items in `w3_interfaces.md`.
- Ops: if a partial apply left new taxi in standardized but `integrated_inserted=0`, re-append those trips (or re-run integrate for the missing `record_id`s) before trusting `mode=auto` product outputs.
