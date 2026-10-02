# Standardization and Validation Contract v1

This document defines the output expected from Role B's `prepare(df, dataset_config)` function.
It is the interface between A's reader/writer and C's integration pipeline.

## Interface

```python
result = prepare(raw_df, dataset_config, run_id="...")
result.accepted
result.rejected
result.metrics
# After A has written both DataFrames:
result.release()
```

For Taxi, Weather and Zones:

```text
raw_input_count = input_count = accepted_count + rejected_count
```

For the national Air Quality file:

```text
raw_input_count = scope_excluded_count + input_count
input_count = accepted_count + rejected_count
```

Rows outside NYC scope are valid but irrelevant source rows, so they are excluded rather than
written to the rejected table.

## Shared conventions

- Standard column names use lower `snake_case`.
- Spark session timezone must be `UTC`.
- Canonical integration timestamps are Spark `timestamp` values named `*_utc`.
- Original/source timestamps are retained where needed to audit timezone conversion.
- Dataset and rule versions are written on every accepted/rejected row.
- `source_file` and `run_id` provide lineage.
- Rejected rows retain `raw_record_json` and an array of `error_reasons`.
- Suspicious but potentially legitimate values use `quality_flags` and remain accepted.
- Duplicate detection prefers a row without validation errors, then sorts by source file and raw JSON.
- Rule version `1.0.1` rejects malformed required timestamps, missing Air units and NaN/Infinity measurements.
- The Taxi fingerprint covers all 19 business source fields and excludes file path, run ID and
  derived environmental fields, preventing merely similar trips from being collapsed.

## Standard keys and time fields

| Dataset | Standard record key | Integration key |
| --- | --- | --- |
| Taxi | `record_id` stable fingerprint | `pickup_hour_utc` |
| Weather | `weather_hour_utc` | `weather_hour_utc` |
| Air Quality | State/county/site/parameter/POC/GMT hour/method | `air_quality_hour_utc` |
| Taxi Zones | `location_id` | `location_id` |

## Important mappings

| Dataset | Source | Standard | Type/meaning |
| --- | --- | --- | --- |
| Taxi | `tpep_pickup_datetime` | `pickup_timestamp_source` | Naive New York wall time |
| Taxi | derived | `pickup_timestamp_utc` | UTC timestamp |
| Taxi | `PULocationID` | `pickup_location_id` | Integer TLC zone key |
| Taxi | `DOLocationID` | `dropoff_location_id` | Integer TLC zone key |
| Taxi | derived | `trip_duration_seconds` | Dropoff minus pickup |
| Weather | year/month/day/hour | `weather_timestamp_source` | Source hour |
| Weather | derived | `weather_hour_utc` | Configurable source timezone converted to UTC |
| Air | `Date GMT` + `Time GMT` | `air_quality_hour_utc` | Authoritative UTC integration hour |
| Air | `Sample Measurement` | `measurement_value` | PM2.5 concentration |
| Air | `Units of Measure` | `measurement_unit` | Must equal supplied LC microgram unit |
| Zones | `LocationID` | `location_id` | Integer TLC zone key |

## Rejecting errors

- Missing primary-key components.
- Unparseable timestamps.
- Taxi pickup outside the supplied Jan-March 2024 source period.
- Taxi dropoff earlier than pickup.
- Negative trip distance, negative passenger count, NaN/Infinity numeric values, or invalid zone IDs.
- Weather values outside defined physical/code domains.
- Missing Air measurement, negative/NaN/Infinity measurement, or unexpected parameter/unit.
- Duplicate standardized business keys.

## Non-rejecting quality flags

- Taxi: zero distance/duration, duration over 24 hours, distance over 100, non-positive passenger
  count, negative fare or negative total amount.
- Weather: missing optional precipitation or weather condition code.
- Air Quality: any non-empty EPA qualifier.

These distinctions deliberately avoid deleting legitimate corrections/refunds or optional
environment observations. The final report may summarize counts from `metrics` rather than
recomputing them.

## Handoff to C

- Join Taxi Zones twice using pickup and dropoff location IDs.
- Verify the right-hand Zone key is unique before joining.
- Weather is already one row per UTC hour.
- Air aggregation accepts the verified PM2.5 LC unit and FEM method 636; other signatures require review.
- First take the median per site/hour, then the median across available sites per hour.
  Do not hard-code six sites; retain `air_quality_site_count` and flagged-observation counts.
- Match environment hours only for pickups in NYC's five boroughs; preserve other trips with null environment values.
- Weather is assumed to describe NYC in UTC; the source provides no location/timezone metadata.
- Report overall and NYC-scope hour match rates separately from non-null measurement counts.
- The integrated table retains Taxi fields, adds `weather_*` metrics and source flags, and
  `air_quality_pm25` in Micrograms/cubic meter (LC). Missing observations are not filled.
- Use left joins and verify the integrated row count equals accepted Taxi row count.
- After the verified write, `scripts.run_integration` publishes `metadata/completed_integration.json` with the batch `run_id`, the four standardized Delta versions and the integrated Delta version. Week 2 analytics register exactly those versions. A missing manifest is bootstrapped from `completed_batch.json` only when every `run_id` in the integrated table equals the batch `run_id`; otherwise integration must be rerun.
- Week 2 hourly calendars (Q3-Q5) span the requested local-date range clipped to the Taxi `valid_pickup_start_utc`/`valid_pickup_end_utc_exclusive` window; hours outside that window are never reported as zero demand.

## Transformation inventory

- All datasets: normalize names to snake_case (reject collisions); retain source types from `schemas.py` for CSV and the Parquet schema for Taxi. Add source file, run/schema/rule versions, quality flags and a record key. Required source columns are checked before transformation.
- Taxi: rename both pickup/dropoff timestamps to `*_timestamp_source`, and PU/DO IDs to `pickup_location_id`/`dropoff_location_id`. Convert both times from New York to UTC; derive UTC pickup hour, New York pickup date and duration in seconds. Other business values remain unchanged. The fingerprint formats numeric measurements to six decimal places; distinctions below that precision are not retained in the key.
- Weather: combine year/month/day/hour into a source timestamp, convert using configured timezone, then hash the UTC hour. Keep all ten measurements and their source labels unchanged; no unit conversion or missing-value filling is performed. Units are not independently verified from this file's metadata.
- Air: filter the configured state/counties before quality checks; rename sample measurement and units. Parse GMT and local-standard date/time pairs separately. Zero-pad state/county/site codes to 2/3/4 characters; cast parameter/method codes to strings. Trim qualifiers and turn empty qualifiers into null. Build site/monitor IDs and hash the documented compound key. Preserve remaining fields, including source date strings and last-change text.
- Zones: normalize `LocationID` to `location_id`; trim borough/zone/service-zone text. Set all three labels to `Unknown` for 264 and `Outside of NYC` for 265. Hash the location ID.
- Classification: apply the rejection/flag rules above, rank duplicates by validity then source file/raw JSON, retain one valid representative where available. Accepted output drops raw JSON and error reasons; rejected output retains both. Optional null measurements stay null.
- Integration: prefix weather fields, compute the two-level Air median and site/flag counts, add pickup/dropoff labels, scope and match flags. Preserve accepted Taxi rows and leave unavailable environmental values null, as specified above.

## Week 4 ML training handoff v1

Role B consumes one row per `(pickup_location_id, target_hour_utc)` for the zone-hour demand task.
The timestamp is a whole UTC hour, the key is non-null and unique, and `trip_count` is a finite,
non-negative label. Role A's integrated route and Role C's raw route must emit the same schema:

```text
pickup_location_id              integer, non-null
target_hour_utc                 timestamp, non-null
trip_count                      numeric, non-null
split                           string: train | validation | test
pickup_zone                     string, nullable
pickup_borough                  string, nullable
weather_coco_lag_1h             string, nullable
demand_lag_1h                   numeric, nullable
demand_lag_24h                  numeric, nullable
demand_rolling_mean_24h         numeric, nullable
weather_temp_lag_1h             numeric, nullable
weather_prcp_lag_1h             numeric, nullable
air_quality_pm25_lag_1h         numeric, nullable
```

Every valid zone in the fixed coverage window must have a row for every hour, including a label of
zero when the hour is inside the verified Taxi coverage and contains no pickups. Missing or
out-of-range source coverage is not a zero. Demand lags and rolling means are computed after that
calendar is completed and use only hours before `target_hour_utc`. Environmental features are
lagged by one hour under the documented offline-availability assumption; target-hour observations
are not features.

The Role A baseline uses the completed January-March snapshot and the UTC interval
`[2024-01-01 05:00:00, 2024-04-01 04:00:00)`. The standardized Taxi table must contain all three
monthly source files listed in `configs/ml.json`; the Weather, Air Quality, and Zone files and
integrated Taxi lineage must match that same pinned batch. Split boundaries are
`2024-03-05 00:00:00` and `2024-03-18 16:00:00` UTC. The published audit metadata records the
source paths, Delta versions, source filenames, coverage, split counts, and label checks. It is
also the handoff to Role B: feature and training runs load the training table at the
`output_version` it names and record that version with their outputs.

The three split values are chronological, non-overlapping blocks, and every zone for one target
hour belongs to the same block. Preprocessing estimators (median imputation, category indexers,
one-hot metadata and standard scaling) fit only on train. UTC identifies the hour; New York local
time is derived only for cyclic calendar features. `configs/ml.json` is the executable column and
feature contract, and [the Role B design note](w4_role_b_features_model.md) documents the handoff.
