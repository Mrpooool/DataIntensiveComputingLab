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
- Duplicate detection is deterministic and rejects every occurrence after the selected first row.
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
- Negative trip distance, negative passenger count, NaN numeric values, or invalid zone IDs.
- Weather values outside defined physical/code domains.
- Missing Air measurement, negative/NaN measurement, or unexpected parameter/unit.
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
- Air remains one row per monitor-hour. C should first compute a robust city background value
  (the agreed design uses the median of the six available site-hour measurements) so the right
  side has at most one row per `air_quality_hour_utc`.
- Use left joins and verify the integrated row count equals accepted Taxi row count.
