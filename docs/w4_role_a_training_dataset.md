# Week 4 Role A: zone-hour training dataset

## Prediction contract

At the start of UTC hour `h`, predict the number of accepted Taxi pickups in each NYC
pickup zone during `h`. The label is `trip_count`; a row is identified by
`(pickup_location_id, target_hour_utc)`. The five NYC boroughs in the pinned zone
lookup define the scope. EWR, `Unknown`, and `Outside of NYC` are excluded because
they are not NYC pickup zones. The output columns and types match
[`data_contract.md`](data_contract.md) and [`configs/ml.json`](../configs/ml.json).

The main experiment uses the original 2024 January-March coverage
`[2024-01-01 05:00:00, 2024-04-01 04:00:00)` UTC. Both bounds and the split cutoffs
are fixed in `configs/ml.json`. The builder requires the published integration
snapshot and completed batch to have the same run and table versions. It reads
those Delta versions, not the latest unregistered tables. It also checks the
snapshot's Taxi coverage (the window an incremental update published, otherwise the
configured validity window in `configs/datasets.json`) and the expected Jan/Feb/Mar Taxi, Weather, Air, and Zone
source filenames; a Week 3 simulated update therefore cannot silently
enter the main experiment. The original source file identifiers and all
source Delta paths/versions are recorded in `training_dataset_metadata.json`.

## Construction and availability

The builder crosses every whole UTC hour in the verified Taxi window with every
NYC zone, then left-joins integrated pickup counts and fills absent counts with
zero. It never creates zero labels outside the verified window. Accepted trips in
excluded zones do not enter this NYC prediction task. Complete zone-hour rows are
required before demand history is calculated: `demand_lag_1h`,
`demand_lag_24h`, and the mean of up to 24 preceding completed hours.
`demand_rolling_mean_24h` is null for the first hour, uses the available
preceding hours for hours 1-23, and uses all 24 preceding hours thereafter.
`demand_lag_1h` is null for the first hour; `demand_lag_24h` is null for the
first 24 hours. B's train-only imputer handles these null values.

Weather and air-quality features are joined from the pinned standardized tables,
not inferred from trips. This preserves prior-hour observations even when a zone
has zero trips. The builder takes one PM2.5 value per hour; the platform route
passes the existing two-stage site/hour median (`aggregate_air_quality`). Every
environment column uses hour `h-1`; missing measurements remain null. These are
offline observation-time features: actual publication delays are unavailable, so
the experiment assumes the previous hour's data is available at the start of
`h`. No target-hour measurement or label is used as a feature. UTC is the key;
B derives New York local time only for cyclic features, preserving the DST
transition without duplicate hour keys.

All zones in a UTC hour share one chronological split. Configured boundaries
yield 1,531 training hours (70.13%), 328 validation hours (15.03%), and 324 test
hours (14.84%). Split times are in the metadata, alongside split row counts,
zero-label counts, label min/max/mean, missing-value counts, and the filtering
rule. The sum of labels must equal the number of integrated NYC trips; otherwise
the builder rejects the snapshot. The Delta table is read back and checked after
publication.

## Run and handoff

Build or select a verified original January-March four-table batch and integrated
snapshot first. A completed snapshot with Week 3 synthetic rows is rejected.
Then run:

```powershell
.\.venv\Scripts\python.exe -m scripts.run_ml_dataset --delta-root data/delta
.\.venv\Scripts\python.exe -m scripts.run_ml_features
.\.venv\Scripts\python.exe -m scripts.run_ml_training
```

The dataset defaults to `data/delta/ml/training_dataset` and its audit JSON to
`data/delta/ml/training_dataset_metadata.json`. That JSON is the handoff: the
feature and training CLIs load the `output_version` it names, not the latest
table version. Use `--output-path` and
`--metadata-path` for an isolated experiment; `--overwrite` explicitly replaces
an existing output. To demonstrate retraining on a later verified snapshot,
copy the ML configuration with that snapshot's coverage and new chronological
split boundaries, use distinct output and metadata paths, and pass that metadata
to B's training CLI with `--training-metadata`.
Do not mix the synthetic update with the main prediction-quality result.

Role C's raw-data route (`ml_raw_route.py`) reuses `build_training_dataset` after
preparing the same inputs from the original files itself, and the Task 4
evaluation requires both routes to produce identical rows before it compares
times.
