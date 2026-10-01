# Week 4 Role B: reusable features and model lifecycle

Role B implements Task 2 and the model-facing part of Task 3 for one prediction task: at the
start of hour `h`, predict the number of Taxi pickups in every NYC pickup zone during `h`. The
training grain is therefore one row per `(pickup_location_id, target_hour_utc)`, and the label is
`trip_count`.

## Input handoff

`src/dic_pipeline/ml_pipeline.py` deliberately starts after dataset construction. Role A (the
integrated route) and Role C (the raw route) must both produce the same columns and meanings:

| Column | Type | Meaning at prediction time |
| --- | --- | --- |
| `pickup_location_id` | integer | TLC pickup-zone identifier |
| `target_hour_utc` | timestamp | whole UTC hour being predicted |
| `trip_count` | numeric | non-negative target count, including explicit zero-demand rows |
| `split` | string | `train`, `validation`, or `test`; disjoint chronological blocks |
| `pickup_zone`, `pickup_borough` | string | zone labels known before the target hour |
| `demand_lag_1h` | numeric | completed pickup count for `h-1` |
| `demand_lag_24h` | numeric | completed pickup count for `h-24`; also the naive baseline |
| `demand_rolling_mean_24h` | numeric | mean of completed hours ending at `h-1` |
| `weather_temp_lag_1h`, `weather_prcp_lag_1h` | numeric | last-hour weather observations |
| `weather_coco_lag_1h` | string | last-hour weather-condition code |
| `air_quality_pm25_lag_1h` | numeric | last-hour PM2.5 observation |

The exact machine-readable contract is `configs/ml.json`. Validation fails on missing columns,
wrong timestamp/numeric types, null or duplicate keys, non-hour timestamps, non-finite values,
negative labels, missing split blocks, or time overlap between splits. It does not silently cast an
incompatible training dataset.

The lag fields are part of the handoff rather than recomputed by Role B. They must be calculated
only after the zone-hour calendar has been completed, and every window must end before the target
hour. Validation and test simulate a prediction made at the start of each hour: the completed
history may be used, but no observation from the target or a later hour may be used. Environmental
lags make the offline availability assumption explicit; target-hour post-event weather is excluded.

## Feature pipeline

`build_feature_pipeline()` returns a normal Spark `Pipeline`. `fit_feature_pipeline()` first checks
the contract, filters `split == train`, and fits every learned preprocessing stage on that subset
only. Validation and test are transformed by the resulting `PipelineModel`.

The stages are:

1. Convert `target_hour_utc` to New York local time and derive cyclic hour, day-of-week and month
   features. UTC remains the key, so the repeated local hour around daylight-saving transitions is
   not ambiguous.
2. Add explicit missing indicators for temperature, precipitation and PM2.5.
3. Median-impute numeric features from training values only.
4. Map null categorical values to an explicit `__MISSING__` token, then alphabetically index and
   one-hot encode zone, borough and weather-condition categories. `handleInvalid="keep"` gives
   unseen validation, test and future categories a stable extra bucket.
5. Standardize the numeric/indicator/time vector using training statistics, then assemble it with
   the categorical vectors into `features`.

The compact feature dataset keeps only the key, label, split and `features`, which removes source
lineage, run metadata and any per-trip fields that would be unknown before prediction. Both the
pipeline model and the Delta feature table can be generated with `scripts.run_ml_features`.

## Training, selection and evaluation

`train_and_evaluate()` builds the full preprocessing plus Linear Regression pipeline for each
configured `(regParam, elasticNetParam)` candidate. Each candidate is fitted on train and ranked by
validation RMSE. Test is evaluated exactly once using the selected candidate. The report contains
validation and test RMSE, MAE and R-squared together with the `demand_lag_24h` baseline. MAPE is not
used because valid zero-demand zone-hours make it undefined or misleading.

The selected full `PipelineModel` is saved, loaded again, and asked to predict a deterministic
ordered sample. The run fails if the reloaded predictions differ outside a strict numerical
tolerance. `metrics.json` also records candidate parameters, split counts, timings, the input path
and the local Python/Spark/Delta/Java environment; `config_snapshot.json` freezes the run settings.
MLlib Linear Regression has no stochastic sampling or seed parameter, so reproducibility rests on
the fixed snapshot, chronological splits, configuration and deterministic estimator rather than a
decorative unused seed.

Retraining is the same operation, not a separate code path: build a new compatible training
snapshot, then run `scripts.run_ml_training` with a new `--run-id`. Every run has its own model,
configuration and metrics directory under the ignored `artifacts/` root. A W3 simulated update must
be labelled as a retraining-mechanism demonstration rather than mixed into the main prediction
quality result.

## Extension and data contribution

A new prediction-time feature can be added to the appropriate categorical or numeric list in the
configuration after the dataset builders for both routes expose it with the same meaning. Add a
missing indicator when absence is informative. The existing imputer, encoder, scaler and assembler
then include it without a parallel pipeline. Its contribution should be measured with controlled
feature groups—time/location only, then demand history, then environment—using identical rows,
splits and model settings; that comparison belongs to Role C's evaluation rather than an assertion
inside the transformer.

Multiple targets should use separate versioned configurations and artifact directories so labels,
metrics and models cannot be mixed. The current validation deliberately encodes non-negative demand
counts and therefore should be adapted explicitly, not silently reused, for fare or duration. A new
source dataset is useful only after its event time, availability time, grain and join rule have been
documented and both preparation routes can reproduce the same training column. This keeps new data
from bypassing the validation and leakage decisions established in Weeks 1–3.

## Commands and ownership boundary

```powershell
# Optional standalone materialization of Task 2 output.
.\.venv\Scripts\python.exe -m scripts.run_ml_features `
  --training-data data/delta/ml/training_dataset

# Task 3; rerun with another input snapshot/run ID to retrain.
.\.venv\Scripts\python.exe -m scripts.run_ml_training `
  --training-data data/delta/ml/training_dataset
```

Role A still owns construction, snapshot/version metadata, zero-hour completion, chronological split
boundaries and the integrated-route CLI. Role C still owns the raw route and the fair route
comparison. Both routes should call this module after proving equality of key, label and feature
inputs; duplicating the downstream preprocessing or model code would weaken the comparison.

The focused tests in `tests/test_ml_pipeline.py` cover the declared input fields, duplicate-key
rejection, train-only imputation, missing indicators, unseen categories, feature-vector production,
metrics, model persistence and prediction after reload.
