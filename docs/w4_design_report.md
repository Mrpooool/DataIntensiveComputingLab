# Week 4: A machine learning pipeline on the urban data platform

In Weeks 1 to 3 we built a platform that ingests four 2024 NYC datasets into validated Delta tables, enriches every accepted Taxi trip with zone labels, hourly weather and PM2.5, and publishes each stage's output as a manifest of exact Delta versions. Week 4 uses that snapshot to predict demand with Spark MLlib. The model is simple on purpose. The work went into the training dataset, which is generated from the platform automatically and reproducibly; the feature pipeline, which learns only from training data; and the training run, which can be repeated, or retrained on new data, with the same commands. The measurements are in the [evaluation report](w4_evaluation_report.md); the column contract is in [data_contract.md](data_contract.md) and [configs/ml.json](../configs/ml.json).

## 1. Prediction problem

At the start of each UTC hour *h*, predict the number of Taxi pickups in each NYC pickup zone during *h*. A row is one `(pickup_location_id, target_hour_utc)`, and the label `trip_count` is the number of accepted trips that start in that zone in that hour.

We chose zone-hour demand over trip duration or fare because it uses all four datasets: Taxi gives the label and the demand history, the zone lookup gives the scope, zone and borough, and Weather and Air Quality give the hourly environment. It also reuses decisions we had already tested in Week 2, where queries Q3 to Q5 build the same hourly calendar and treat an hour without trips as zero demand only inside the validated Taxi window. And it answers a planning question: how many taxis each zone will need in the next hour.

## 2. Training dataset design (Task 1)

`scripts.run_ml_dataset` builds the dataset with Spark from the published snapshot (`ml_dataset.materialize_training_dataset`) and writes it as a Delta table with an audit file beside it.

### Inputs

The builder reads the integration snapshot and the completed batch, requires both to name the same run and table versions, and loads every table with `versionAsOf`; it never reads the latest table. The coverage window comes from the snapshot when an incremental update has extended it, and otherwise from the Taxi validity window in `datasets.json`. The window must equal the one configured in `ml.json`, and the source file names in the tables' lineage must equal the configured ones: the three January to March Taxi files and the Weather, Air Quality and zone files. A snapshot that contains the Week 3 synthetic update cannot enter the main experiment by accident.

### Target

The builder crosses the 262 zones of the five boroughs with all 2,183 hours of the window `[2024-01-01 05:00, 2024-04-01 04:00)` UTC and joins the pickup counts from the integrated table onto that grid. A zone-hour without trips gets the label 0; 335,531 of the 571,946 rows (59%) are zeros. Hours outside the window get no row, because there a missing trip means missing data, not zero demand. Trips the platform rejected are not counted, and the 37,569 accepted trips in EWR, `Unknown` or `Outside of NYC` fall outside the task. The builder checks that the labels add up to the 9,517,007 NYC trips in the snapshot and that every zone has every hour.

### Features

Only values known at the start of *h* are allowed:

| Feature | Source | Why |
| --- | --- | --- |
| `demand_lag_1h`, `demand_lag_24h` | Taxi | Demand persists hour to hour and repeats daily; the 24-hour lag is also the baseline |
| `demand_rolling_mean_24h` | Taxi | The zone's recent level, less noisy than a single hour |
| `pickup_zone`, `pickup_borough` | Zone lookup | Zones differ in level by orders of magnitude |
| `weather_temp_lag_1h`, `weather_prcp_lag_1h`, `weather_coco_lag_1h` | Weather | Cold and rain plausibly change taxi use |
| `air_quality_pm25_lag_1h` | Air Quality | Tests whether air quality relates to demand |

The demand features are computed after the grid is complete, over the zone's own preceding rows, so a zero hour counts as zero instead of being skipped, and every window ends at *h*−1. Hour of day, day of week and month are derived later, in the feature pipeline.

### Assumptions

We assume that Weather and PM2.5 for hour *h*−1 are available at the start of *h*. The real publication delays are unknown, and the target hour's own observations are never used. The environment is one city-wide value per hour, the same for every zone; PM2.5 is the median over sites of each site's hourly median, as in Week 1. Missing measurements stay null: precipitation is missing for 7.6% of rows, the other environment values for under 0.1%. The demand lags are null only at the start of the series. Keys are UTC hours, so the repeated or skipped local hour at a daylight-saving change cannot create duplicate keys.

### Splits

The `split` column divides whole hours chronologically: train before 2024-03-05 00:00 (1,531 hours, 70.1%), validation before 2024-03-18 16:00 (328 hours, 15.0%), and test until the end (324 hours, 14.8%). All zones of one hour share a split. Validation and test simulate predictions made hour by hour: a row's lags may use the true counts of earlier hours, including hours of an earlier split, but never its own hour or a later one. The boundaries are fixed in `ml.json`, so they do not move between runs.

### Handoff

The builder writes `data/delta/ml/training_dataset`, reads it back, and writes `training_dataset_metadata.json` with the source run, the source Delta versions and paths, the source file names, the coverage window, the split boundaries and counts, the label distribution, the missing-value rates and the output Delta version. Training starts from that file, just as each platform stage starts from the previous stage's manifest.

## 3. Feature engineering strategy (Task 2)

`ml_pipeline.build_feature_pipeline` returns a standard Spark `Pipeline` built from `ml.json`:

1. A `SQLTransformer` converts `target_hour_utc` to New York time and derives sine and cosine encodings of hour, day of week and month, so that 23:00 lies close to 00:00. It adds a 0/1 indicator for each missing environment value and maps missing categories to an explicit `__MISSING__` value.
2. An `Imputer` fills missing numeric values with the training median.
3. A `StringIndexer` per categorical feature, in alphabetical order, feeds a `OneHotEncoder`. With `handleInvalid="keep"`, a zone or weather code that first appears after training gets its own bucket instead of failing the run.
4. A `VectorAssembler` collects the numeric values, indicators and time encodings, a `StandardScaler` scales them to unit variance with training statistics, and a final assembler appends the one-hot vectors into `features`, 299 values per row.

Every learned stage is fitted on the train split only and then applied to validation and test, so no statistic from a later period reaches the model. The stored feature output keeps only the key, label, split and `features`; lineage columns, run identifiers and trip-level values never enter it.

Demand history needed the most preprocessing: completing a grid of 571,946 rows from about 236,000 observed zone-hours, then three window functions per zone. It could not live inside the Spark ML pipeline, because a fitted stage cannot see the earlier rows of other splits without either leaking them or losing them. The environment features needed hourly uniqueness, the two-level PM2.5 median, the one-hour shift and the missing-value indicators. The categorical features needed only indexing and encoding, with one caveat: zone names are not unique in the lookup (Corona for 56 and 57; Governor's Island/Ellis Island/Liberty Island for 103 to 105), so those zones share a category. They are small zones, and the zone ID would be the exact key.

To add a feature that both preparation routes produce with the same meaning, put its column into the categorical or numeric list in `ml.json`, and into the missing-indicator list if its absence carries information. Imputation, indicators, encoding, scaling and assembly then include it without code changes. Features are named after their source dataset (`weather_*`, `air_quality_*`), and the Task 4 feature-group experiment uses that prefix to measure what each dataset contributes.

## 4. Machine learning pipeline (Task 3)

`scripts.run_ml_training` runs `train_and_evaluate` in six steps:

1. It reads the dataset metadata and loads the training table at the recorded Delta version, not the latest one.
2. `validate_training_dataset` rejects missing columns, wrong types, null or duplicate keys, timestamps that are not whole hours, negative or non-finite labels, overlapping splits, and numeric features with no training value to impute from. It never casts silently.
3. The feature pipeline is fitted once on train and applied to all three splits, which are cached. Preprocessing is deterministic, so refitting it for every candidate would produce the same stages.
4. One Linear Regression per configured `regParam`/`elasticNetParam` pair, currently `regParam` 0 and 0.1, is fitted on train and scored on validation, and the lowest validation RMSE wins. Linear Regression is fast (about 1.3 s for both candidates), deterministic and easy to explain, which suits an assignment about the workflow rather than accuracy.
5. The selected model is scored once on test with RMSE, MAE and R², next to the previous day's same hour as a baseline. We do not report MAPE, because zero-demand rows make it undefined.
6. The fitted feature stages and the regression are saved as one `PipelineModel`, which predicts from the raw training-dataset columns. The run loads the model again and requires identical predictions for 20 fixed test rows. `metrics.json` records every candidate, the metrics, the baseline, the split counts, the training data's path, version and source run, the timings, and the Python, Spark, Delta and Java versions; `config_snapshot.json` freezes the configuration. Every run writes to its own `artifacts/w4/training/<run-id>` directory.

### Reusable parts

The snapshot loader, the zone-hour builder (used by both Task 4 routes), the feature pipeline and `train_and_evaluate` are all driven by `ml.json`, and the metadata file connects them.

### Retraining

When new data arrives, the platform's incremental update publishes a new snapshot. A copy of `ml.json` with the new coverage window, split boundaries and source files builds a new training dataset, and the same training command trains on it under a new run ID, leaving earlier runs untouched. We did this on the Week 3 updated snapshot with [ml_w3_update.json](../configs/ml_w3_update.json) and changed no code. That snapshot holds synthetic data, so the run demonstrates the mechanism and says nothing about prediction quality.

### Multiple prediction tasks

Each task gets its own configuration file and its own artifact directories, so labels, metrics and models cannot mix. The feature pipeline and `train_and_evaluate` read everything from the configuration. A trip-level task such as fare or duration would need its own builder at trip grain, which could read the integrated table directly, and adjusted label checks, since the current ones require non-negative counts. The model type is fixed to Linear Regression; supporting others means adding an estimator choice to the configuration.

## 5. Comparing the two development approaches (Task 4)

Approach A (`ml_raw_route.py`) builds the same dataset from the original files. It converts Taxi local time to UTC, applies the platform's Taxi rejection rules and duplicate collapse, keeps the NYC Air monitors and takes the two-level PM2.5 median. It then calls the same builder and pipeline as Approach B, which reads the pinned snapshot. Because the two routes share everything after preparation, any difference in time or result comes from preparation. `scripts.run_w4_evaluation` requires both routes to produce identical rows before it reports any time, and a fixture test runs the platform's real ingestion and integration against the raw route on files that contain every rejection case. The routes produced identical rows; preparation took 8.6 s instead of 43.0 s, training took the same time, and the preparation code is 8 lines instead of 97. The [evaluation report](w4_evaluation_report.md) has the details and the discussion.

## 6. Engineering decisions and trade-offs

| Decision | Benefit | Cost |
| --- | --- | --- |
| Zone-hour grid with explicit zeros | Uses all four datasets; matches the Week 2 calendars; zeros are real labels | 59% of rows are zero; needs a trusted coverage window |
| Lags built in the dataset, not in the ML `Pipeline` | Windows can use earlier splits' true history without leakage | Features are made in two places; both routes must share the builder |
| Environment shifted by one hour | No target-hour information leaks | Availability is assumed, not measured |
| City-wide environment values | Matches the data the platform has | Cannot explain differences between zones; adds nothing to a linear model (Task 4) |
| Chronological splits by whole hour | Tests the future, not interpolation | Test covers the second half of March only |
| Linear Regression with a small grid | Fast, deterministic, explainable | Misses interactions and non-linear effects |
| Preprocessing fitted once, on train only | Less work; feature and model fit timed separately | Every candidate shares one preprocessing choice |
| Version pinning from snapshot to model | Every model names its exact data | More metadata to keep consistent |
| Strict input validation | Bad inputs fail early with a clear message | New tasks must adjust the checks explicitly |
| Raw route mirrors the platform's Taxi rules | Rows identical to the platform, so the comparison is fair | The rules exist twice; the raw route must follow platform changes by hand |

Two gaps remain. The three ML commands do not write `pipeline_runs` rows yet, so the Week 3 monitoring queries cannot see them. And the training dataset is rebuilt in full for every snapshot; with frequent updates, the demand history should be maintained incrementally, as the Week 3 products are.
