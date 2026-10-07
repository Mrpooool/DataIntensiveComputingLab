# Week 4 evaluation report: machine learning on the platform

This report covers the full-data evaluation of 2026-10-02, run `20261002T155150Z` (route comparison and feature groups), and the training runs `main-20261003` and `retrain-w3-update`, all on the code of commit `ae244c1`. The evaluation ran before that commit's last change, the source-file naming for Spark-written directories, which it does not use. It measures how well the model predicts, how the raw-file and platform routes to the same training dataset compare, which datasets contribute useful features, and how reproducible and retrainable the pipeline is. The design is in the [design report](w4_design_report.md).

## Method

The task is zone-hour demand: at the start of each UTC hour, predict the number of Taxi pickups in each of the 262 NYC pickup zones. The January to March snapshot gives 262 zones × 2,183 hours = 571,946 rows, split chronologically into 401,122 train, 85,936 validation and 84,888 test rows.

Both routes end in the same zone-hour builder (`ml_dataset.build_training_dataset`) and the same Role B pipeline, and differ only in what comes before:

- Approach A, raw files (`ml_raw_route.py`), reads the three Taxi Parquet files, the Weather CSV, the 2.37 GB national Air Quality CSV and the zone lookup, and does its own cleaning and integration.
- Approach B, platform (`w4_evaluation.platform_training_dataset`), reads the pinned snapshot from Weeks 1 to 3 (run `563b32da-90fe-4cb5-997d-eef5f4a21d9b`, every standardized table and the integrated table at version 0) through the same `pinned_inputs` that the training-dataset CLI uses.

Each run first builds its route's training dataset and writes it to a fresh Delta directory; we call this the prepare time. It then runs `train_and_evaluate` on the written table. That call fits the feature pipeline once on train and applies it to all splits (feature fit), fits the two Linear Regression candidates (model fit), evaluates the one with the better validation RMSE once on test, and scores the baseline. The whole call is the training time; it does not save the model. Each route is warmed up once and then measured three times, alternating which route goes first. The tables report medians, and every sample is in [w4_evaluation_timings.csv](w4_evaluation_timings.csv). Spark's cache is cleared before each run; the operating-system file cache is not controlled.

Before any time is compared, the two warm-up datasets are compared row by row, with `exceptAll` in both directions. Every later run must then reproduce the first run's row count, order-independent content hash and test metrics, or the comparison reports a mismatch instead of times. Neither check failed: both routes produced the same 571,946 rows, and every run had the same hash and the same test metrics.

The feature-group experiment trains the same candidates on the published training dataset (`data/delta/ml/training_dataset`, version 0) with cumulative feature sets. The cyclic hour, day-of-week and month features are always included.

Environment: Windows 11, Python 3.11.9, Spark 4.2.0, Delta Lake 4.4.0, Java 21.0.7, `local[4]`, 4 GB driver, 128 shuffle partitions.

## Results

### Model evaluation

| Model (run `main-20261003`) | Validation RMSE | Test RMSE | Test MAE | Test R² |
| --- | ---: | ---: | ---: | ---: |
| Linear Regression, `regParam` 0.0 (selected) | 14.103 | 13.346 | 4.883 | 0.941 |
| Linear Regression, `regParam` 0.1 | 14.116 | | | |
| Baseline: same hour on the previous day | 23.346 | 20.483 | 5.519 | 0.862 |

The selected model lowers test RMSE by 35% and MAE by 12% compared with the baseline. RMSE weighs large errors more, so most of the improvement comes from removing the baseline's large misses. The saved model, loaded again, gave identical predictions for 20 fixed test rows. The run took 47.9 s including the save and the reload check. Test was evaluated once, after selection on validation.

### Route comparison

| Median seconds | Approach B: platform | Approach A: raw files |
| --- | ---: | ---: |
| Prepare: inputs to training Delta table | 8.6 | 43.0 |
| Feature fit (train) and transform (all splits) | 6.2 | 6.0 |
| Model fit, two candidates | 1.3 | 1.3 |
| Training total | 12.7 | 12.2 |
| End to end | 21.3 | 55.3 |
| One-time platform build (Week 3 measurement) | 312.7 | not needed |

The platform route has the training dataset ready five times sooner. The raw route has to re-parse all 8.1 million rows of the national Air file to keep 51,885 NYC rows, check 9.55 million Taxi rows against the rejection rules and collapse duplicates with a shuffle over all 19 Taxi columns, and it does all of this on every build. The platform route reads two columns of the integrated table and the small standardized Weather and Air tables.

Training takes the same time on both routes because both train on identical rows. All of the time the platform saves is in preparation.

For a single build the platform costs more. Ingesting and integrating the original files took 224.9 s plus 87.8 s in the Week 3 evaluation (run `20260926T180757Z-afffd333`, same machine, monitoring on), about seven times one raw build. That build standardizes and validates every column of every row, keeps rejected rows with their raw record, and writes three layers of tables with lineage, while this task needs two Taxi columns. Counting this dataset alone, the platform catches up after about nine builds (312.7 + 8.6n = 43.0n). The same snapshot also serves the Week 2 queries and products and absorbs the Week 3 incremental updates, so that cost is shared.

### Implementation and preprocessing complexity

| Code lines, without blanks, comments and docstrings | Approach B: platform | Approach A: raw files |
| --- | ---: | ---: |
| Preparation specific to this task | 8, plus 27 of snapshot checks | 97 |
| Shared builder (calendar, lags, environment join, split) | 65 | 65 |
| Platform modules reused from Weeks 1 to 3 | 1,392 | none |

For this one task, the raw route repeats the conversion of Taxi local time to UTC, nine Taxi rejection conditions including the zone-lookup check, duplicate collapse, typing of the Weather and Air CSV columns, the NYC state and county scope, the PM2.5 unit filter, and the site-then-city median. The 97 lines are short because they leave out what the platform keeps for every consumer: audit columns, rejected rows with reasons, per-run lineage, monitoring and schema checks. A second prediction task, such as fare or duration, would need another copy, and every rule change would have to be made in each copy.

The rules decide the labels. 36 rejected trips lie in NYC zones inside the coverage window: 35 with a dropoff before the pickup and one duplicate. Without the matching rules the raw route would count them, and its labels would differ from the platform's. We noticed this only because the evaluation compares rows with the platform; a stand-alone raw pipeline has nothing to check against. For Weather and Air the raw route checks only the values it uses. The platform rejected no Weather or Air row of the 2024 files, so the outputs agree, but if a file had, say, an out-of-range humidity, the platform would drop that hour and the raw route would keep it. The fixture test `tests/test_w4_evaluation.py` runs the platform's own ingestion and integration and the raw route on files that contain each Taxi rejection case, duplicate Taxi and Air rows, a non-NYC Air monitor and the daylight-saving change, and requires identical rows.

### Feature groups

| Features (cumulative) | Validation RMSE | Test RMSE | Test MAE | Test R² |
| --- | ---: | ---: | ---: | ---: |
| Taxi only: time, zone, borough, demand history | 14.095 | 13.349 | 4.838 | 0.941 |
| + Weather: temperature, precipitation, condition | 14.105 | 13.347 | 4.869 | 0.941 |
| + Air Quality: PM2.5 | 14.103 | 13.346 | 4.883 | 0.941 |

All of the gain over the baseline comes from Taxi and the zone lookup. Weather and PM2.5 change test RMSE only in the third decimal, and they make validation RMSE slightly worse and test MAE worse, so selection on validation would keep the Taxi-only model. The environment values are one NYC-wide background value per hour, the same for all 262 zones, and a linear model adds them as one shift to every zone. Zone-hour demand varies mostly with the zone's level and its last few hours. An effect such as rain moving demand from one zone to another would need zone × weather interactions or a non-linear model.

### Reproducibility and retraining

The training dataset metadata of the platform route names the snapshot run, the Delta version of each of the five source tables, the original file names, the coverage window and the split boundaries. Training loads the dataset at the version that the metadata records, and writes the path, version and source run ID into `metrics.json`, next to the model and a copy of the configuration, in a directory per run ID. The raw route was just as deterministic here and gave the same hash in every run, but only because the files did not change. Nothing records which version of a file it read or which rows it dropped and why, and the files can be replaced in place.

Retraining needed no code change. On the Week 3 updated snapshot (run `d8bba723-4033-4c0e-8e72-5f9ff747e268`; Taxi version 1, Weather and Air version 3, integrated version 1; coverage extended to 2024-07-01 04:00 UTC), one new configuration, [ml_w3_update.json](../configs/ml_w3_update.json), and the same two commands built 1,144,154 rows (262 × 4,367 hours, 10,183,235 trips) and trained run `retrain-w3-update`. It selected `regParam` 0.1 and reached test RMSE 2.00 against 2.18 for the baseline, but MAE 1.36 against 0.68. These numbers say nothing about prediction quality. The Week 3 update is a 7% sample of January to March trips moved forward 13 weeks, so demand from April to June is about 7% of the real level, and a model fitted mostly on full-volume weeks over-predicts it. The run demonstrates the mechanism only: a new snapshot, a new configuration and a new run directory.

## Discussion

### Which engineering decisions from Weeks 1 to 3 simplified machine learning?

The version-pinned manifests helped most. The training dataset names its exact inputs, and the coverage window that the snapshot publishes defines where an hour without trips is a real zero. Because the platform stores UTC and converts at the edges, the Taxi local times were already resolved; the training key is a UTC hour, and the New York features are derived from it without daylight-saving ambiguity. The rejected-row tables with reason codes let us find the 36 trips that decide label parity. The NYC scope and the two-level PM2.5 median had been decided and tested in Week 1, and the configuration-driven contract in `datasets.json` set the pattern for `ml.json`.

### Which datasets contributed the most useful features?

Taxi supplies the label and the demand history, and the zone lookup supplies zone and borough. Together they account for all of the improvement over the baseline. Weather and Air Quality add nothing measurable to a linear model at this grain.

### Which parts of the workflow became reusable?

The snapshot and its loaders, `aggregate_air_quality`, `load_calendar_coverage`, the zone-hour builder used by both routes, the train-only feature pipeline and `train_and_evaluate`. The last of these runs any configuration; the feature groups and the retraining run are just different configurations. The timing harness follows the Week 3 evaluation.

### How did the platform reduce preprocessing before feature engineering?

Approach B's preparation is eight lines that select pinned tables. Approach A's is 97 lines of parsing, time conversion, rules, scoping and aggregation that someone has to keep in step with the platform by hand. Per build, the platform route prepares in 8.6 s instead of 43.0 s.

### How would the ML pipeline change if additional datasets were introduced?

A new source would enter the platform first, with a contract in `datasets.json`, a transform, rules, and either an integration join or a standardized hourly table. The builder would then join it at the target hour with an explicit availability lag, and its columns would go into `ml.json` under a dataset prefix. The imputer, missing indicators, encoders and scaler pick such columns up without code changes, and the feature-group experiment measures the new source's contribution by its prefix. The raw route would need its own copy of the new cleaning.

### What improvements to the platform would most help future machine-learning applications?

Hourly Weather and PM2.5 should be published as data products with their own coverage, so that consumers stop rebuilding them from standardized rows. The platform should record when environmental observations become available; the dataset currently assumes one hour. The zone-hour demand history should be maintained incrementally as Week 3 updates arrive, instead of recomputing every lag. Zone-level environmental data would let a model explain differences between zones, which city-wide values cannot. Finally, the three ML stages should write rows to `pipeline_runs`, which they do not do yet.

## Limitations

All figures come from one machine, `local[4]`, one data size and three measured runs per route. The operating-system file cache is not controlled, and the warm-up absorbs cold reads. The one-time platform cost comes from the Week 3 evaluation, measured a week earlier on the same machine with monitoring on. We trained only Linear Regression, so the feature-group result shows that the environment adds nothing to a linear model, not that it cannot help any model. The one-hour availability of environmental data is an assumption. The retraining run uses synthetic update data and shows the mechanism only.

## Reproduction and evidence

```powershell
.\.venv\Scripts\python.exe -m scripts.run_ml_dataset
.\.venv\Scripts\python.exe -m scripts.run_ml_training --run-id main-20261003
.\.venv\Scripts\python.exe -m scripts.run_w4_evaluation          # both experiments
.\.venv\Scripts\python.exe -m scripts.run_ml_dataset --delta-root data/benchmark/w3/operated `
  --config configs/ml_w3_update.json --output-path data/delta/ml/training_dataset_w3_update `
  --metadata-path data/delta/ml/training_dataset_w3_update_metadata.json
.\.venv\Scripts\python.exe -m scripts.run_ml_training --config configs/ml_w3_update.json `
  --training-metadata data/delta/ml/training_dataset_w3_update_metadata.json --run-id retrain-w3-update
```

Results are in `data/benchmark/w4/20261002T155150Z/results.json` and `artifacts/w4/training/<run>/metrics.json`; the timing samples are copied to [w4_evaluation_timings.csv](w4_evaluation_timings.csv).
