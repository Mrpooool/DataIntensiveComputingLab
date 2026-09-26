# Week 3 role B: validation, schema evolution and analytical consistency

This note records the validation decisions implemented for Assignment Tasks 2 and 4. It is the handoff from role B to the incremental pipeline (role A) and the monitoring/evaluation work (role C).

## 1. Validation flow

Full ingestion and incremental updates use the same `prepare` entry point. Validation is applied after standardisation and before the accepted/rejected split:

1. standardise names, timestamps and units;
2. evaluate generic and dataset-specific rules;
3. append stable error codes to `error_reasons` and non-fatal codes to `quality_flags`;
4. mark duplicate rows within the input batch;
5. write valid rows to `standardized/<dataset>` and invalid rows to `rejected/<dataset>`;
6. report per-code counts to `metadata/pipeline_runs`.

An invalid row therefore does not fail the whole batch and cannot enter the integrated table or analytical products. Unsupported schema evolution is different: it invalidates the update contract itself, so the update is stopped and recorded as failed instead of guessing how to transform the file.

`prepare(..., validate=False)`, `ingest_dataset(..., validate=False)` and `apply_updates(..., validate=False)` are provided only for the isolated validation-overhead experiment. They preserve the empty `error_reasons`/`quality_flags` columns and duplicate collapse, so the rest of the pipeline has the same shape. The published pipeline must keep validation enabled.

## 2. Rule catalogue

The existing Week 1 rules remain unchanged. Week 3 adds the following checks.

| Condition | Error code | Scope | Behaviour |
| --- | --- | --- | --- |
| Taxi pickup or drop-off ID is absent from the current Taxi Zone lookup | `missing_reference_record` | Taxi | isolate and report the trip |
| Evolved `humidity` is null | `incomplete_record` | Weather updates containing the column | isolate and report the observation |
| `humidity` is non-finite or outside 0–100 | `invalid_attribute_value` | Weather updates containing the column | isolate and report the observation |
| Evolved `aqi` is null | `incomplete_record` | Air Quality updates containing the column | isolate and report the observation |
| `aqi` is non-finite or outside 0–500 | `invalid_attribute_value` | Air Quality updates containing the column | isolate and report the observation |
| Same business key appears more than once in one input | `duplicate_record` | all datasets | deterministic winner accepted; other rows isolated |

Reference data is supplied explicitly to the validator. Full Taxi ingestion reads the raw zone lookup; incremental Taxi validation reads the published standardized lookup. This keeps the rule deterministic for the snapshot being updated.

### Extending the rule set

Dataset-specific and cross-dataset rules can be registered without editing `validate_dataset`:

```python
from dic_pipeline.validation import register_rule_builder, unregister_rule_builder

def extra_weather_rules(df, config, references):
    return [("custom_error", condition)], [("custom_quality_flag", warning)]

register_rule_builder("weather", extra_weather_rules)
# run validation
unregister_rule_builder("weather", extra_weather_rules)
```

Use the dataset name for a specialised rule or `"*"` for a generic rule. A builder returns `(error_rules, quality_flag_rules)`, where each rule is `(stable_code, Spark_Column_condition)`. Stable codes are important because monitoring aggregates them across runs.

Generic rules belong in the core when their meaning is independent of the source, for example duplicate handling and required business-key components. Bounds, units, reference sets and source-specific completeness belong to dataset builders. This keeps shared mechanics reusable without hiding domain assumptions in a generic framework.

## 3. Schema-evolution policy

The source contract is compared with the observed schema before an update is prepared. `check_schema` checks column presence, duplicate names, data types and nullability. Additions are opt-in under `schema_evolution.allow_add` in `configs/datasets.json`.

Automatically accepted changes are deliberately narrow:

| Dataset | Column | Type | Source nullable | Row-level requirement |
| --- | --- | --- | --- | --- |
| Weather | `humidity` | `double` | yes | must be present and in 0–100 for each new row |
| Air Quality | `aqi` | `double` | yes | must be present and in 0–500 for each new row |

The columns are nullable in the Delta schema because historical rows predate them. New update rows are nevertheless required by validation. On acceptance, the dataset `schema_version` and `rule_version` become `1.1.0`, and the accepted change is stored in `schema_changes_json`.

The following changes require manual review and migration: removing or renaming a column, adding an unlisted column, changing a type, relaxing a required non-null field, changing a key's meaning, or supplying a duplicate column name. They raise `SchemaValidationError`; no target table or product is partly updated.

The manifest's claimed changes are not trusted as validation evidence. The update reader derives the actual CSV header or Parquet schema, checks it against the configured contract, and only then parses or prepares the data.

## 4. Analytical consistency

The evolved columns are intentionally not added to `integrated_taxi_trips` or the Week 2 products in Week 3. Existing Q1–Q6 schemas and meanings therefore remain backward compatible. `humidity` and `aqi` are retained in their standardized source tables for future products.

The refresh boundary is based on data dependencies rather than on the presence of any update:

| Product | Incremental boundary | When a full recomputation is required |
| --- | --- | --- |
| `daily_mobility_summary` | affected local dates/hours | key, timezone or metric-definition change |
| `taxi_zone_statistics` | affected month × zone groups | zone mapping, grouping key or metric-definition change |
| `weather_impact_summary` | full rebuild whenever the integrated table changes | weather category mapping or join semantics change |
| `air_quality_impact_summary` | affected local hours | PM2.5/AQI category mapping or join semantics change |

Adding `humidity` or `aqi` alone does not change any existing product, so no product should be refreshed solely because one of those columns appeared. New rows still affect products through their existing fields and dirty keys. Any future product that uses the evolved attributes must declare its dependency, output schema version and recomputation boundary before publication.

Role A owns the physical MERGE/rebuild implementation; this policy defines when that implementation is valid. Role C can verify the decision by comparing automatic refresh output with a full rebuild on an isolated copy.

## 5. Monitoring and verification

Every row-level run reports whether validation was enabled, total rejected rows, `validation_failure_counts_json`, schema/rule versions and accepted `schema_changes_json`. Operators can aggregate failures with:

```bash
python -m scripts.run_monitoring_report --query validation_failures_by_target
python -m scripts.run_monitoring_report --query failure_codes_by_target
```

Targeted tests cover reference failures, incomplete/invalid evolved values, extension builders, schema rejection, the validation-off shape, and the full/ incremental regression paths. The evaluation runner owns final timing; it should compare validation on/off only on fresh copies with equivalent valid output.
