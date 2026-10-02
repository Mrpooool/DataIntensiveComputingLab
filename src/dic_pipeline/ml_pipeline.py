"""Reusable Week 4 feature engineering and Spark ML training for zone-hour demand."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Any
from zoneinfo import ZoneInfo

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import (
    Imputer,
    OneHotEncoder,
    SQLTransformer,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.regression import LinearRegression
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import IntegralType, NumericType, StringType, TimestampType

from .ingestion import PROJECT_ROOT


DEFAULT_ML_CONFIG = PROJECT_ROOT / "configs" / "ml.json"
FEATURES_COLUMN = "features"
PREDICTION_COLUMN = "prediction"
_SPLIT_ORDER = ("train", "validation", "test")
_SUPPORTED_METRICS = frozenset({"rmse", "mae", "r2"})


@dataclass(frozen=True)
class TrainingResult:
    """Fitted model and serializable evidence from one reproducible training run."""

    model: PipelineModel
    report: dict[str, Any]


def load_ml_config(path: str | Path = DEFAULT_ML_CONFIG) -> dict[str, Any]:
    """Load and validate the small version-controlled ML contract."""
    config = json.loads(Path(path).read_text(encoding="utf-8"))
    required_scalars = (
        "schema_version",
        "prediction_task",
        "analysis_timezone",
        "timestamp_column",
        "label_column",
        "split_column",
        "imputer_strategy",
        "baseline_prediction_column",
    )
    for name in required_scalars:
        if not isinstance(config.get(name), str) or not config[name]:
            raise ValueError(f"ML config {name!r} must be a non-empty string.")
    ZoneInfo(config["analysis_timezone"])

    for name in (
        "key_columns",
        "categorical_features",
        "numeric_features",
        "missing_indicator_features",
    ):
        values = config.get(name)
        if not isinstance(values, list) or not values or len(values) != len(set(values)):
            raise ValueError(f"ML config {name!r} must be a non-empty unique list.")
        if not all(isinstance(value, str) and value for value in values):
            raise ValueError(f"ML config {name!r} contains an invalid column name.")
    unknown_indicators = set(config["missing_indicator_features"]) - set(
        config["numeric_features"]
    )
    if unknown_indicators:
        raise ValueError(
            "Missing indicators must refer to numeric features: "
            f"{sorted(unknown_indicators)}"
        )
    feature_overlap = set(config["categorical_features"]) & set(
        config["numeric_features"]
    )
    if feature_overlap:
        raise ValueError(
            f"Categorical and numeric features must be disjoint: {sorted(feature_overlap)}"
        )
    if config["timestamp_column"] not in config["key_columns"]:
        raise ValueError("timestamp_column must be one of key_columns.")
    protected = {
        config["label_column"],
        config["split_column"],
        FEATURES_COLUMN,
        PREDICTION_COLUMN,
    }
    if protected & (set(config["categorical_features"]) | set(config["numeric_features"])):
        raise ValueError("Label, split and output columns cannot also be model features.")
    if config["baseline_prediction_column"] not in config["numeric_features"]:
        raise ValueError("baseline_prediction_column must be a configured numeric feature.")

    split_values = config.get("split_values")
    if not isinstance(split_values, dict) or set(split_values) != set(_SPLIT_ORDER):
        raise ValueError("split_values must define train, validation and test.")
    if len(set(split_values.values())) != len(_SPLIT_ORDER):
        raise ValueError("Configured split values must be distinct.")

    if config["imputer_strategy"] not in {"mean", "median"}:
        raise ValueError("imputer_strategy must be mean or median.")
    if not isinstance(config.get("scale_numeric"), bool):
        raise ValueError("scale_numeric must be true or false.")
    metrics = config.get("evaluation_metrics")
    if not isinstance(metrics, list) or not metrics or not set(metrics).issubset(
        _SUPPORTED_METRICS
    ):
        raise ValueError(f"evaluation_metrics must use {sorted(_SUPPORTED_METRICS)}.")
    if "rmse" not in metrics or len(metrics) != len(set(metrics)):
        raise ValueError("evaluation_metrics must contain unique names including rmse.")

    model = config.get("model")
    if not isinstance(model, dict) or model.get("type") != "linear_regression":
        raise ValueError("Only model.type='linear_regression' is supported in Week 4 role B.")
    for name in ("reg_params", "elastic_net_params"):
        values = model.get(name)
        if not isinstance(values, list) or not values:
            raise ValueError(f"model.{name} must be a non-empty list.")
        if any(not math.isfinite(float(value)) or float(value) < 0 for value in values):
            raise ValueError(f"model.{name} must contain finite non-negative values.")
    if any(float(value) > 1 for value in model["elastic_net_params"]):
        raise ValueError("model.elastic_net_params cannot contain values above 1.")
    if int(model.get("max_iter", 0)) < 1 or float(model.get("tolerance", 0)) <= 0:
        raise ValueError("Linear regression max_iter and tolerance must be positive.")
    if int(config.get("reload_check_rows", 0)) < 1:
        raise ValueError("reload_check_rows must be positive.")
    return config


def required_training_columns(config: Mapping[str, Any]) -> tuple[str, ...]:
    """Public input contract that role A's training-dataset builder must produce."""
    ordered = [
        *config["key_columns"],
        config["timestamp_column"],
        config["label_column"],
        config["split_column"],
        *config["categorical_features"],
        *config["numeric_features"],
    ]
    return tuple(dict.fromkeys(str(name) for name in ordered))


def _split_names(config: Mapping[str, Any]) -> dict[str, str]:
    return {name: str(config["split_values"][name]) for name in _SPLIT_ORDER}


def validate_training_dataset(frame: DataFrame, config: Mapping[str, Any]) -> dict[str, int]:
    """Fail fast on leakage-prone or incompatible training-dataset contracts."""
    session_timezone = frame.sparkSession.conf.get("spark.sql.session.timeZone")
    if session_timezone != "UTC":
        raise ValueError(
            f"Spark session timezone must be UTC for ML keys, got {session_timezone!r}."
        )
    missing = sorted(set(required_training_columns(config)) - set(frame.columns))
    if missing:
        raise ValueError(f"Training dataset is missing required columns: {missing}")
    generated = {
        FEATURES_COLUMN,
        PREDICTION_COLUMN,
        "numeric_features_unscaled",
        "numeric_features_scaled",
        "local_hour",
        "local_day_of_week",
        "local_month",
        "local_hour_sin",
        "local_hour_cos",
        "local_dow_sin",
        "local_dow_cos",
        "local_month_sin",
        "local_month_cos",
        *[f"{name}__imputed" for name in config["numeric_features"]],
        *[f"{name}__missing" for name in config["missing_indicator_features"]],
        *[f"{name}__filled" for name in config["categorical_features"]],
        *[f"{name}__index" for name in config["categorical_features"]],
        *[f"{name}__encoded" for name in config["categorical_features"]],
    }
    collisions = sorted(generated & set(frame.columns))
    if collisions:
        raise ValueError(f"Training dataset contains reserved output columns: {collisions}")

    timestamp_column = str(config["timestamp_column"])
    label_column = str(config["label_column"])
    split_column = str(config["split_column"])
    if not isinstance(frame.schema[timestamp_column].dataType, TimestampType):
        raise TypeError(f"{timestamp_column} must be a Spark timestamp column.")
    if not isinstance(frame.schema[label_column].dataType, NumericType):
        raise TypeError(f"{label_column} must be numeric.")
    if not isinstance(frame.schema[split_column].dataType, StringType):
        raise TypeError(f"{split_column} must be a Spark string column.")
    non_integral_keys = [
        name
        for name in config["key_columns"]
        if str(name) != timestamp_column
        and not isinstance(frame.schema[str(name)].dataType, IntegralType)
    ]
    if non_integral_keys:
        raise TypeError(f"Non-timestamp training keys must be integral: {non_integral_keys}")
    non_string = [
        name
        for name in config["categorical_features"]
        if not isinstance(frame.schema[str(name)].dataType, StringType)
    ]
    if non_string:
        raise TypeError(f"Configured categorical features are not strings: {non_string}")
    non_numeric = [
        name
        for name in config["numeric_features"]
        if not isinstance(frame.schema[str(name)].dataType, NumericType)
    ]
    if non_numeric:
        raise TypeError(f"Configured numeric features are not numeric: {non_numeric}")

    keys = [str(name) for name in config["key_columns"]]
    invalid_key = F.col(timestamp_column).isNull()
    for name in keys:
        invalid_key = invalid_key | F.col(name).isNull()
    label = F.col(label_column).cast("double")
    quality_expressions = [
        F.max(F.when(invalid_key, 1).otherwise(0)).alias("invalid_key"),
        F.max(
            F.when(
                (F.minute(timestamp_column) != 0) | (F.second(timestamp_column) != 0),
                1,
            ).otherwise(0)
        ).alias("partial_hour"),
        F.max(
            F.when(
                F.col(label_column).isNull()
                | F.isnan(label)
                | label.isin(float("inf"), float("-inf"))
                | (label < 0),
                1,
            ).otherwise(0)
        ).alias("invalid_label"),
    ]
    numeric_flags = {}
    for index, name in enumerate(config["numeric_features"]):
        value = F.col(str(name)).cast("double")
        alias = f"invalid_numeric_{index}"
        numeric_flags[alias] = str(name)
        quality_expressions.append(
            F.max(
                F.when(
                    F.col(str(name)).isNotNull()
                    & (F.isnan(value) | value.isin(float("inf"), float("-inf"))),
                    1,
                ).otherwise(0)
            ).alias(alias)
        )
    quality = frame.agg(*quality_expressions).first()
    if quality["invalid_key"]:
        raise ValueError("Training keys and target timestamp must be non-null.")
    if frame.groupBy(*keys).count().where(F.col("count") > 1).limit(1).count():
        raise ValueError(f"Training dataset must be unique on {keys}.")
    if quality["partial_hour"]:
        raise ValueError(f"{timestamp_column} must contain whole UTC hours.")
    if quality["invalid_label"]:
        raise ValueError("Demand labels must be finite, non-null and non-negative.")
    invalid_numeric = [name for alias, name in numeric_flags.items() if quality[alias]]
    if invalid_numeric:
        raise ValueError(f"Numeric features contain non-finite values: {invalid_numeric}")

    expected = _split_names(config)
    allowed = set(expected.values())
    split_rows = frame.groupBy(split_column).agg(
        F.count(F.lit(1)).alias("row_count"),
        F.min(timestamp_column).alias("minimum"),
        F.max(timestamp_column).alias("maximum"),
    ).collect()
    if any(row[split_column] is None for row in split_rows):
        raise ValueError("Training split cannot be null.")
    by_split = {str(row[split_column]): row for row in split_rows}
    observed = set(by_split)
    if observed != allowed:
        raise ValueError(
            f"Training dataset splits must be exactly {sorted(allowed)}, got {sorted(observed)}."
        )

    bounds: dict[str, tuple[Any, Any]] = {}
    counts: dict[str, int] = {}
    for logical_name in _SPLIT_ORDER:
        value = expected[logical_name]
        row = by_split[value]
        counts[logical_name] = int(row["row_count"])
        bounds[logical_name] = (row["minimum"], row["maximum"])
    if not (
        bounds["train"][1] < bounds["validation"][0]
        and bounds["validation"][1] < bounds["test"][0]
    ):
        raise ValueError("Train, validation and test must be disjoint chronological blocks.")
    train = frame.where(F.col(split_column) == expected["train"])
    non_null_counts = train.agg(
        *[
            F.count(F.col(str(name))).alias(str(name))
            for name in config["numeric_features"]
        ]
    ).first()
    entirely_missing = [
        str(name)
        for name in config["numeric_features"]
        if non_null_counts[str(name)] == 0
    ]
    if entirely_missing:
        raise ValueError(
            f"Train has no value from which to impute numeric features: {entirely_missing}"
        )
    return counts


def _derived_sql(config: Mapping[str, Any]) -> str:
    timestamp = str(config["timestamp_column"])
    timezone_name = str(config["analysis_timezone"]).replace("'", "''")
    indicators = []
    for name in config["missing_indicator_features"]:
        safe = str(name).replace("`", "``")
        indicators.append(
            f"CASE WHEN `{safe}` IS NULL THEN 1.0 ELSE 0.0 END "
            f"AS `{safe}__missing`"
        )
    categorical_fills = []
    for name in config["categorical_features"]:
        safe = str(name).replace("`", "``")
        categorical_fills.append(
            f"COALESCE(CAST(`{safe}` AS STRING), '__MISSING__') "
            f"AS `{safe}__filled`"
        )
    local = f"from_utc_timestamp(`{timestamp}`, '{timezone_name}')"
    derived = [
        f"hour({local}) AS `local_hour`",
        f"dayofweek({local}) AS `local_day_of_week`",
        f"month({local}) AS `local_month`",
        f"sin(2.0 * pi() * hour({local}) / 24.0) AS `local_hour_sin`",
        f"cos(2.0 * pi() * hour({local}) / 24.0) AS `local_hour_cos`",
        f"sin(2.0 * pi() * (dayofweek({local}) - 1.0) / 7.0) AS `local_dow_sin`",
        f"cos(2.0 * pi() * (dayofweek({local}) - 1.0) / 7.0) AS `local_dow_cos`",
        f"sin(2.0 * pi() * (month({local}) - 1.0) / 12.0) AS `local_month_sin`",
        f"cos(2.0 * pi() * (month({local}) - 1.0) / 12.0) AS `local_month_cos`",
        *indicators,
        *categorical_fills,
    ]
    return "SELECT *, " + ", ".join(derived) + " FROM __THIS__"


def feature_stages(config: Mapping[str, Any]) -> list[Any]:
    """Build unfitted Spark ML stages; every learned stage is fit on train only."""
    numeric = [str(name) for name in config["numeric_features"]]
    imputed = [f"{name}__imputed" for name in numeric]
    categorical = [str(name) for name in config["categorical_features"]]
    categorical_filled = [f"{name}__filled" for name in categorical]
    indexes = [f"{name}__index" for name in categorical]
    encoded = [f"{name}__encoded" for name in categorical]
    indicators = [f"{name}__missing" for name in config["missing_indicator_features"]]
    temporal = (
        "local_hour_sin",
        "local_hour_cos",
        "local_dow_sin",
        "local_dow_cos",
        "local_month_sin",
        "local_month_cos",
    )

    stages: list[Any] = [SQLTransformer(statement=_derived_sql(config))]
    stages.append(
        Imputer(
            inputCols=numeric,
            outputCols=imputed,
            strategy=str(config["imputer_strategy"]),
        )
    )
    stages.extend(
        StringIndexer(
            inputCol=input_name,
            outputCol=output_name,
            handleInvalid="keep",
            stringOrderType="alphabetAsc",
        )
        for input_name, output_name in zip(categorical_filled, indexes, strict=True)
    )
    stages.append(
        OneHotEncoder(
            inputCols=indexes,
            outputCols=encoded,
            handleInvalid="keep",
            dropLast=False,
        )
    )
    stages.append(
        VectorAssembler(
            inputCols=[*imputed, *indicators, *temporal],
            outputCol="numeric_features_unscaled",
            handleInvalid="error",
        )
    )
    numeric_output = "numeric_features_unscaled"
    if bool(config.get("scale_numeric", True)):
        stages.append(
            StandardScaler(
                inputCol="numeric_features_unscaled",
                outputCol="numeric_features_scaled",
                withMean=False,
                withStd=True,
            )
        )
        numeric_output = "numeric_features_scaled"
    stages.append(
        VectorAssembler(
            inputCols=[numeric_output, *encoded],
            outputCol=FEATURES_COLUMN,
            handleInvalid="error",
        )
    )
    return stages


def build_feature_pipeline(config: Mapping[str, Any]) -> Pipeline:
    """Return the reusable Task 2 pipeline without a prediction model."""
    return Pipeline(stages=feature_stages(config))


def fit_feature_pipeline(
    frame: DataFrame, config: Mapping[str, Any]
) -> tuple[PipelineModel, dict[str, int]]:
    """Fit imputers/encoders/scalers on train only and return their reusable model."""
    counts = validate_training_dataset(frame, config)
    train_value = _split_names(config)["train"]
    train = frame.where(F.col(str(config["split_column"])) == train_value)
    return build_feature_pipeline(config).fit(train), counts


def select_feature_output(frame: DataFrame, config: Mapping[str, Any]) -> DataFrame:
    """Remove source/audit attributes and retain only ML keys, label, split and features."""
    columns = [
        *[str(name) for name in config["key_columns"]],
        str(config["label_column"]),
        str(config["split_column"]),
        FEATURES_COLUMN,
    ]
    return frame.select(*dict.fromkeys(columns))


def _metric_values(
    predictions: DataFrame,
    config: Mapping[str, Any],
    *,
    prediction_column: str = PREDICTION_COLUMN,
) -> dict[str, float]:
    label_column = str(config["label_column"])
    metrics = {}
    for metric in config["evaluation_metrics"]:
        evaluator = RegressionEvaluator(
            labelCol=label_column,
            predictionCol=prediction_column,
            metricName=str(metric),
        )
        metrics[str(metric)] = float(evaluator.evaluate(predictions))
    return metrics


def _baseline_metrics(
    frame: DataFrame, config: Mapping[str, Any]
) -> dict[str, float | int | None]:
    baseline = str(config["baseline_prediction_column"])
    usable = frame.where(F.col(baseline).isNotNull()).withColumn(
        "baseline_prediction", F.col(baseline).cast("double")
    )
    count = usable.count()
    if count == 0:
        return {"row_count": 0, **{metric: None for metric in config["evaluation_metrics"]}}
    return {
        "row_count": count,
        **_metric_values(usable, config, prediction_column="baseline_prediction"),
    }


def _candidate_grid(config: Mapping[str, Any]) -> list[dict[str, float]]:
    return [
        {"reg_param": float(reg), "elastic_net_param": float(elastic)}
        for reg in config["model"]["reg_params"]
        for elastic in config["model"]["elastic_net_params"]
    ]


def build_regression(
    config: Mapping[str, Any], *, reg_param: float, elastic_net_param: float
) -> LinearRegression:
    """Build the Linear Regression estimator for one candidate; it reads ``features``."""
    model_config = config["model"]
    return LinearRegression(
        featuresCol=FEATURES_COLUMN,
        labelCol=str(config["label_column"]),
        predictionCol=PREDICTION_COLUMN,
        regParam=float(reg_param),
        elasticNetParam=float(elastic_net_param),
        maxIter=int(model_config["max_iter"]),
        tol=float(model_config["tolerance"]),
        standardization=False,
    )


def _verify_reload(
    model: PipelineModel,
    model_path: str | Path,
    test: DataFrame,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    keys = [str(name) for name in config["key_columns"]]
    sample = test.orderBy(*keys).limit(int(config["reload_check_rows"]))
    before = model.transform(sample).select(*keys, PREDICTION_COLUMN).collect()
    loaded = PipelineModel.load(str(model_path))
    after = loaded.transform(sample).select(*keys, PREDICTION_COLUMN).collect()
    if len(before) != len(after):
        raise RuntimeError("Reloaded model returned a different prediction count.")
    for original, reloaded in zip(before, after, strict=True):
        if tuple(original[name] for name in keys) != tuple(reloaded[name] for name in keys):
            raise RuntimeError("Reloaded model changed the deterministic verification keys.")
        if not math.isclose(
            float(original[PREDICTION_COLUMN]),
            float(reloaded[PREDICTION_COLUMN]),
            rel_tol=1e-10,
            abs_tol=1e-10,
        ):
            raise RuntimeError("Reloaded model predictions differ from the fitted model.")
    return {"verified": True, "row_count": len(before)}


def train_and_evaluate(
    frame: DataFrame,
    config: Mapping[str, Any],
    *,
    model_path: str | Path | None = None,
) -> TrainingResult:
    """Fit preprocessing once on train, select the regression on validation, test once.

    The saved model is one PipelineModel: the fitted feature stages followed by the
    selected regression, so a reload predicts from the raw training-dataset columns.
    """
    total_started = perf_counter()
    counts = validate_training_dataset(frame, config)
    split_column = str(config["split_column"])
    split_values = _split_names(config)
    feature_started = perf_counter()
    feature_model = build_feature_pipeline(config).fit(
        frame.where(F.col(split_column) == split_values["train"])
    )
    kept = dict.fromkeys(
        [str(config["label_column"]), str(config["baseline_prediction_column"]), FEATURES_COLUMN]
    )
    featured = {
        name: feature_model.transform(frame.where(F.col(split_column) == value))
        .select(*kept)
        .cache()
        for name, value in split_values.items()
    }
    try:
        for part in featured.values():
            part.count()
        feature_seconds = perf_counter() - feature_started

        candidates = []
        best: tuple[float, int, Any, dict[str, float]] | None = None
        for index, params in enumerate(_candidate_grid(config)):
            started = perf_counter()
            regression = build_regression(config, **params).fit(featured["train"])
            fit_seconds = perf_counter() - started
            validation_metrics = _metric_values(
                regression.transform(featured["validation"]), config
            )
            candidates.append({
                "candidate_index": index,
                **params,
                "model_fit_seconds": fit_seconds,
                "validation_seconds": perf_counter() - started - fit_seconds,
                "validation_metrics": validation_metrics,
            })
            rank = (validation_metrics["rmse"], index)
            if best is None or rank < (best[0], best[1]):
                best = (rank[0], rank[1], regression, params)
        if best is None:
            raise RuntimeError("No ML candidate was fitted.")
        _rmse, selected_index, regression, selected_params = best
        selected_model = PipelineModel(stages=[*feature_model.stages, regression])
        test_started = perf_counter()
        test_metrics = _metric_values(regression.transform(featured["test"]), config)
        test_seconds = perf_counter() - test_started
        validation_baseline = _baseline_metrics(featured["validation"], config)
        test_baseline = _baseline_metrics(featured["test"], config)

        reload_check = None
        if model_path is not None:
            selected_model.write().overwrite().save(str(model_path))
            reload_check = _verify_reload(
                selected_model,
                model_path,
                frame.where(F.col(split_column) == split_values["test"]),
                config,
            )
        report = {
            "status": "success",
            "prediction_task": config["prediction_task"],
            "schema_version": config["schema_version"],
            "split_counts": counts,
            "feature_fit_seconds": feature_seconds,
            "candidate_count": len(candidates),
            "candidates": candidates,
            "selected_candidate_index": selected_index,
            "selected_parameters": selected_params,
            "validation_metrics": candidates[selected_index]["validation_metrics"],
            "test_metrics": test_metrics,
            "baseline": {
                "prediction_column": config["baseline_prediction_column"],
                "validation": validation_baseline,
                "test": test_baseline,
            },
            "test_evaluation_seconds": test_seconds,
            "total_seconds": perf_counter() - total_started,
            "model_path": None if model_path is None else str(model_path),
            "reload_check": reload_check,
        }
        return TrainingResult(model=selected_model, report=report)
    finally:
        for part in featured.values():
            part.unpersist()


def load_training_dataset(
    spark: SparkSession, metadata_path: str | Path
) -> tuple[DataFrame, dict[str, Any]]:
    """Load the training Delta version that the dataset metadata names, never the latest."""
    metadata = json.loads(Path(metadata_path).read_text(encoding="utf-8"))
    frame = spark.read.format("delta").option(
        "versionAsOf", int(metadata["output_version"])
    ).load(str(metadata["output_path"]))
    return frame, {
        "metadata_path": str(metadata_path),
        "path": str(metadata["output_path"]),
        "version": int(metadata["output_version"]),
        "source_run_id": metadata["source_run_id"],
    }


def write_json(path: str | Path, value: Mapping[str, Any]) -> None:
    """Atomically write a human-readable run/config/metric artifact."""
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    pending = output.with_suffix(output.suffix + ".tmp")
    pending.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    pending.replace(output)
