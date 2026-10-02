"""Week 4 Task 4: two routes to the same training dataset, and feature-group contributions.

Approach A (``raw``) prepares the four original files itself (``ml_raw_route``);
Approach B (``platform``) reads the pinned Week 1-3 snapshot. Both share the zone-hour
builder and Role B's feature and training pipeline, so time can differ only in
preparation. Each run writes its training dataset to a fresh directory and trains on
the written table. Before any time is compared, the two warm-up datasets must contain
the same rows; afterwards every run must reproduce the first run's dataset hash and
test metrics, or the comparison is reported as a mismatch without timings.

The feature-group comparison trains the same model on cumulative feature sets of the
published training dataset: Taxi only (time, location, demand history), then with
Weather, then with Air Quality.
"""

from __future__ import annotations

from pathlib import Path
from statistics import median
from time import perf_counter
from typing import Any, Callable, Mapping

from pyspark.sql import DataFrame, SparkSession, functions as F

from .integration import aggregate_air_quality
from .ml_dataset import build_training_dataset, pinned_inputs
from .ml_pipeline import train_and_evaluate
from .ml_raw_route import raw_training_dataset
from .w3_evaluation import remove_tree


ROUTES = ("platform", "raw")
PHASES = ("prepare", "feature_fit", "model_fit", "training_total", "end_to_end")
# Cumulative groups; a feature belongs to its source dataset by its column prefix.
FEATURE_GROUPS = (
    ("taxi_only", ("weather_", "air_quality_")),
    ("plus_weather", ("air_quality_",)),
    ("plus_air_quality", ()),
)


def platform_training_dataset(
    spark: SparkSession, delta_root: str | Path, config: Mapping[str, Any],
) -> DataFrame:
    """Approach B: the builder on the pinned standardized and integrated Delta versions."""
    frames, _snapshot = pinned_inputs(spark, delta_root, config)
    return build_training_dataset(
        spark, frames["integrated"], frames["taxi_zones"], frames["weather"],
        aggregate_air_quality(frames["air_quality"]), config,
    )


def dataset_signature(frame: DataFrame) -> list[int]:
    """Row count and an order-independent content hash."""
    row = frame.agg(F.count(F.lit(1)), F.bit_xor(F.xxhash64(*frame.columns))).first()
    return [int(row[0]), int(row[1] or 0)]


def row_differences(platform: DataFrame, raw: DataFrame) -> dict[str, int]:
    return {
        "only_platform": platform.exceptAll(raw).count(),
        "only_raw": raw.exceptAll(platform).count(),
    }


def run_route(
    spark: SparkSession,
    build: Callable[[], DataFrame],
    config: Mapping[str, Any],
    workspace: Path,
) -> dict[str, Any]:
    """Build and write one route's dataset, then run the shared training lifecycle on it."""
    path = workspace / "training_dataset"
    spark.catalog.clearCache()
    started = perf_counter()
    build().write.format("delta").save(str(path))
    prepare = perf_counter() - started
    dataset = spark.read.format("delta").load(str(path))
    report = train_and_evaluate(dataset, config).report
    model_fit = sum(candidate["model_fit_seconds"] for candidate in report["candidates"])
    return {
        "signature": dataset_signature(dataset),
        # Partition layouts differ between routes, which can move the last bits of a fit.
        "test_metrics": {name: round(value, 6) for name, value in report["test_metrics"].items()},
        "seconds": {
            "prepare": prepare,
            "feature_fit": report["feature_fit_seconds"],
            "model_fit": model_fit,
            "training_total": report["total_seconds"],
            "end_to_end": prepare + report["total_seconds"],
        },
    }


def compare_routes(
    spark: SparkSession,
    *,
    delta_root: str | Path,
    data_dir: str | Path,
    config: Mapping[str, Any],
    workspace_root: Path,
    run_prefix: str,
    repeats: int = 3,
    on_run: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Warm up both routes, require identical rows, then time alternated repeats."""
    if repeats < 1:
        raise ValueError("repeats must be at least 1")
    builders = {
        "platform": lambda: platform_training_dataset(spark, delta_root, config),
        "raw": lambda: raw_training_dataset(spark, data_dir, config),
    }
    schedule = [("warmup", 0, route) for route in ROUTES]
    for repeat in range(1, repeats + 1):
        order = ROUTES if repeat % 2 else ROUTES[::-1]
        schedule += [("measured", repeat, route) for route in order]

    result: dict[str, Any] = {"name": "route_comparison", "status": "running", "runs": []}
    expected = None
    for phase, repeat, route in schedule:
        run_id = f"{run_prefix}-{route}-{phase}{repeat or ''}"
        workspace = workspace_root / run_id
        workspace.mkdir(parents=True)
        entry = {
            "route": route, "phase": phase, "repeat": repeat, "run_id": run_id,
            **run_route(spark, builders[route], config, workspace),
        }
        result["runs"].append(entry)
        if on_run is not None:
            on_run(entry)
        if phase == "measured":
            remove_tree(workspace)
        elif route == ROUTES[-1]:
            warmups = [workspace_root / f"{run_prefix}-{name}-warmup" for name in ROUTES]
            result["row_differences"] = row_differences(*[
                spark.read.format("delta").load(str(path / "training_dataset")) for path in warmups
            ])
            for path in warmups:
                remove_tree(path)
            if any(result["row_differences"].values()):
                result["status"] = "mismatch"
                return result
        outcome = {"signature": entry["signature"], "test_metrics": entry["test_metrics"]}
        if expected is None:
            expected = outcome
        elif outcome != expected:
            result["status"] = "mismatch"
            result["mismatch"] = {"expected": expected, "actual": outcome, "run_id": run_id}
            return result

    result["output"] = expected
    result["median_seconds"] = {
        route: {
            name: median(run["seconds"][name] for run in result["runs"]
                          if run["route"] == route and run["phase"] == "measured")
            for name in PHASES
        }
        for route in ROUTES
    }
    result["status"] = "success"
    return result


def feature_group_config(
    config: Mapping[str, Any], excluded_prefixes: tuple[str, ...],
) -> dict[str, Any]:
    """The ML config without the features of the excluded source datasets."""
    lists = ("categorical_features", "numeric_features", "missing_indicator_features")
    return {
        **config,
        **{name: [f for f in config[name] if not f.startswith(excluded_prefixes)] for name in lists},
    }


def compare_feature_groups(dataset: DataFrame, config: Mapping[str, Any]) -> dict[str, Any]:
    """Train the same model on each cumulative feature group of one training dataset."""
    groups = []
    for name, excluded in FEATURE_GROUPS:
        group = feature_group_config(config, excluded)
        report = train_and_evaluate(dataset, group).report
        groups.append({
            "name": name,
            "categorical_features": group["categorical_features"],
            "numeric_features": group["numeric_features"],
            "selected_parameters": report["selected_parameters"],
            "validation_metrics": report["validation_metrics"],
            "test_metrics": report["test_metrics"],
            "training_seconds": report["total_seconds"],
        })
    return {
        "name": "feature_groups", "status": "success",
        "baseline": report["baseline"], "groups": groups,
    }
