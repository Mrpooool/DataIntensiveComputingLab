"""Build the Week 4 zone-hour demand dataset from a pinned platform snapshot."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import unquote, urlparse

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from .data_products import integration_snapshot
from .ingestion import DEFAULT_DELTA_ROOT
from .integration import INTEGRATED_TABLE, NYC_BOROUGHS, aggregate_air_quality
from .ml_pipeline import required_training_columns, validate_training_dataset, write_json
from .queries import load_calendar_coverage


def _utc_hour(value: str) -> datetime:
    parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    if parsed.minute or parsed.second:
        raise ValueError(f"Not a whole UTC hour: {value}")
    return parsed


def dataset_bounds(config: Mapping[str, Any]) -> tuple[datetime, datetime, datetime, datetime]:
    """Validate the fixed coverage and chronological split boundaries."""
    settings = config["training_dataset"]
    bounds = tuple(_utc_hour(str(settings[name])) for name in (
        "coverage_start_utc", "train_end_utc_exclusive",
        "validation_end_utc_exclusive", "coverage_end_utc_exclusive",
    ))
    if not all(left < right for left, right in zip(bounds, bounds[1:])):
        raise ValueError("Training coverage and split boundaries must be strictly chronological.")
    return bounds


def _source_file_name(value: str) -> str:
    parsed = urlparse(value)
    return Path(unquote(parsed.path) if parsed.scheme == "file" else value).name


def source_file_identifiers(
    frames: Mapping[str, DataFrame], config: Mapping[str, Any],
) -> dict[str, list[str]]:
    """Check the four original filesets before treating absent pickup hours as zero."""
    expected = config["training_dataset"]["expected_source_files"]
    datasets = ("taxi", "weather", "air_quality", "taxi_zones")
    if set(expected) != set(datasets):
        raise ValueError("Expected source files must be configured for all four datasets.")
    actual = {}
    for dataset in datasets:
        names = sorted({
            _source_file_name(row["source_file"])
            for row in frames[dataset].select("source_file").distinct().collect()
            if row["source_file"] is not None
        })
        if names != sorted(expected[dataset]):
            raise ValueError(f"{dataset} source files {names} differ from expected {expected[dataset]}.")
        actual[dataset] = names
    integrated_names = sorted({
        _source_file_name(row["source_file"])
        for row in frames["integrated"].select("source_file").distinct().collect()
        if row["source_file"] is not None
    })
    if integrated_names != actual["taxi"]:
        raise ValueError("Integrated Taxi source files differ from the pinned standardized Taxi files.")
    actual["integrated"] = integrated_names
    return actual


def pinned_inputs(
    spark: SparkSession, delta_root: str | Path, config: Mapping[str, Any],
) -> tuple[dict[str, DataFrame], dict[str, Any]]:
    """Reject inconsistent or expanded snapshots before reading fixed Delta versions."""
    root = Path(delta_root)
    snapshot = integration_snapshot(spark, root)
    batch = snapshot.get("completed_batch")
    if not batch or batch.get("run_id") != snapshot["run_id"]:
        raise ValueError("Integration snapshot and completed batch run IDs differ.")
    if batch.get("versions") != snapshot["standardized_versions"]:
        raise ValueError("Integration snapshot and completed batch versions differ.")
    # An original batch carries no window; the configured Taxi validity window applies.
    coverage = load_calendar_coverage(snapshot=snapshot)
    start, _, _, end = dataset_bounds(config)
    expected = (start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S"))
    if coverage != expected:
        raise ValueError(f"Snapshot coverage {coverage} differs from configured {expected}.")
    snapshot["coverage_window"] = {
        "valid_pickup_start_utc": coverage[0], "valid_pickup_end_utc_exclusive": coverage[1],
    }
    paths = {name: root / "standardized" / name for name in snapshot["standardized_versions"]}
    paths["integrated"] = root / INTEGRATED_TABLE
    versions = {**snapshot["standardized_versions"], "integrated": snapshot["integrated_version"]}
    frames = {
        name: spark.read.format("delta").option("versionAsOf", int(versions[name])).load(str(path))
        for name, path in paths.items()
    }
    snapshot["source_paths"] = {name: str(path) for name, path in paths.items()}
    return frames, snapshot


def build_training_dataset(
    spark: SparkSession,
    integrated: DataFrame,
    zones: DataFrame,
    weather: DataFrame,
    air: DataFrame,
    config: Mapping[str, Any],
) -> DataFrame:
    """Complete the NYC zone-hour calendar, then add past-only demand and environment."""
    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("Spark session timezone must be UTC.")
    start, train_end, validation_end, end = dataset_bounds(config)
    zone_frame = zones.where(F.col("borough").isin(*NYC_BOROUGHS)).select(
        F.col("location_id").cast("int").alias("pickup_location_id"),
        F.col("zone").alias("pickup_zone"),
        F.col("borough").alias("pickup_borough"),
    )
    if zone_frame.where(F.col("pickup_location_id").isNull()).limit(1).count():
        raise ValueError("NYC zones contain null location IDs.")
    if zone_frame.groupBy("pickup_location_id").count().where("count > 1").limit(1).count():
        raise ValueError("NYC zones contain duplicate location IDs.")
    if not zone_frame.limit(1).count():
        raise ValueError("No NYC zones were found in the pinned lookup.")

    start_epoch = int(start.timestamp())
    hour_count = int((end - start).total_seconds() // 3600)
    hours = spark.range(hour_count).select(
        F.timestamp_seconds(F.lit(start_epoch) + F.col("id") * F.lit(3600)).alias("target_hour_utc")
    )
    counts = integrated.where(
        (F.col("pickup_hour_utc") >= F.lit(start))
        & (F.col("pickup_hour_utc") < F.lit(end))
    ).groupBy(
        F.col("pickup_location_id"), F.col("pickup_hour_utc").alias("target_hour_utc")
    ).agg(F.count("*").cast("double").alias("observed_count"))
    calendar = zone_frame.crossJoin(hours).join(
        counts, ["pickup_location_id", "target_hour_utc"], "left"
    ).withColumn("trip_count", F.coalesce("observed_count", F.lit(0.0))).drop("observed_count")

    history = Window.partitionBy("pickup_location_id").orderBy("target_hour_utc")
    calendar = (
        calendar.withColumn("demand_lag_1h", F.lag("trip_count", 1).over(history))
        .withColumn("demand_lag_24h", F.lag("trip_count", 24).over(history))
        .withColumn(
            "demand_rolling_mean_24h",
            F.avg("trip_count").over(history.rowsBetween(-24, -1)),
        )
    )
    weather_hours = weather.select(
        F.expr("weather_hour_utc + INTERVAL 1 HOUR").alias("target_hour_utc"),
        F.col("temp").cast("double").alias("weather_temp_lag_1h"),
        F.col("prcp").cast("double").alias("weather_prcp_lag_1h"),
        F.col("coco").cast("string").alias("weather_coco_lag_1h"),
    )
    air_hours = aggregate_air_quality(air).select(
        F.expr("air_quality_hour_utc + INTERVAL 1 HOUR").alias("target_hour_utc"),
        F.col("air_quality_pm25").cast("double").alias("air_quality_pm25_lag_1h"),
    )
    result = calendar.join(F.broadcast(weather_hours), "target_hour_utc", "left").join(
        F.broadcast(air_hours), "target_hour_utc", "left"
    )
    result = result.withColumn(
        "split",
        F.when(F.col("target_hour_utc") < F.lit(train_end), "train")
        .when(F.col("target_hour_utc") < F.lit(validation_end), "validation")
        .otherwise("test"),
    )
    return result.select(*required_training_columns(config))


def materialize_training_dataset(
    spark: SparkSession,
    config: Mapping[str, Any],
    *,
    delta_root: str | Path = DEFAULT_DELTA_ROOT,
    output_path: str | Path | None = None,
    metadata_path: str | Path | None = None,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Publish a checked Delta training table and its source/split audit record."""
    root = Path(delta_root)
    output = Path(output_path or root / "ml" / "training_dataset")
    metadata = Path(metadata_path or root / "ml" / "training_dataset_metadata.json")
    frames, snapshot = pinned_inputs(spark, root, config)
    source_files = source_file_identifiers(frames, config)
    frame = build_training_dataset(
        spark, frames["integrated"], frames["taxi_zones"], frames["weather"],
        frames["air_quality"], config,
    ).cache()
    frame.count()
    split_counts = validate_training_dataset(frame, config)
    metrics = frame.agg(
        F.count("*").alias("row_count"),
        F.sum("trip_count").alias("label_total"),
        F.sum(F.when(F.col("trip_count") == 0, 1).otherwise(0)).alias("zero_demand_rows"),
        F.min("trip_count").alias("label_min"),
        F.max("trip_count").alias("label_max"),
        F.avg("trip_count").alias("label_mean"),
        *[
            F.sum(F.when(F.col(name).isNull(), 1).otherwise(0)).alias(name)
            for name in [*config["categorical_features"], *config["numeric_features"]]
        ],
    ).first().asDict()
    start, train_end, validation_end, end = dataset_bounds(config)
    expected = int((end - start).total_seconds() // 3600)
    nyc_zones = frames["taxi_zones"].where(F.col("borough").isin(*NYC_BOROUGHS)).select(
        F.col("location_id").alias("pickup_location_id"), F.lit(1).alias("in_nyc")
    )
    zone_count = nyc_zones.count()
    if metrics["row_count"] != expected * zone_count:
        raise RuntimeError("Training table does not contain every NYC zone-hour.")
    source_counts = frames["integrated"].join(
        F.broadcast(nyc_zones), "pickup_location_id", "left"
    ).agg(
        F.count("*").alias("input_trip_count"),
        F.sum(F.when(F.col("in_nyc").isNull(), 1).otherwise(0)).alias("excluded_non_nyc_trip_count"),
    ).first().asDict()
    included_trips = source_counts["input_trip_count"] - source_counts["excluded_non_nyc_trip_count"]
    if metrics["label_total"] != included_trips:
        raise RuntimeError("Zone-hour labels do not account for every NYC trip in the snapshot.")
    output.parent.mkdir(parents=True, exist_ok=True)
    frame.write.format("delta").mode("overwrite" if overwrite else "errorifexists").option(
        "overwriteSchema", "true"
    ).save(str(output))
    version = int(DeltaTable.forPath(spark, str(output)).history(1).first()["version"])
    if spark.read.format("delta").option("versionAsOf", version).load(str(output)).count() != metrics["row_count"]:
        raise RuntimeError("Training Delta readback row count differs from the source frame.")
    report = {
        "status": "success", "prediction_task": config["prediction_task"],
        "schema_version": config["schema_version"],
        "output_path": str(output), "output_version": version,
        "source_run_id": snapshot["run_id"],
        "source_paths": snapshot["source_paths"],
        "source_versions": {**snapshot["standardized_versions"], "integrated": snapshot["integrated_version"]},
        "source_files": source_files,
        "coverage_window": snapshot["coverage_window"],
        "split_boundaries_utc": {
            "train_start": start.isoformat(), "validation_start": train_end.isoformat(),
            "test_start": validation_end.isoformat(), "end_exclusive": end.isoformat(),
        },
        "split_counts": split_counts,
        "row_count": metrics["row_count"], "zone_count": zone_count,
        "input_trip_count": source_counts["input_trip_count"],
        "excluded_non_nyc_trip_count": source_counts["excluded_non_nyc_trip_count"],
        "included_nyc_trip_count": included_trips,
        "zero_demand_rows": metrics["zero_demand_rows"],
        "label_distribution": {name: metrics[f"label_{name}"] for name in ("total", "min", "max", "mean")},
        "missing_counts": {
            name: metrics[name] for name in [*config["categorical_features"], *config["numeric_features"]]
        },
        "missing_rates": {
            name: metrics[name] / metrics["row_count"]
            for name in [*config["categorical_features"], *config["numeric_features"]]
        },
        "filtering": "NYC five-borough pickup zones only; no hours outside verified Taxi coverage",
    }
    write_json(metadata, report)
    frame.unpersist()
    return report
