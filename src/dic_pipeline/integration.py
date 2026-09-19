"""Enrichment of standardized Taxi trips and publication of the analytics snapshot."""

import json
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F

from .ingestion import DEFAULT_DELTA_ROOT, load_completed_batch, read_completed_batch, write_delta


INTEGRATED_TABLE = "integrated/integrated_taxi_trips"
INTEGRATION_MANIFEST = "metadata/completed_integration.json"
NYC_BOROUGHS = ("Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island")
WEATHER_METRICS = (
    "temp", "rhum", "prcp", "snwd", "wdir", "wspd", "wpgt", "pres", "cldc", "coco",
)
# Verified against the delivered NYC observations; review before adding other methods.
AIR_SIGNATURE = ("88101", "Micrograms/cubic meter (LC)", "FEM", "636")


def _require_unique_key(frame: DataFrame, key: str) -> None:
    invalid = frame.groupBy(key).count().where(F.col(key).isNull() | (F.col("count") > 1))
    if invalid.limit(1).count():
        raise ValueError(f"{key} must be non-null and unique before joining.")


def add_taxi_zones(taxi: DataFrame, zones: DataFrame) -> DataFrame:
    """Add pickup/dropoff zone names and boroughs, preserving every Taxi row."""
    _require_unique_key(zones, "location_id")

    pickup_zones = zones.select(
        F.col("location_id").alias("pickup_location_id"),
        F.col("zone").alias("pickup_zone"),
        F.col("borough").alias("pickup_borough"),
    )
    dropoff_zones = zones.select(
        F.col("location_id").alias("dropoff_location_id"),
        F.col("zone").alias("dropoff_zone"),
        F.col("borough").alias("dropoff_borough"),
    )
    # The 265-row lookup is small enough to broadcast to each Spark task.
    return (
        taxi.join(F.broadcast(pickup_zones), "pickup_location_id", "left")
        .join(F.broadcast(dropoff_zones), "dropoff_location_id", "left")
        .select(*taxi.columns, "pickup_zone", "pickup_borough", "dropoff_zone", "dropoff_borough")
    )


def aggregate_air_quality(air: DataFrame) -> DataFrame:
    """Compute PM2.5 medians with equal weight per available site, not per instrument."""
    signatures = air.select(
        "parameter_code", "measurement_unit", "method_type", "method_code",
    ).distinct().limit(2).collect()
    if signatures and [tuple(row) for row in signatures] != [AIR_SIGNATURE]:
        raise ValueError("Air aggregation requires PM2.5 LC, Micrograms/cubic meter (LC), FEM method 636.")

    site_hours = air.groupBy("site_id", "air_quality_hour_utc").agg(
        F.median("measurement_value").alias("site_pm25"),
        F.count(F.when(F.col("qualifier").isNotNull(), 1)).alias("flagged_count"),
    )
    return site_hours.groupBy("air_quality_hour_utc").agg(
        F.median("site_pm25").alias("air_quality_pm25"),
        F.count("*").alias("air_quality_site_count"),
        F.sum("flagged_count").alias("air_quality_flagged_observation_count"),
    )


def add_environment(taxi_with_zones: DataFrame, weather: DataFrame, air: DataFrame) -> DataFrame:
    """Left-join same-hour NYC background observations; leave missing values unfilled."""
    _require_unique_key(weather, "weather_hour_utc")
    weather_fields = [name for metric in WEATHER_METRICS for name in (metric, f"{metric}_source")]
    weather_hours = weather.select(
        "weather_hour_utc", "weather_timestamp_source",
        *[F.col(name).alias(f"weather_{name}") for name in weather_fields],
        F.col("quality_flags").alias("weather_quality_flags"),
    )
    air_hours = aggregate_air_quality(air)
    trips = taxi_with_zones.withColumn(
        "environment_in_scope",
        F.coalesce(F.col("pickup_borough").isin(*NYC_BOROUGHS), F.lit(False)),
    )
    return (
        trips.join(
            F.broadcast(weather_hours),
            (trips.pickup_hour_utc == weather_hours.weather_hour_utc) & trips.environment_in_scope,
            "left",
        )
        .join(
            F.broadcast(air_hours),
            (F.col("pickup_hour_utc") == air_hours.air_quality_hour_utc) & F.col("environment_in_scope"),
            "left",
        )
        .withColumn("weather_matched", F.col("weather_hour_utc").isNotNull())
        .withColumn("air_quality_matched", F.col("air_quality_hour_utc").isNotNull())
    )


def integration_metrics(taxi: DataFrame, integrated: DataFrame) -> dict:
    """Verify one output per trip and distinguish hour coverage from metric availability."""
    conditions = {
        "nyc_trip_count": F.col("environment_in_scope"),
        "weather_match_count": F.col("weather_matched"),
        "air_quality_match_count": F.col("air_quality_matched"),
        "air_quality_pm25_nonnull_count": F.col("air_quality_pm25").isNotNull(),
    }
    conditions.update({
        f"weather_{name}_nonnull_count": F.col(f"weather_{name}").isNotNull()
        for name in WEATHER_METRICS
    })
    stats = integrated.agg(
        F.count("*").alias("output_count"),
        F.count_distinct("record_id").alias("unique_record_count"),
        *[F.count(F.when(condition, 1)).alias(name) for name, condition in conditions.items()],
    ).first().asDict()
    stats["input_count"] = taxi.count()
    if stats["output_count"] != stats["input_count"] or stats["unique_record_count"] != stats["output_count"]:
        raise ValueError("Integration must preserve the Taxi count and non-null unique record_id.")
    for source in ("weather", "air_quality"):
        matched = stats[f"{source}_match_count"]
        for scope, denominator in (("overall", stats["input_count"]), ("nyc", stats["nyc_trip_count"])):
            stats[f"{source}_match_rate_{scope}"] = matched / denominator if denominator else None
    return stats


def integrate(
    taxi: DataFrame, weather: DataFrame, air: DataFrame, zones: DataFrame,
) -> tuple[DataFrame, dict]:
    """Enrich accepted trips and return the verified DataFrame plus coverage metrics."""
    with_zones = add_taxi_zones(taxi, zones)
    integrated = add_environment(with_zones, weather, air)
    return integrated, integration_metrics(taxi, integrated)


def verify_integrated_provenance(
    spark: SparkSession,
    table_path: str | Path,
    source_batch: Mapping[str, Any],
) -> None:
    """Refuse to pair an integrated table with a completed batch it was not built from."""
    run_ids = sorted(
        row.run_id
        for row in spark.read.format("delta").load(str(table_path)).select("run_id").distinct().collect()
    )
    expected = str(source_batch["run_id"])
    if run_ids != [expected]:
        raise RuntimeError(
            f"Integrated table {table_path} carries ingestion run(s) {run_ids}, not the completed "
            f"batch {expected!r}. Run scripts.run_integration on the current batch first."
        )


def publish_integration_snapshot(
    spark: SparkSession,
    delta_root: str | Path,
    *,
    source_batch: Mapping[str, Any],
) -> dict[str, Any]:
    """Atomically publish the integrated Delta version with the source versions it came from."""
    root = Path(delta_root)
    table_path = root / INTEGRATED_TABLE
    verify_integrated_provenance(spark, table_path, source_batch)
    version = int(DeltaTable.forPath(spark, str(table_path)).history(1).first()["version"])
    snapshot = {
        "run_id": source_batch["run_id"],
        "standardized_versions": {
            name: int(value) for name, value in source_batch["versions"].items()
        },
        "integrated_version": version,
        "published_at": datetime.now(timezone.utc).isoformat(),
    }
    manifest = root / INTEGRATION_MANIFEST
    manifest.parent.mkdir(parents=True, exist_ok=True)
    pending = manifest.with_suffix(".tmp")
    pending.write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")
    pending.replace(manifest)
    return snapshot


def build_integrated_table(
    spark: SparkSession,
    delta_root: str | Path = DEFAULT_DELTA_ROOT,
) -> dict[str, Any]:
    """Integrate the completed batch, verify the write, then publish the analytics snapshot."""
    root = Path(delta_root)
    batch = load_completed_batch(root)
    tables = read_completed_batch(spark, root)
    integrated, stats = integrate(
        tables["taxi"], tables["weather"], tables["air_quality"], tables["taxi_zones"],
    )
    output = root / INTEGRATED_TABLE
    write_delta(integrated.repartition("pickup_date"), output, partition_by=["pickup_date"])
    written_count = spark.read.format("delta").load(str(output)).count()
    if written_count != stats["output_count"]:
        raise RuntimeError("Written integrated row count differs from the verified result.")
    metrics_path = output.parent / "integration_metrics.json"
    metrics_path.write_text(json.dumps(stats, indent=2) + "\n", encoding="utf-8")
    snapshot = publish_integration_snapshot(spark, root, source_batch=batch)
    return {"stats": stats, "snapshot": snapshot, "output": str(output)}
