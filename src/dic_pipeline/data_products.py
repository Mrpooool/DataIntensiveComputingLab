"""Week 2 input registration and durable analytical data-product refreshes."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import perf_counter
from typing import Any
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .ingestion import DATASETS, DEFAULT_DELTA_ROOT, PROJECT_ROOT, write_delta
from .integration import INTEGRATED_TABLE, INTEGRATION_MANIFEST, publish_integration_snapshot
from .queries import DEFAULT_QUERY_CONFIG, integrated_weather_category_sql, load_query_config


DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "data_products.json"
BATCH_MANIFEST = "metadata/completed_batch.json"
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
ANALYSIS_TIMEZONE = "America/New_York"
RESERVED_METADATA_COLUMNS = (
    "data_source",
    "source_delta_version",
    "created_at_utc",
    "refreshed_at_utc",
    "schema_version",
)
REFRESH_METADATA_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("data_source", StringType(), False),
        StructField("source_path", StringType(), False),
        StructField("source_delta_version", LongType(), False),
        StructField("source_snapshot_json", StringType(), False),
        StructField("created_at_utc", TimestampType(), False),
        StructField("started_at", TimestampType(), False),
        StructField("finished_at", TimestampType(), False),
        StructField("execution_seconds", DoubleType(), False),
        StructField("row_count", LongType(), True),
        StructField("data_bytes", LongType(), True),
        StructField("data_files", LongType(), True),
        StructField("schema_version", StringType(), False),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True),
    ]
)

ProductBuilder = Callable[[SparkSession], DataFrame]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _local_timestamp(column: str, timezone_name: str = ANALYSIS_TIMEZONE):
    return F.from_utc_timestamp(F.col(column), timezone_name)


def load_product_config(path: str | Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    config = json.loads(Path(path).read_text(encoding="utf-8"))
    products = config.get("products")
    if not isinstance(products, dict) or not products:
        raise ValueError("Data-product config must define a non-empty products object.")
    for name, settings in products.items():
        if not isinstance(settings, dict) or not settings.get("schema_version"):
            raise ValueError(f"Product {name!r} must define schema_version.")
    return config


def product_table_stats(spark: SparkSession, path: str | Path) -> dict[str, int]:
    """Current Delta snapshot size and file count, excluding logs and stale files."""
    table = DeltaTable.forPath(spark, str(path))
    detail = table.detail().first()
    return {
        "data_bytes": int(detail.sizeInBytes),
        "data_files": int(detail.numFiles),
        "delta_version": int(table.history(1).first()["version"]),
    }


def integration_snapshot(
    spark: SparkSession,
    delta_root: str | Path = DEFAULT_DELTA_ROOT,
) -> dict[str, Any]:
    """Read and validate the immutable handoff for analytics products.

    ``scripts.run_integration`` publishes ``completed_integration.json`` after
    every successful integration. Week 1 outputs predate that manifest; for
    them the snapshot is bootstrapped from the completed batch and the
    integrated table, but only when the table's ingestion run_id proves it was
    built from that batch.
    """
    root = Path(delta_root)
    manifest_path = root / INTEGRATION_MANIFEST
    batch_path = root / BATCH_MANIFEST
    integrated_path = root / INTEGRATED_TABLE
    if not manifest_path.exists():
        if not batch_path.exists():
            raise RuntimeError(
                "No completed integration snapshot or completed batch. "
                "Run scripts.run_ingestion --dataset all and scripts.run_integration first."
            )
        if not (integrated_path / "_delta_log").exists():
            raise RuntimeError(
                "No integrated Delta table. Run scripts.run_integration after a completed batch."
            )
        source_batch = json.loads(batch_path.read_text(encoding="utf-8"))
        publish_integration_snapshot(spark, root, source_batch=source_batch)
    snapshot = json.loads(manifest_path.read_text(encoding="utf-8"))
    required = {"run_id", "standardized_versions", "integrated_version"}
    if not required.issubset(snapshot):
        raise ValueError(f"Integration snapshot is missing {sorted(required - set(snapshot))}.")
    if set(snapshot["standardized_versions"]) != set(DATASETS):
        raise ValueError("Integration snapshot must reference all standardized datasets.")
    if batch_path.exists():
        snapshot["completed_batch"] = json.loads(batch_path.read_text(encoding="utf-8"))
    snapshot["integrated_path"] = str(integrated_path)
    return snapshot


def register_analytics_inputs(
    spark: SparkSession,
    delta_root: str | Path = DEFAULT_DELTA_ROOT,
) -> dict[str, Any]:
    """Register one consistent source snapshot as stable Spark SQL views."""
    root = Path(delta_root)
    snapshot = integration_snapshot(spark, root)
    integrated_path = Path(snapshot["integrated_path"])
    if not (integrated_path / "_delta_log").exists():
        raise RuntimeError(f"Integrated Delta table is missing: {integrated_path}")
    for dataset, version in snapshot["standardized_versions"].items():
        path = root / "standardized" / dataset
        if not (path / "_delta_log").exists():
            raise RuntimeError(f"Standardized Delta table is missing: {path}")
        (
            spark.read.format("delta")
            .option("versionAsOf", int(version))
            .load(str(path))
            .createOrReplaceTempView(f"standardized_{dataset}")
        )
    (
        spark.read.format("delta")
        .option("versionAsOf", int(snapshot["integrated_version"]))
        .load(str(integrated_path))
        .createOrReplaceTempView("integrated_taxi_trips")
    )
    return snapshot


def _metadata_path(output_root: str | Path) -> Path:
    return Path(output_root) / "metadata" / "product_refresh_runs"


def _utc_from_micros(micros: int) -> datetime:
    return _EPOCH + timedelta(microseconds=int(micros))


def _created_at_frame(
    spark: SparkSession,
    output_root: str | Path,
    product_name: str,
    fallback: datetime,
) -> DataFrame:
    """Keep created_at inside Spark; only timezone-aware Python datetimes cross into it.

    A naive datetime is interpreted in the host's local zone on the way into
    Spark, which shifted UTC metadata by the host offset on non-UTC machines.
    """
    product_path = Path(output_root) / product_name
    if (product_path / "_delta_log").exists():
        existing = (
            spark.read.format("delta")
            .load(str(product_path))
            .agg(F.min("created_at_utc").alias("created_at_utc"))
        )
        if existing.where(F.col("created_at_utc").isNotNull()).limit(1).count():
            return existing
    if fallback.tzinfo is None:
        raise ValueError("created_at fallback must be timezone-aware.")
    return spark.createDataFrame([(fallback,)], "created_at_utc timestamp")


def _write_refresh_metadata(
    spark: SparkSession,
    output_root: str | Path,
    record: Mapping[str, Any],
) -> None:
    path = _metadata_path(output_root)
    mode = "append" if (path / "_delta_log").exists() else "overwrite"
    (
        spark.createDataFrame([dict(record)], REFRESH_METADATA_SCHEMA)
        .coalesce(1)
        .write.format("delta")
        .mode(mode)
        .option("mergeSchema", "true")
        .save(str(path))
    )


def _assert_unique_keys(frame: DataFrame, keys: Sequence[str], product_name: str) -> None:
    if not keys:
        return
    missing = [name for name in keys if name not in frame.columns]
    if missing:
        raise RuntimeError(f"Product {product_name!r} is missing key columns: {missing}")
    duplicates = frame.groupBy(*keys).count().where(F.col("count") > 1)
    if duplicates.limit(1).count():
        raise RuntimeError(f"Product {product_name!r} has duplicate business keys.")


def _schema_signature(frame: DataFrame) -> list[tuple[str, str]]:
    return [(field.name, field.dataType.simpleString()) for field in frame.schema.fields]


def refresh_product(
    spark: SparkSession,
    product_name: str,
    builder: ProductBuilder,
    *,
    snapshot: Mapping[str, Any],
    settings: Mapping[str, Any],
    delta_root: str | Path = DEFAULT_DELTA_ROOT,
    output_root: str | Path | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Build, overwrite, read back, and record one data product."""
    current_run_id = run_id or str(uuid4())
    started_at = _utc_now()
    started = perf_counter()
    root = Path(delta_root)
    products_root = Path(output_root) if output_root is not None else root / "analytics"
    created_at_frame = _created_at_frame(spark, products_root, product_name, started_at)
    # Read the epoch, not a collected datetime: collect() renders host-local naive values.
    created_at = _utc_from_micros(
        created_at_frame.select(F.unix_micros("created_at_utc")).first()[0]
    )
    source_version = int(snapshot["integrated_version"])
    source_path = str(snapshot.get("integrated_path") or root / "integrated" / "integrated_taxi_trips")
    source_snapshot_json = json.dumps(dict(snapshot), sort_keys=True, default=str)
    row_count: int | None = None
    data_bytes: int | None = None
    data_files: int | None = None
    status = "failed"
    error_message: str | None = None
    refreshed_at = started_at

    try:
        product = builder(spark)
        if not isinstance(product, DataFrame):
            raise TypeError(f"Builder for {product_name!r} must return a Spark DataFrame.")
        conflicts = sorted(set(product.columns).intersection(RESERVED_METADATA_COLUMNS))
        if conflicts:
            raise ValueError(f"Product {product_name!r} uses reserved columns: {conflicts}")
        refreshed_at = _utc_now()
        enriched = (
            product.crossJoin(created_at_frame)
            .withColumn("data_source", F.lit("integrated_taxi_trips"))
            .withColumn("source_delta_version", F.lit(source_version).cast("long"))
            .withColumn("refreshed_at_utc", F.lit(refreshed_at))
            .withColumn("schema_version", F.lit(str(settings["schema_version"])))
        )
        _assert_unique_keys(enriched, list(settings.get("keys") or []), product_name)
        row_count = enriched.count()
        output_path = products_root / product_name
        write_delta(
            enriched,
            output_path,
            num_files=int(settings.get("output_files", 1)),
        )
        written = spark.read.format("delta").load(str(output_path))
        if written.count() != row_count:
            raise RuntimeError(f"Read-back row count failed for {product_name!r}.")
        if _schema_signature(written) != _schema_signature(enriched):
            raise RuntimeError(f"Read-back schema failed for {product_name!r}.")
        _assert_unique_keys(written, list(settings.get("keys") or []), product_name)
        stats = product_table_stats(spark, output_path)
        data_bytes = stats["data_bytes"]
        data_files = stats["data_files"]
        status = "success"
    except Exception as error:
        error_message = f"{type(error).__name__}: {error}"
        raise
    finally:
        record = {
            "run_id": current_run_id,
            "product_name": product_name,
            "data_source": "integrated_taxi_trips",
            "source_path": source_path,
            "source_delta_version": source_version,
            "source_snapshot_json": source_snapshot_json,
            "created_at_utc": created_at,
            "started_at": started_at,
            "finished_at": _utc_now(),
            "execution_seconds": perf_counter() - started,
            "row_count": row_count,
            "data_bytes": data_bytes,
            "data_files": data_files,
            "schema_version": str(settings["schema_version"]),
            "status": status,
            "error_message": error_message,
        }
        _write_refresh_metadata(spark, products_root, record)
    record["refreshed_at_utc"] = refreshed_at
    return record


def refresh_data_products(
    spark: SparkSession,
    builders: Mapping[str, ProductBuilder] | None = None,
    *,
    delta_root: str | Path = DEFAULT_DELTA_ROOT,
    output_root: str | Path | None = None,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    selected: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    """Refresh configured products from one registered integrated snapshot."""
    config = load_product_config(config_path)
    configured = config["products"]
    names = list(configured) if not selected or selected == ["all"] else list(selected)
    resolved_builders = dict(builders or DEFAULT_PRODUCT_BUILDERS)
    unknown = sorted(set(names) - set(configured))
    missing = sorted(set(names) - set(resolved_builders))
    if unknown:
        raise KeyError(f"Unknown configured data products: {unknown}")
    if missing:
        raise KeyError(f"No aggregation builder supplied for data products: {missing}")
    snapshot = register_analytics_inputs(spark, delta_root)
    run_id = str(uuid4())
    products_root = Path(output_root) if output_root is not None else Path(delta_root) / "analytics"
    return [
        refresh_product(
            spark,
            name,
            resolved_builders[name],
            snapshot=snapshot,
            settings=configured[name],
            delta_root=delta_root,
            output_root=products_root,
            run_id=run_id,
        )
        for name in names
    ]


def _trips(spark: SparkSession) -> DataFrame:
    return spark.table("integrated_taxi_trips")


def _nyc_trips(spark: SparkSession) -> DataFrame:
    """Environment products use the same NYC scope as Q2-Q4."""
    return _trips(spark).where(F.col("environment_in_scope"))


def build_daily_mobility_summary(spark: SparkSession) -> DataFrame:
    """Local date/hour metrics keyed by the actual UTC hour."""
    trips = (
        _trips(spark)
        .withColumn("local_pickup_ts", _local_timestamp("pickup_hour_utc"))
        .withColumn("local_pickup_date", F.to_date("local_pickup_ts"))
        .withColumn("local_pickup_hour", F.hour("local_pickup_ts"))
        .withColumn("local_weekday", F.date_format("local_pickup_ts", "EEEE"))
    )
    return trips.groupBy("pickup_hour_utc").agg(
        F.first("local_pickup_date").alias("local_pickup_date"),
        F.first("local_pickup_hour").alias("local_pickup_hour"),
        F.first("local_weekday").alias("local_weekday"),
        F.count(F.lit(1)).alias("trip_count"),
        F.sum("trip_distance").alias("distance_sum"),
        F.count("trip_distance").alias("valid_distance_count"),
        F.sum("trip_duration_seconds").alias("duration_sum"),
        F.count("trip_duration_seconds").alias("valid_duration_count"),
    )


def build_taxi_zone_statistics(spark: SparkSession) -> DataFrame:
    trips = _trips(spark).withColumn("local_pickup_ts", _local_timestamp("pickup_hour_utc"))
    return trips.groupBy(
        F.date_format(F.col("local_pickup_ts"), "yyyy-MM").alias("local_pickup_month"),
        "pickup_location_id",
    ).agg(
        F.first("pickup_zone", ignorenulls=True).alias("pickup_zone"),
        F.first("pickup_borough", ignorenulls=True).alias("pickup_borough"),
        F.count(F.lit(1)).alias("trip_count"),
        F.sum("trip_distance").alias("distance_sum"),
        F.count("trip_distance").alias("valid_distance_count"),
        F.sum("fare_amount").alias("fare_sum"),
        F.count("fare_amount").alias("valid_fare_count"),
    )


def build_weather_impact_summary(spark: SparkSession) -> DataFrame:
    """NYC trips per pickup zone and frozen weather category, labelled exactly like Q2/Q4.

    observed_hour_count is the number of hours in which the zone had trips under
    that category. The per-category denominator Q4 uses is the weather-hour
    calendar, which depends on the requested range and is derived at query time.
    """
    weather_category = integrated_weather_category_sql(load_query_config(DEFAULT_QUERY_CONFIG))
    trips = _nyc_trips(spark).withColumn("weather_category", F.expr(weather_category))
    return trips.groupBy("pickup_location_id", "weather_category").agg(
        F.first("pickup_zone", ignorenulls=True).alias("pickup_zone"),
        F.first("pickup_borough", ignorenulls=True).alias("pickup_borough"),
        F.count(F.lit(1)).alias("trip_count"),
        F.countDistinct("pickup_hour_utc").alias("observed_hour_count"),
        F.sum("trip_distance").alias("distance_sum"),
        F.count("trip_distance").alias("valid_distance_count"),
    )


def build_air_quality_impact_summary(spark: SparkSession) -> DataFrame:
    """NYC demand per UTC hour with the matched hourly PM2.5, the same population as Q3."""
    trips = (
        _nyc_trips(spark)
        .withColumn("local_pickup_ts", _local_timestamp("pickup_hour_utc"))
        .withColumn("local_pickup_date", F.to_date("local_pickup_ts"))
    )
    return (
        trips.groupBy("pickup_hour_utc")
        .agg(
            F.first("local_pickup_date").alias("local_pickup_date"),
            F.first("air_quality_pm25", ignorenulls=True).alias("air_quality_pm25"),
            F.count(F.lit(1)).alias("trip_count"),
            F.count(F.when(F.col("air_quality_matched"), 1)).alias("matched_trip_count"),
            F.count(F.when(~F.col("air_quality_matched"), 1)).alias("unmatched_trip_count"),
        )
        .withColumn(
            "match_status",
            F.when(F.col("unmatched_trip_count") == 0, F.lit("matched"))
            .when(F.col("matched_trip_count") == 0, F.lit("unmatched"))
            .otherwise(F.lit("mixed")),
        )
    )


DEFAULT_PRODUCT_BUILDERS: dict[str, ProductBuilder] = {
    "daily_mobility_summary": build_daily_mobility_summary,
    "taxi_zone_statistics": build_taxi_zone_statistics,
    "weather_impact_summary": build_weather_impact_summary,
    "air_quality_impact_summary": build_air_quality_impact_summary,
}
