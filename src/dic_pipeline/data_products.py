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

from .ingestion import DATASETS, DEFAULT_DELTA_ROOT, PROJECT_ROOT, write_delta
from .integration import INTEGRATED_TABLE, INTEGRATION_MANIFEST, publish_integration_snapshot
from .monitoring import record_run, run_row
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
# Hour-grained products: mode=auto MERGEs dirty pickup_hour_utc keys only.
INCREMENTAL_HOUR_PRODUCTS = frozenset({
    "daily_mobility_summary",
    "air_quality_impact_summary",
})
HOUR_KEY = "pickup_hour_utc"

ProductBuilder = Callable[..., DataFrame]


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


def _built_from_version(spark: SparkSession, product_path: Path) -> int | None:
    """The integrated Delta version the product was last refreshed from; None if never built."""
    if not (product_path / "_delta_log").exists():
        return None
    value = (
        spark.read.format("delta").load(str(product_path))
        .agg(F.max("source_delta_version")).first()[0]
    )
    return None if value is None else int(value)


def _enrich_product(
    product: DataFrame,
    *,
    created_at_frame: DataFrame,
    source_version: int,
    refreshed_at: datetime,
    schema_version: str,
) -> DataFrame:
    """Add the metadata columns and materialize the product once.

    Products are small; without this the key check, the row count and the
    write would each recompute the aggregation over every trip.
    """
    return (
        product.crossJoin(created_at_frame)
        .withColumn("data_source", F.lit("integrated_taxi_trips"))
        .withColumn("source_delta_version", F.lit(source_version).cast("long"))
        .withColumn("refreshed_at_utc", F.lit(refreshed_at))
        .withColumn("schema_version", F.lit(schema_version))
        .localCheckpoint()
    )


def _dirty_hour_keys(
    spark: SparkSession,
    product_name: str,
    integrated_path: str,
    built_from: int,
) -> DataFrame:
    """Hours holding trips of ingestion runs absent from the version the product was built from.

    Integrated appends are atomic per run, so a run id is either wholly in that
    version or wholly new; this also covers several updates between refreshes.
    """
    if product_name == "air_quality_impact_summary":
        trips = _nyc_trips(spark)
    else:
        trips = _trips(spark)
    seen_runs = (
        spark.read.format("delta").option("versionAsOf", built_from).load(integrated_path)
        .select("run_id").distinct()
    )
    return trips.join(seen_runs, "run_id", "left_anti").select(HOUR_KEY).distinct()


def _build_hour_slice(spark: SparkSession, product_name: str, dirty_hours: DataFrame) -> DataFrame:
    if product_name == "daily_mobility_summary":
        scope = _trips(spark).join(dirty_hours, HOUR_KEY, "inner")
        return build_daily_mobility_summary(spark, trips=scope)
    if product_name == "air_quality_impact_summary":
        scope = _nyc_trips(spark).join(dirty_hours, HOUR_KEY, "inner")
        return build_air_quality_impact_summary(spark, trips=scope)
    raise KeyError(f"Product {product_name!r} does not support hour-key incremental refresh.")


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
    monitoring: bool = True,
    refresh_mode: str = "full",
) -> dict[str, Any]:
    """Build or incrementally MERGE one data product, then record monitoring.

    ``refresh_mode='full'`` overwrites the product. ``refresh_mode='incremental'``
    is supported for :data:`INCREMENTAL_HOUR_PRODUCTS`: recompute the
    ``pickup_hour_utc`` keys that received trips since the integrated version the
    product was built from, and MERGE them. Falls back to a full rebuild when the
    product table does not exist yet.
    """
    if refresh_mode not in ("full", "incremental"):
        raise ValueError("refresh_mode must be 'full' or 'incremental'")
    current_run_id = run_id or str(uuid4())
    started_at = _utc_now()
    started = perf_counter()
    root = Path(delta_root)
    products_root = Path(output_root) if output_root is not None else root / "analytics"
    output_path = products_root / product_name
    product_exists = (output_path / "_delta_log").exists()
    effective_mode = refresh_mode
    if effective_mode == "incremental" and (
        product_name not in INCREMENTAL_HOUR_PRODUCTS or not product_exists
    ):
        effective_mode = "full"
    created_at_frame = _created_at_frame(spark, products_root, product_name, started_at)
    # Read the epoch, not a collected datetime: collect() renders host-local naive values.
    created_at = _utc_from_micros(
        created_at_frame.select(F.unix_micros("created_at_utc")).first()[0]
    )
    source_version = int(snapshot["integrated_version"])
    source_path = str(snapshot.get("integrated_path") or root / "integrated" / "integrated_taxi_trips")
    source_snapshot_json = json.dumps(dict(snapshot), sort_keys=True, default=str)
    row_count: int | None = None
    inserted_count: int | None = None
    updated_count: int | None = None
    target_rows_before: int | None = None
    data_bytes: int | None = None
    data_files: int | None = None
    output_version: int | None = None
    status = "failed"
    error: Exception | None = None
    error_message: str | None = None
    refreshed_at = started_at
    keys = list(settings.get("keys") or [])
    schema_version = str(settings["schema_version"])

    try:
        if effective_mode == "incremental":
            built_from = _built_from_version(spark, output_path)
            if built_from is None:
                effective_mode = "full"
            else:
                target_rows_before = spark.read.format("delta").load(str(output_path)).count()
                dirty = _dirty_hour_keys(spark, product_name, source_path, built_from).localCheckpoint()
                if dirty.limit(1).count() == 0:
                    inserted_count = 0
                    updated_count = 0
                    row_count = target_rows_before
                else:
                    slice_frame = _build_hour_slice(spark, product_name, dirty)
                    if not isinstance(slice_frame, DataFrame):
                        raise TypeError(
                            f"Builder for {product_name!r} must return a Spark DataFrame."
                        )
                    conflicts = sorted(
                        set(slice_frame.columns).intersection(RESERVED_METADATA_COLUMNS)
                    )
                    if conflicts:
                        raise ValueError(
                            f"Product {product_name!r} uses reserved columns: {conflicts}"
                        )
                    refreshed_at = _utc_now()
                    enriched = _enrich_product(
                        slice_frame,
                        created_at_frame=created_at_frame,
                        source_version=source_version,
                        refreshed_at=refreshed_at,
                        schema_version=schema_version,
                    )
                    _assert_unique_keys(enriched, keys, product_name)
                    existing_keys = (
                        spark.read.format("delta")
                        .load(str(output_path))
                        .select(HOUR_KEY)
                        .distinct()
                    )
                    new_keys = enriched.select(HOUR_KEY).join(
                        existing_keys, HOUR_KEY, "left_anti"
                    )
                    inserted_count = new_keys.count()
                    updated_count = enriched.count() - inserted_count
                    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
                    (
                        DeltaTable.forPath(spark, str(output_path))
                        .alias("t")
                        .merge(enriched.alias("s"), f"t.{HOUR_KEY} = s.{HOUR_KEY}")
                        .whenMatchedUpdateAll()
                        .whenNotMatchedInsertAll()
                        .execute()
                    )
                    written = spark.read.format("delta").load(str(output_path))
                    row_count = written.count()
                    if target_rows_before + inserted_count != row_count:
                        raise RuntimeError(
                            f"Incremental row invariant failed for {product_name!r}: "
                            f"before={target_rows_before} inserted={inserted_count} "
                            f"after={row_count}."
                        )
                    _assert_unique_keys(written, keys, product_name)
                stats = product_table_stats(spark, output_path)
                data_bytes = stats["data_bytes"]
                data_files = stats["data_files"]
                output_version = stats["delta_version"]
                status = "success"

        if effective_mode == "full":
            product = builder(spark)
            if not isinstance(product, DataFrame):
                raise TypeError(f"Builder for {product_name!r} must return a Spark DataFrame.")
            conflicts = sorted(set(product.columns).intersection(RESERVED_METADATA_COLUMNS))
            if conflicts:
                raise ValueError(f"Product {product_name!r} uses reserved columns: {conflicts}")
            refreshed_at = _utc_now()
            enriched = _enrich_product(
                product,
                created_at_frame=created_at_frame,
                source_version=source_version,
                refreshed_at=refreshed_at,
                schema_version=schema_version,
            )
            _assert_unique_keys(enriched, keys, product_name)
            row_count = enriched.count()
            inserted_count = row_count
            updated_count = 0
            target_rows_before = None
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
            _assert_unique_keys(written, keys, product_name)
            stats = product_table_stats(spark, output_path)
            data_bytes = stats["data_bytes"]
            data_files = stats["data_files"]
            output_version = stats["delta_version"]
            status = "success"
    except Exception as caught:
        error = caught
        error_message = f"{type(caught).__name__}: {caught}"
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
            "inserted_count": inserted_count,
            "updated_count": updated_count,
            "target_rows_before": target_rows_before,
            "data_bytes": data_bytes,
            "data_files": data_files,
            "schema_version": schema_version,
            "status": status,
            "error_message": error_message,
            "refresh_mode": effective_mode,
        }
        source_versions = {"integrated_taxi_trips": source_version}
        source_versions.update({
            f"standardized_{name}": int(value)
            for name, value in snapshot.get("standardized_versions", {}).items()
        })
        row = run_row(
            run_id=current_run_id,
            stage="product_refresh",
            target=product_name,
            mode=effective_mode,
            started_at=started_at,
            execution_seconds=record["execution_seconds"],
            status=status,
            error=error,
            inserted_count=inserted_count,
            updated_count=updated_count,
            target_rows_before=target_rows_before,
            target_rows_after=row_count,
            schema_version=record["schema_version"],
            source_versions=source_versions,
            output_version=output_version,
            output_bytes=data_bytes,
            output_files=data_files,
        )
        # On the failure path the product error keeps propagating (see record_run).
        record_run(spark, row, delta_root, enabled=monitoring, error=error)
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
    run_id: str | None = None,
    monitoring: bool = True,
    mode: str = "full",
) -> list[dict[str, Any]]:
    """Refresh configured products from one registered integrated snapshot.

    ``mode='full'`` rebuilds every selected product. ``mode='auto'`` skips products
    already built from the registered integrated version (they get a skipped
    monitoring row). The others are refreshed: hour-grained products in
    :data:`INCREMENTAL_HOUR_PRODUCTS` by a dirty-key MERGE, the rest fully.
    """
    if mode not in ("full", "auto"):
        raise ValueError("mode must be 'full' or 'auto'")
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
    run_id = run_id or str(uuid4())
    products_root = Path(output_root) if output_root is not None else Path(delta_root) / "analytics"
    current_version = int(snapshot["integrated_version"])
    records: list[dict[str, Any]] = []
    for name in names:
        built_from = None if mode == "full" else _built_from_version(spark, products_root / name)
        if built_from is None:
            refresh_mode = "full"
        elif built_from == current_version:
            refresh_mode = "skip"
        elif name in INCREMENTAL_HOUR_PRODUCTS:
            refresh_mode = "incremental"
        else:
            refresh_mode = "full"
        if refresh_mode == "skip":
            started_at = _utc_now()
            row = run_row(
                run_id=run_id,
                stage="product_refresh",
                target=name,
                mode="skip",
                started_at=started_at,
                execution_seconds=0.0,
                status="skipped",
                schema_version=str(configured[name]["schema_version"]),
            )
            record_run(spark, row, delta_root, enabled=monitoring)
            records.append({
                "run_id": run_id,
                "product_name": name,
                "status": "skipped",
                "refresh_mode": "skip",
                "row_count": None,
                "schema_version": str(configured[name]["schema_version"]),
            })
            continue
        record = refresh_product(
            spark,
            name,
            resolved_builders[name],
            snapshot=snapshot,
            settings=configured[name],
            delta_root=delta_root,
            output_root=products_root,
            run_id=run_id,
            monitoring=monitoring,
            refresh_mode=refresh_mode,
        )
        records.append(record)
    return records


def _trips(spark: SparkSession) -> DataFrame:
    return spark.table("integrated_taxi_trips")


def _nyc_trips(spark: SparkSession) -> DataFrame:
    """Environment products use the same NYC scope as Q2-Q4."""
    return _trips(spark).where(F.col("environment_in_scope"))


def build_daily_mobility_summary(
    spark: SparkSession,
    trips: DataFrame | None = None,
) -> DataFrame:
    """Local date/hour metrics keyed by the actual UTC hour."""
    base = trips if trips is not None else _trips(spark)
    framed = (
        base.withColumn("local_pickup_ts", _local_timestamp("pickup_hour_utc"))
        .withColumn("local_pickup_date", F.to_date("local_pickup_ts"))
        .withColumn("local_pickup_hour", F.hour("local_pickup_ts"))
        .withColumn("local_weekday", F.date_format("local_pickup_ts", "EEEE"))
    )
    return framed.groupBy("pickup_hour_utc").agg(
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


def build_air_quality_impact_summary(
    spark: SparkSession,
    trips: DataFrame | None = None,
) -> DataFrame:
    """NYC demand per UTC hour with the matched hourly PM2.5, the same population as Q3."""
    base = trips if trips is not None else _nyc_trips(spark)
    framed = (
        base.withColumn("local_pickup_ts", _local_timestamp("pickup_hour_utc"))
        .withColumn("local_pickup_date", F.to_date("local_pickup_ts"))
    )
    return (
        framed.groupBy("pickup_hour_utc")
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
