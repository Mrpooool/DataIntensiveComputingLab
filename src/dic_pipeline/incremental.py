"""Week 3 incremental updates: generate update files and apply them to Delta tables.

Contract: ``docs/w3_interfaces.md`` (role A). Reuses ``prepare`` for validation and
standardization; merges into existing standardized tables without a full rebuild.
"""

from __future__ import annotations

import csv
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import perf_counter
from typing import Any, Mapping, Sequence
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import DoubleType, StructField, StructType

from .contracts import load_dataset_config
from .ingestion import (
    DATASETS,
    DEFAULT_DELTA_ROOT,
    load_completed_batch,
)
from .integration import (
    INTEGRATED_TABLE,
    integrate,
    publish_integration_snapshot,
)
from .monitoring import delta_output_stats, record_run, run_row, utc_now
from .preparation import SchemaValidationError, prepare
from .schemas import RAW_SCHEMAS, REQUIRED_RAW_COLUMNS
from .validation import check_schema


UPDATE_DATASETS = ("taxi", "weather", "air_quality")
BATCH_MANIFEST = "metadata/completed_batch.json"

# Assignment defaults for the public generate API / CLI (Task 1).
# Taxi: 5–10% new trips, 1–2% copied duplicates. Weather/Air: new hours after the source.
TAXI_NEW_FRACTION = 0.07
TAXI_DUP_FRACTION = 0.015
DEFAULT_NEW_HOURS = 24 * 7


def _parse_utc(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def _format_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pending = path.with_suffix(path.suffix + ".tmp")
    pending.write_text(json.dumps(dict(payload), indent=2, default=str) + "\n", encoding="utf-8")
    pending.replace(path)


def _standardized(spark: SparkSession, delta_root: Path, dataset: str) -> DataFrame:
    path = delta_root / "standardized" / dataset
    if not (path / "_delta_log").exists():
        raise RuntimeError(f"Missing standardized table for {dataset}: {path}")
    return spark.read.format("delta").load(str(path))


def _table_version(spark: SparkSession, path: Path) -> int:
    return int(DeltaTable.forPath(spark, str(path)).history(1).first()["version"])


def _ensure_delta_columns(spark: SparkSession, path: Path, columns: Sequence[tuple[str, str]]) -> None:
    """Add nullable columns so MERGE can insert evolved rows (Windows-safe absolute path)."""
    if not (path / "_delta_log").exists() or not columns:
        return
    table = spark.read.format("delta").load(str(path))
    existing = {field.name for field in table.schema.fields}
    additions = [f"{name} {dtype}" for name, dtype in columns if name not in existing]
    if not additions:
        return
    # Relative paths break ALTER TABLE on Windows (parsed as catalog.schema.table).
    abs_path = path.resolve().as_posix()
    spark.sql(f"ALTER TABLE delta.`{abs_path}` ADD COLUMNS ({', '.join(additions)})")


def read_update_source(
    spark: SparkSession,
    dataset: str,
    path: str | Path,
    *,
    schema_changes: Sequence[Mapping[str, Any]] | None = None,
) -> DataFrame:
    """Read an update file; evolved CSV columns are accepted then attached."""
    del schema_changes
    path = Path(path)
    config = load_dataset_config(dataset)
    fmt = str(config["source_format"])
    if fmt == "parquet":
        frame = spark.read.format("parquet").load(str(path))
        _accepted, unsupported = check_schema(
            frame.schema,
            RAW_SCHEMAS[dataset],
            policy=config.get("schema_evolution"),
        )
        if unsupported:
            raise SchemaValidationError(
                f"{dataset} update has unsupported schema changes: {unsupported}"
            )
        return frame

    expected = list(REQUIRED_RAW_COLUMNS[dataset])
    with path.open(encoding="utf-8", newline="") as handle:
        header = next(csv.reader(handle))
    header = [name.strip() for name in header]
    accepted, unsupported = check_schema(
        header,
        expected,
        policy=config.get("schema_evolution"),
    )
    if unsupported:
        raise SchemaValidationError(
            f"{dataset} update has unsupported schema changes: {unsupported}"
        )
    base_schema = RAW_SCHEMAS[dataset]
    extra_fields = []
    for change in accepted:
        if change.get("op") != "add":
            continue
        name = str(change["column"])
        if name not in base_schema.fieldNames():
            extra_fields.append(StructField(name, DoubleType(), True))
    read_schema = StructType(list(base_schema.fields) + extra_fields) if extra_fields else base_schema
    reader = (
        spark.read.format("csv")
        .schema(read_schema)
        .option("header", "true")
        .option("enforceSchema", "false")
    )
    for key, value in config.get("reader_options", {}).items():
        if key == "header":
            continue
        reader = reader.option(str(key), str(value))
    frame = reader.load(str(path))
    _accepted, unsupported = check_schema(
        frame.schema,
        RAW_SCHEMAS[dataset],
        policy=config.get("schema_evolution"),
    )
    if unsupported:
        raise SchemaValidationError(
            f"{dataset} update has unsupported schema changes: {unsupported}"
        )
    return frame


def generate_update(
    spark: SparkSession,
    dataset: str,
    out_dir: str | Path,
    *,
    delta_root: str | Path = DEFAULT_DELTA_ROOT,
    seed: int = 0,
    new_fraction: float | None = None,
    duplicate_fraction: float | None = None,
    new_hours: int | None = None,
) -> dict[str, Any]:
    """Write one update file for ``taxi``, ``weather`` or ``air_quality`` (deterministic).

    Default sizes match Assignment Task 1 (Taxi 7% new / 1.5% duplicates; Weather and
    Air Quality seven days of new hours with schema evolution). The optional
    ``new_fraction``, ``duplicate_fraction`` and ``new_hours`` arguments are only for
    small fixture tests; the CLI never passes them.
    """
    if dataset not in UPDATE_DATASETS:
        raise KeyError(f"Unsupported update dataset {dataset!r}")
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    root = Path(delta_root)
    rng = random.Random(seed)
    taxi_new = TAXI_NEW_FRACTION if new_fraction is None else float(new_fraction)
    taxi_dup = TAXI_DUP_FRACTION if duplicate_fraction is None else float(duplicate_fraction)
    hours = DEFAULT_NEW_HOURS if new_hours is None else int(new_hours)
    if not 0 < taxi_new <= 1 or not 0 < taxi_dup <= 1:
        raise ValueError("new_fraction and duplicate_fraction must be in (0, 1].")
    if hours < 1:
        raise ValueError("new_hours must be at least 1.")
    # Guard the public defaults: CLI / evaluation must stay inside the assignment bands.
    if new_fraction is None and not 0.05 <= TAXI_NEW_FRACTION <= 0.10:
        raise RuntimeError("TAXI_NEW_FRACTION must stay within the assignment 5–10% band.")
    if duplicate_fraction is None and not 0.01 <= TAXI_DUP_FRACTION <= 0.02:
        raise RuntimeError("TAXI_DUP_FRACTION must stay within the assignment 1–2% band.")
    if dataset == "taxi":
        return _generate_taxi_update(
            spark, root, out_dir, rng, new_fraction=taxi_new, duplicate_fraction=taxi_dup
        )
    if dataset == "weather":
        return _generate_weather_update(spark, root, out_dir, rng, new_hours=hours)
    return _generate_air_update(spark, root, out_dir, rng, new_hours=hours)


def _generate_taxi_update(
    spark: SparkSession,
    delta_root: Path,
    out_dir: Path,
    rng: random.Random,
    *,
    new_fraction: float,
    duplicate_fraction: float,
) -> dict[str, Any]:
    """Build a TLC-shaped Parquet update in Spark (no driver collect)."""
    taxi = _standardized(spark, delta_root, "taxi")
    total = taxi.count()
    if total < 1:
        raise RuntimeError("Cannot generate a Taxi update from an empty standardized table.")
    max_pickup = _parse_utc(taxi.agg(F.max("pickup_timestamp_utc")).first()[0])
    new_count = max(1, min(total, int(round(total * new_fraction))))
    dup_count = max(1, min(total, int(round(total * duplicate_fraction))))
    seed_new = rng.randint(0, 10_000)
    seed_dup = rng.randint(0, 10_000)

    # Slight oversample then limit so counts match the requested percentages.
    # Tiny fixture tables can yield an empty Bernoulli sample; fall back to a seeded limit.
    def _take(frame: DataFrame, fraction: float, seed: int, limit: int) -> DataFrame:
        sampled = frame.sample(False, min(1.0, fraction * 1.25), seed).limit(limit)
        if sampled.limit(1).count() == 0:
            return frame.orderBy(F.rand(seed)).limit(limit)
        return sampled

    sample_new = _take(taxi, new_fraction, seed_new, new_count)
    sample_dup = _take(taxi, duplicate_fraction, seed_dup, dup_count)
    sample_min = sample_new.agg(F.min("pickup_timestamp_utc")).first()[0]
    if sample_min is None:
        raise RuntimeError("Taxi new-trip sample is empty.")
    # Shift by whole weeks (keeps weekday and time of day), one week more than the
    # sample's distance to the original maximum, so every new pickup comes after it.
    weeks = (max_pickup - _parse_utc(sample_min)) // timedelta(weeks=1) + 1
    shift_secs = int(timedelta(weeks=weeks).total_seconds())
    shifted = (
        sample_new
        .withColumn(
            "pickup_timestamp_utc",
            F.col("pickup_timestamp_utc") + F.expr(f"INTERVAL {shift_secs} SECONDS"),
        )
        .withColumn(
            "dropoff_timestamp_utc",
            F.col("dropoff_timestamp_utc") + F.expr(f"INTERVAL {shift_secs} SECONDS"),
        )
    )
    new_raw = _taxi_standardized_to_raw(shifted, use_source_wall_time=False)
    dup_raw = _taxi_standardized_to_raw(sample_dup, use_source_wall_time=True)
    actual_new = new_raw.count()
    actual_dup = dup_raw.count()
    if actual_new < 1 or actual_dup < 1:
        raise RuntimeError(
            f"Taxi update sample too small after sampling (new={actual_new}, dup={actual_dup})."
        )
    frame = new_raw.unionByName(dup_raw)
    path = out_dir / "taxi_trips_update.parquet"
    # Multi-file write keeps full-scale updates off a single driver-sized part.
    frame.repartition(8).write.mode("overwrite").parquet(str(path))
    end_pickup = _parse_utc(shifted.agg(F.max("pickup_timestamp_utc")).first()[0])
    window_end = end_pickup.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    config = load_dataset_config("taxi")
    return {
        "dataset": "taxi",
        "path": str(path),
        "new_count": actual_new,
        "duplicate_count": actual_dup,
        "schema_changes": [],
        "valid_pickup_start_utc": str(config["valid_pickup_start_utc"]),
        "valid_pickup_end_utc_exclusive": _format_utc(window_end),
        "source_max_pickup_utc": _format_utc(max_pickup),
        "new_fraction_target": new_fraction,
        "duplicate_fraction_target": duplicate_fraction,
        "base_row_count": total,
    }


def _taxi_standardized_to_raw(frame: DataFrame, *, use_source_wall_time: bool) -> DataFrame:
    """Project standardized Taxi rows onto the TLC raw Parquet schema."""
    if use_source_wall_time:
        pickup = F.col("pickup_timestamp_source")
        dropoff = F.col("dropoff_timestamp_source")
    else:
        pickup = F.from_utc_timestamp("pickup_timestamp_utc", "America/New_York")
        dropoff = F.from_utc_timestamp("dropoff_timestamp_utc", "America/New_York")
    return frame.select(
        F.col("vendor_id").cast("int").alias("VendorID"),
        pickup.alias("tpep_pickup_datetime"),
        dropoff.alias("tpep_dropoff_datetime"),
        F.col("passenger_count").cast("long").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("ratecode_id").cast("long").alias("RatecodeID"),
        F.col("store_and_fwd_flag").cast("string").alias("store_and_fwd_flag"),
        F.col("pickup_location_id").cast("int").alias("PULocationID"),
        F.col("dropoff_location_id").cast("int").alias("DOLocationID"),
        F.col("payment_type").cast("long").alias("payment_type"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("extra").cast("double").alias("extra"),
        F.col("mta_tax").cast("double").alias("mta_tax"),
        F.col("tip_amount").cast("double").alias("tip_amount"),
        F.col("tolls_amount").cast("double").alias("tolls_amount"),
        F.col("improvement_surcharge").cast("double").alias("improvement_surcharge"),
        F.col("total_amount").cast("double").alias("total_amount"),
        F.col("congestion_surcharge").cast("double").alias("congestion_surcharge"),
        F.col("airport_fee").cast("double").alias("Airport_fee"),
    )


def _taxi_raw_from_standardized(
    row: Any,
    pickup: datetime,
    dropoff: datetime,
    *,
    duplicate: bool,
) -> dict[str, Any]:
    """Legacy row-wise helper kept for debugging; full-scale path uses Spark projection."""
    as_dict = row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
    if duplicate and as_dict.get("pickup_timestamp_source") is not None:
        pickup_local = as_dict["pickup_timestamp_source"]
        dropoff_local = as_dict["dropoff_timestamp_source"]
    else:
        from zoneinfo import ZoneInfo

        ny = ZoneInfo("America/New_York")
        pickup_local = pickup.astimezone(ny).replace(tzinfo=None)
        dropoff_local = dropoff.astimezone(ny).replace(tzinfo=None)
    return {
        "VendorID": int(as_dict["vendor_id"]) if as_dict.get("vendor_id") is not None else 2,
        "tpep_pickup_datetime": pickup_local,
        "tpep_dropoff_datetime": dropoff_local,
        "passenger_count": int(as_dict.get("passenger_count") or 1),
        "trip_distance": float(as_dict.get("trip_distance") or 1.0),
        "RatecodeID": int(as_dict.get("ratecode_id") or 1),
        "store_and_fwd_flag": str(as_dict.get("store_and_fwd_flag") or "N"),
        "PULocationID": int(as_dict["pickup_location_id"]),
        "DOLocationID": int(as_dict["dropoff_location_id"]),
        "payment_type": int(as_dict.get("payment_type") or 1),
        "fare_amount": float(as_dict.get("fare_amount") or 10.0),
        "extra": float(as_dict.get("extra") or 0.0),
        "mta_tax": float(as_dict.get("mta_tax") or 0.5),
        "tip_amount": float(as_dict.get("tip_amount") or 0.0),
        "tolls_amount": float(as_dict.get("tolls_amount") or 0.0),
        "improvement_surcharge": float(as_dict.get("improvement_surcharge") or 1.0),
        "total_amount": float(as_dict.get("total_amount") or 10.0),
        "congestion_surcharge": float(as_dict.get("congestion_surcharge") or 0.0),
        "Airport_fee": float(as_dict.get("airport_fee") or 0.0),
    }


def _generate_weather_update(
    spark: SparkSession,
    delta_root: Path,
    out_dir: Path,
    rng: random.Random,
    *,
    new_hours: int,
) -> dict[str, Any]:
    weather = _standardized(spark, delta_root, "weather")
    max_hour = weather.agg(F.max("weather_hour_utc")).first()[0]
    max_hour = _parse_utc(max_hour)
    template = weather.orderBy(F.desc("weather_hour_utc")).limit(1).collect()[0]
    hours = new_hours
    path = out_dir / "weather_update.csv"
    fieldnames = list(REQUIRED_RAW_COLUMNS["weather"]) + ["humidity"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        base_rhum = float(template["rhum"] if template["rhum"] is not None else 60.0)
        for index in range(hours):
            hour = max_hour + timedelta(hours=index + 1)
            # rhum already is relative humidity; the new column must agree with it.
            humidity = round(min(100.0, max(20.0, base_rhum + rng.uniform(-10.0, 10.0))), 1)
            writer.writerow({
                "year": hour.year,
                "month": hour.month,
                "day": hour.day,
                "hour": hour.hour,
                "temp": float(template["temp"] if template["temp"] is not None else 5.0)
                + rng.uniform(-1.0, 1.0),
                "temp_source": template["temp_source"] or "isd_lite",
                "rhum": humidity,
                "rhum_source": template["rhum_source"] or "isd_lite",
                "prcp": 0.0,
                "prcp_source": "isd_lite",
                "snwd": "",
                "snwd_source": "",
                "wdir": float(template["wdir"] if template["wdir"] is not None else 180.0),
                "wdir_source": template["wdir_source"] or "isd_lite",
                "wspd": float(template["wspd"] if template["wspd"] is not None else 5.0),
                "wspd_source": template["wspd_source"] or "isd_lite",
                "wpgt": "",
                "wpgt_source": "",
                "pres": float(template["pres"] if template["pres"] is not None else 1010.0),
                "pres_source": template["pres_source"] or "isd_lite",
                "cldc": int(template["cldc"] if template["cldc"] is not None else 4),
                "cldc_source": template["cldc_source"] or "isd_lite",
                "coco": int(template["coco"] if template["coco"] is not None else 3),
                "coco_source": template["coco_source"] or "metar",
                "humidity": humidity,
            })
    end = max_hour + timedelta(hours=hours + 1)
    return {
        "dataset": "weather",
        "path": str(path),
        "new_count": hours,
        "duplicate_count": 0,
        "schema_changes": [
            {"op": "add", "column": "humidity", "type": "double", "nullable": True},
        ],
        "valid_pickup_start_utc": None,
        "valid_pickup_end_utc_exclusive": _format_utc(end),
        "source_max_hour_utc": _format_utc(max_hour),
    }


def _generate_air_update(
    spark: SparkSession,
    delta_root: Path,
    out_dir: Path,
    rng: random.Random,
    *,
    new_hours: int,
) -> dict[str, Any]:
    air = _standardized(spark, delta_root, "air_quality")
    max_hour = air.agg(F.max("air_quality_hour_utc")).first()[0]
    max_hour = _parse_utc(max_hour)
    template = air.orderBy(F.desc("air_quality_hour_utc")).limit(1).collect()[0]
    hours = new_hours
    path = out_dir / "air_quality_update.csv"
    fieldnames = list(REQUIRED_RAW_COLUMNS["air_quality"]) + ["aqi"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for index in range(hours):
            hour = max_hour + timedelta(hours=index + 1)
            # Date Local approximates NYC; GMT matches the UTC hour used by the contract.
            local = hour - timedelta(hours=5)
            writer.writerow({
                "State Code": template["state_code"],
                "County Code": template["county_code"],
                "Site Num": template["site_num"],
                "Parameter Code": template["parameter_code"],
                "POC": int(template["poc"]),
                "Latitude": float(template["latitude"] or 40.7),
                "Longitude": float(template["longitude"] or -74.0),
                "Datum": "NAD83",
                "Parameter Name": "PM2.5 - Local Conditions",
                "Date Local": local.strftime("%Y-%m-%d"),
                "Time Local": local.strftime("%H:%M"),
                "Date GMT": hour.strftime("%Y-%m-%d"),
                "Time GMT": hour.strftime("%H:%M"),
                "Sample Measurement": float(template["measurement_value"] or 8.0)
                + rng.uniform(-1.0, 1.0),
                "Units of Measure": "Micrograms/cubic meter (LC)",
                "MDL": 0.5,
                "Uncertainty": "",
                "Qualifier": "",
                "Method Type": template["method_type"] or "FEM",
                "Method Code": template["method_code"],
                "Method Name": "Incremental monitor",
                "State Name": "New York",
                "County Name": "New York",
                "Date of Last Change": hour.strftime("%Y-%m-%d"),
                "aqi": round(rng.uniform(0, 150), 1),
            })
    end = max_hour + timedelta(hours=hours + 1)
    return {
        "dataset": "air_quality",
        "path": str(path),
        "new_count": hours,
        "duplicate_count": 0,
        "schema_changes": [
            {"op": "add", "column": "aqi", "type": "double", "nullable": True},
        ],
        "valid_pickup_start_utc": None,
        "valid_pickup_end_utc_exclusive": _format_utc(end),
        "source_max_hour_utc": _format_utc(max_hour),
    }


def _merge_accepted(
    spark: SparkSession,
    dataset: str,
    accepted: DataFrame,
    target_path: Path,
) -> tuple[int, int, int]:
    """MERGE accepted rows; return (inserted, updated, duplicate)."""
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    before = spark.read.format("delta").load(str(target_path)).count()
    source_count = accepted.count()
    if dataset == "weather" and "humidity" in accepted.columns:
        _ensure_delta_columns(spark, target_path, [("humidity", "double")])
    if dataset == "air_quality" and "aqi" in accepted.columns:
        _ensure_delta_columns(spark, target_path, [("aqi", "double")])

    target = DeltaTable.forPath(spark, str(target_path))
    key = {
        "taxi": "t.record_id = s.record_id",
        "weather": "t.weather_hour_utc = s.weather_hour_utc",
        "air_quality": (
            "t.state_code = s.state_code AND t.county_code = s.county_code AND "
            "t.site_num = s.site_num AND t.parameter_code = s.parameter_code AND "
            "t.poc = s.poc AND t.air_quality_hour_utc = s.air_quality_hour_utc AND "
            "t.method_code = s.method_code"
        ),
    }[dataset]
    merge = target.alias("t").merge(accepted.alias("s"), key)
    if dataset == "taxi":
        merge.whenNotMatchedInsertAll().execute()
        after = spark.read.format("delta").load(str(target_path)).count()
        inserted = after - before
        updated = 0
        duplicate = source_count - inserted
    else:
        merge.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        after = spark.read.format("delta").load(str(target_path)).count()
        inserted = after - before
        # Matched rows rewrite in place; treat the remainder of the source as updates.
        updated = max(source_count - inserted, 0)
        duplicate = 0
    return inserted, updated, max(duplicate, 0)


def _append_rejected(rejected: DataFrame, path: Path) -> None:
    if rejected.limit(1).count() == 0:
        return
    (
        rejected.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(str(path))
    )


def _append_integrated(
    spark: SparkSession,
    delta_root: Path,
    new_taxi: DataFrame,
) -> dict[str, Any]:
    """Enrich only the newly inserted taxi rows and append them to the integrated table."""
    if new_taxi.limit(1).count() == 0:
        return {"inserted": 0}
    weather = spark.read.format("delta").load(str(delta_root / "standardized" / "weather"))
    air = spark.read.format("delta").load(str(delta_root / "standardized" / "air_quality"))
    zones = spark.read.format("delta").load(str(delta_root / "standardized" / "taxi_zones"))
    # Drop evolved columns that the integrator does not select, if present.
    weather_cols = [c for c in weather.columns if c != "humidity"]
    air_cols = [c for c in air.columns if c != "aqi"]
    integrated, stats = integrate(new_taxi, weather.select(*weather_cols), air.select(*air_cols), zones)
    output = delta_root / INTEGRATED_TABLE
    (
        integrated.repartition("pickup_date")
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("pickup_date")
        .save(str(output))
    )
    return {"inserted": int(stats["output_count"]), "stats": stats}


def apply_updates(
    spark: SparkSession,
    updates: Sequence[Mapping[str, Any]],
    *,
    delta_root: str | Path = DEFAULT_DELTA_ROOT,
    run_id: str | None = None,
    validate: bool = True,
    monitoring: bool = True,
) -> list[dict[str, Any]]:
    """Apply update manifests to standardized / rejected / integrated tables and republish."""
    root = Path(delta_root)
    current_run_id = run_id or str(uuid4())
    batch = load_completed_batch(root)
    lineage = list(batch.get("lineage") or [batch["run_id"]])
    if lineage[-1] != batch["run_id"]:
        lineage.append(batch["run_id"])

    # Extend the window a previous update published, not the configured one.
    coverage = batch.get("coverage_window") or load_dataset_config("taxi")
    coverage_start = str(coverage["valid_pickup_start_utc"])
    coverage_end = str(coverage["valid_pickup_end_utc_exclusive"])
    records: list[dict[str, Any]] = []
    integrated_before = spark.read.format("delta").load(str(root / INTEGRATED_TABLE)).count()

    for update in updates:
        dataset = str(update["dataset"])
        if dataset == "taxi_zones":
            continue
        if dataset not in UPDATE_DATASETS:
            raise KeyError(f"Unsupported update dataset {dataset!r}")
        started_at = utc_now()
        started = perf_counter()
        counts: dict[str, Any] = {
            "processed_count": 0,
            "inserted_count": 0,
            "updated_count": 0,
            "duplicate_count": 0,
            "rejected_count": 0,
            "scope_excluded_count": 0,
        }
        target_path = root / "standardized" / dataset
        target_before = spark.read.format("delta").load(str(target_path)).count()
        output_stats: dict[str, int] = {}
        schema_changes: list[dict[str, Any]] = []
        try:
            config = load_dataset_config(dataset)
            raw = read_update_source(
                spark,
                dataset,
                update["path"],
                schema_changes=update.get("schema_changes"),
            )
            schema_changes, unsupported = check_schema(
                raw.schema,
                RAW_SCHEMAS[dataset],
                policy=config.get("schema_evolution"),
            )
            if unsupported:
                raise SchemaValidationError(
                    f"{dataset} update has unsupported schema changes: {unsupported}"
                )
            if dataset == "taxi":
                end = update.get("valid_pickup_end_utc_exclusive") or coverage_end
                coverage_end = max(coverage_end, str(end))
                config = dict(config)
                config["valid_pickup_end_utc_exclusive"] = coverage_end
                if update.get("valid_pickup_start_utc"):
                    coverage_start = str(update["valid_pickup_start_utc"])
                    config["valid_pickup_start_utc"] = coverage_start
            if schema_changes:
                config = dict(config)
                config["schema_version"] = "1.1.0"
            reference_data: dict[str, Any] = {}
            if validate and dataset == "taxi":
                zone_rows = (
                    spark.read.format("delta")
                    .load(str(root / "standardized" / "taxi_zones"))
                    .select("location_id")
                    .where("location_id IS NOT NULL")
                    .collect()
                )
                reference_data["taxi_zone_ids"] = tuple(
                    sorted({int(row["location_id"]) for row in zone_rows})
                )
            result = prepare(
                raw,
                config,
                run_id=current_run_id,
                validate=validate,
                reference_data=reference_data,
            )
            metrics = dict(result.metrics)
            counts.update({
                "processed_count": metrics["input_count"],
                "rejected_count": metrics["rejected_count"],
                "scope_excluded_count": metrics.get("scope_excluded_count") or 0,
            })
            inserted, updated, duplicate = _merge_accepted(
                spark, dataset, result.accepted, target_path
            )
            counts.update({
                "inserted_count": inserted,
                "updated_count": updated,
                "duplicate_count": duplicate,
            })
            _append_rejected(result.rejected, root / "rejected" / dataset)
            result.release()
            if monitoring:
                output_stats = delta_output_stats(spark, target_path)
            target_after = spark.read.format("delta").load(str(target_path)).count()
            record = {
                "dataset": dataset,
                "run_id": current_run_id,
                **counts,
                "target_rows_before": target_before,
                "target_rows_after": target_after,
                "schema_version": config["schema_version"],
                "rule_version": config["rule_version"],
                "schema_changes": schema_changes,
                "status": "success",
            }
            records.append(record)
        except Exception as error:
            row = run_row(
                run_id=current_run_id,
                stage="incremental_update",
                target=dataset,
                mode="incremental",
                started_at=started_at,
                execution_seconds=perf_counter() - started,
                status="failed",
                error=error,
                target_rows_before=target_before,
                validation_enabled=validate,
                schema_changes=schema_changes,
                input_paths=[str(update.get("path"))],
                **counts,
            )
            record_run(spark, row, root, enabled=monitoring, error=error)
            raise
        row = run_row(
            run_id=current_run_id,
            stage="incremental_update",
            target=dataset,
            mode="incremental",
            started_at=started_at,
            execution_seconds=perf_counter() - started,
            status="success",
            processed_count=counts["processed_count"],
            inserted_count=counts["inserted_count"],
            updated_count=counts["updated_count"],
            duplicate_count=counts["duplicate_count"],
            rejected_count=counts["rejected_count"],
            scope_excluded_count=counts["scope_excluded_count"],
            target_rows_before=target_before,
            target_rows_after=record["target_rows_after"],
            validation_enabled=validate,
            validation_failure_counts=metrics.get("error_counts"),
            quality_flag_counts=metrics.get("quality_flag_counts"),
            schema_version=record["schema_version"],
            rule_version=record["rule_version"],
            schema_changes=schema_changes,
            input_paths=[str(update.get("path"))],
            **output_stats,
        )
        record_run(spark, row, root, enabled=monitoring)

    # Append-only integration of the standardized Taxi rows whose ingestion run the
    # integrated table has not seen: this run's inserts, plus those of an earlier
    # apply that failed after its MERGE (a rerun inserts nothing new itself).
    integ_started_at = utc_now()
    integ_started = perf_counter()
    try:
        integrated_runs = (
            spark.read.format("delta").load(str(root / INTEGRATED_TABLE))
            .select("run_id").distinct()
        )
        pending = _standardized(spark, root, "taxi").join(integrated_runs, "run_id", "left_anti")
        pending_runs = sorted(str(row.run_id) for row in pending.select("run_id").distinct().collect())
        integ_inserted = int(_append_integrated(spark, root, pending).get("inserted") or 0)
        earlier = [value for value in pending_runs if value != current_run_id and value not in lineage]
        lineage += [*earlier, current_run_id]
        versions = {
            name: _table_version(spark, root / "standardized" / name) for name in DATASETS
        }
        batch_payload = {
            "run_id": current_run_id,
            "lineage": lineage,
            "versions": versions,
            "coverage_window": {
                "valid_pickup_start_utc": coverage_start,
                "valid_pickup_end_utc_exclusive": coverage_end,
            },
        }
        _atomic_write_json(root / BATCH_MANIFEST, batch_payload)
        publish_integration_snapshot(spark, root, source_batch=batch_payload)
    except Exception as error:
        row = run_row(
            run_id=current_run_id,
            stage="incremental_update",
            target="integrated_taxi_trips",
            mode="incremental",
            started_at=integ_started_at,
            execution_seconds=perf_counter() - integ_started,
            status="failed",
            error=error,
            target_rows_before=integrated_before,
        )
        record_run(spark, row, root, enabled=monitoring, error=error)
        raise
    integrated_after = spark.read.format("delta").load(str(root / INTEGRATED_TABLE)).count()
    integ_output = delta_output_stats(spark, root / INTEGRATED_TABLE) if monitoring else {}
    row = run_row(
        run_id=current_run_id,
        stage="incremental_update",
        target="integrated_taxi_trips",
        mode="incremental",
        started_at=integ_started_at,
        execution_seconds=perf_counter() - integ_started,
        status="success",
        processed_count=integ_inserted,
        inserted_count=integ_inserted,
        updated_count=0,
        duplicate_count=0,
        rejected_count=0,
        target_rows_before=integrated_before,
        target_rows_after=integrated_after,
        **integ_output,
    )
    record_run(spark, row, root, enabled=monitoring)
    records.append({
        "dataset": "integrated_taxi_trips",
        "run_id": current_run_id,
        "inserted_count": integ_inserted,
        "target_rows_before": integrated_before,
        "target_rows_after": integrated_after,
        "status": "success",
    })
    return records
