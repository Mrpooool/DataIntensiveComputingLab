from __future__ import annotations

from dataclasses import dataclass, field
from time import perf_counter
from typing import Any, Mapping
from uuid import uuid4

from pyspark import StorageLevel
from pyspark.sql import DataFrame, functions as F

from .schemas import REQUIRED_RAW_COLUMNS
from .transforms import transform_dataset
from .validation import mark_duplicate_rows, validate_dataset


class SchemaValidationError(ValueError):
    """Raised when a source is missing fields required by its data contract."""


@dataclass(frozen=True)
class PreparationResult:
    accepted: DataFrame
    rejected: DataFrame
    metrics: dict[str, Any]
    _classified_cache: DataFrame = field(repr=False)

    def release(self) -> None:
        """Release the shared cache after accepted and rejected outputs are written."""
        self._classified_cache.unpersist()


def _validate_raw_columns(df: DataFrame, dataset: str) -> None:
    if dataset not in REQUIRED_RAW_COLUMNS:
        raise KeyError(f"Unknown dataset {dataset!r}")
    missing = sorted(set(REQUIRED_RAW_COLUMNS[dataset]) - set(df.columns))
    if missing:
        raise SchemaValidationError(
            f"{dataset} is missing required source columns: {', '.join(missing)}"
        )


def _apply_scope(
    df: DataFrame, dataset: str, config: Mapping[str, Any]
) -> tuple[DataFrame, int | None]:
    if dataset != "air_quality":
        return df, None
    raw_input_count = df.count()
    counties = [str(value) for value in config["scope_counties"]]
    scoped = df.where(
        (F.col("State Name") == str(config["scope_state"]))
        & F.col("County Name").isin(counties)
    )
    return scoped, raw_input_count


def _attach_audit_columns(df: DataFrame, run_id: str, config: Mapping[str, Any]) -> DataFrame:
    raw_columns = list(df.columns)
    discovered_file = F.input_file_name()
    return (
        df.withColumn("raw_record_json", F.to_json(F.struct(*[F.col(name) for name in raw_columns])))
        .withColumn(
            "source_file",
            F.when(F.length(discovered_file) > 0, discovered_file).otherwise(
                F.lit(str(config.get("source_label", "in_memory")))
            ),
        )
        .withColumn("run_id", F.lit(run_id))
        .withColumn("schema_version", F.lit(str(config["schema_version"])))
        .withColumn("rule_version", F.lit(str(config["rule_version"])))
    )


def _count_codes(df: DataFrame, column: str) -> dict[str, int]:
    rows = (
        df.select(F.explode(F.col(column)).alias("code"))
        .groupBy("code")
        .count()
        .collect()
    )
    return {str(row["code"]): int(row["count"]) for row in rows}


def prepare(
    df: DataFrame,
    dataset_config: Mapping[str, Any],
    *,
    run_id: str | None = None,
) -> PreparationResult:
    """Standardize, validate and de-duplicate one raw Spark DataFrame.

    Air Quality is first restricted to the five NYC counties. These national rows are
    counted as scope exclusions, not rejected data-quality records.
    """
    started = perf_counter()
    dataset = str(dataset_config["dataset"])
    current_run_id = run_id or str(uuid4())
    _validate_raw_columns(df, dataset)

    scoped, raw_input_count = _apply_scope(df, dataset, dataset_config)
    audited = _attach_audit_columns(scoped, current_run_id, dataset_config)
    transformed = transform_dataset(audited, dataset, dataset_config) #统一列名和时间，生成行程时长、关联用的小时字段等
    classified = validate_dataset(transformed, dataset, dataset_config) #检查时间倒序、非法数值、缺失关键字段等，记录错误原因
    classified = mark_duplicate_rows(classified, dataset_config["duplicate_key"]) #按各数据集的业务键识别重复记录
    # Full Taxi rows include raw JSON; keep the shared cache off the JVM heap.
    classified.persist(StorageLevel.DISK_ONLY)

    in_scope_input_count = classified.count()
    error_counts = _count_codes(classified, "error_reasons")
    quality_flag_counts = _count_codes(classified, "quality_flags")
    rejected_count = classified.where(F.size("error_reasons") > 0).count()
    accepted_count = in_scope_input_count - rejected_count

    accepted = classified.where(F.size("error_reasons") == 0).drop(
        "raw_record_json", "error_reasons"
    )
    rejected = classified.where(F.size("error_reasons") > 0)

    if raw_input_count is None:
        raw_input_count = in_scope_input_count
    excluded_count = raw_input_count - in_scope_input_count
    metrics = {
        "run_id": current_run_id,
        "dataset": dataset,
        "schema_version": str(dataset_config["schema_version"]),
        "rule_version": str(dataset_config["rule_version"]),
        "raw_input_count": raw_input_count,
        "scope_excluded_count": excluded_count,
        "input_count": in_scope_input_count,
        "accepted_count": accepted_count,
        "rejected_count": rejected_count,
        "duplicate_count": error_counts.get("duplicate_record", 0),
        "error_counts": error_counts,
        "quality_flag_counts": quality_flag_counts,
        "processing_seconds": perf_counter() - started,
        "status": "success",
    }
    return PreparationResult(
        accepted=accepted,
        rejected=rejected,
        metrics=metrics,
        _classified_cache=classified,
    )
