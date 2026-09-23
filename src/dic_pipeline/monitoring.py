"""Week 3 platform monitoring: one Delta table of pipeline executions and its ops queries.

Every stage (ingestion, incremental update, integration, product refresh) writes
one row per target to ``<delta_root>/metadata/pipeline_runs``. Callers build the
row with :func:`run_row` and hand it to :func:`record_run` from their own
success and failure paths.

Count semantics (``docs/w3_interfaces.md`` is the contract for the other roles):

* ``processed_count``: in-scope input rows of this run.
* ``rejected_count``: rows written to ``rejected/`` (any code, including
  duplicates inside the same input).
* ``duplicate_count``: rows skipped because their key already exists in the
  target table (only incremental stages; a full overwrite has none).
* ``inserted_count`` / ``updated_count``: rows added / rewritten in the target.

For row-level stages ``processed = inserted + updated + duplicate + rejected``;
an incremental stage also keeps ``target_rows_after = target_rows_before + inserted``.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


PIPELINE_RUNS = "metadata/pipeline_runs"
SQL_DIRECTORY = Path(__file__).with_name("sql") / "monitoring"
STAGES = ("ingestion", "incremental_update", "integration", "product_refresh")
MODES = ("full", "incremental", "skip")
STATUSES = ("success", "failed", "skipped")
MAX_ERROR_LENGTH = 4000

PIPELINE_RUNS_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("stage", StringType(), False),
        StructField("target", StringType(), False),
        StructField("mode", StringType(), False),
        StructField("started_at", TimestampType(), False),
        StructField("finished_at", TimestampType(), False),
        StructField("execution_seconds", DoubleType(), False),
        StructField("processed_count", LongType(), True),
        StructField("inserted_count", LongType(), True),
        StructField("updated_count", LongType(), True),
        StructField("duplicate_count", LongType(), True),
        StructField("rejected_count", LongType(), True),
        StructField("scope_excluded_count", LongType(), True),
        StructField("target_rows_before", LongType(), True),
        StructField("target_rows_after", LongType(), True),
        StructField("validation_enabled", BooleanType(), True),
        StructField("validation_failure_counts_json", StringType(), True),
        StructField("quality_flag_counts_json", StringType(), True),
        StructField("schema_version", StringType(), True),
        StructField("rule_version", StringType(), True),
        StructField("schema_changes_json", StringType(), True),
        StructField("input_paths_json", StringType(), True),
        StructField("source_versions_json", StringType(), True),
        StructField("output_version", LongType(), True),
        StructField("output_bytes", LongType(), True),
        StructField("output_files", LongType(), True),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True),
    ]
)
# Keyword arguments of run_row that are stored as JSON text.
JSON_FIELDS = {
    "validation_failure_counts": "validation_failure_counts_json",
    "quality_flag_counts": "quality_flag_counts_json",
    "schema_changes": "schema_changes_json",
    "input_paths": "input_paths_json",
    "source_versions": "source_versions_json",
}
_COLUMNS = {field.name for field in PIPELINE_RUNS_SCHEMA.fields}
_STAGE_FIELDS = _COLUMNS - {
    "run_id", "stage", "target", "mode", "started_at", "finished_at",
    "execution_seconds", "status", "error_message", *JSON_FIELDS.values(),
}
METRIC_FIELDS = frozenset(_STAGE_FIELDS | set(JSON_FIELDS))
"""Keys a stage may pass to :func:`run_row`; also the counts contract for roles A and B."""


class MonitoringWriteError(RuntimeError):
    """The stage succeeded and its output is durable, but its monitoring row was not written."""

    def __init__(self, record: Mapping[str, Any]):
        super().__init__(
            f"Monitoring row for {record['stage']}/{record['target']} run {record['run_id']} "
            "was not written; the record was printed to stderr."
        )
        self.record = dict(record)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def run_row(
    *,
    run_id: str,
    stage: str,
    target: str,
    started_at: datetime,
    execution_seconds: float,
    status: str,
    mode: str = "full",
    error: BaseException | None = None,
    **metrics: Any,
) -> dict[str, Any]:
    """Build one ``pipeline_runs`` row; unknown metric names fail fast instead of vanishing."""
    if stage not in STAGES:
        raise ValueError(f"Unknown stage {stage!r}; expected one of {STAGES}")
    if mode not in MODES:
        raise ValueError(f"Unknown mode {mode!r}; expected one of {MODES}")
    if status not in STATUSES:
        raise ValueError(f"Unknown status {status!r}; expected one of {STATUSES}")
    unknown = sorted(set(metrics) - METRIC_FIELDS)
    if unknown:
        raise ValueError(f"Unknown monitoring fields {unknown}")
    # A naive datetime is read in the host's zone on its way into Spark (see findings.md).
    if started_at.tzinfo is None:
        raise ValueError("started_at must be timezone-aware")

    row: dict[str, Any] = {name: None for name in _COLUMNS}
    row.update(
        run_id=str(run_id),
        stage=stage,
        target=str(target),
        mode=mode,
        started_at=started_at,
        finished_at=utc_now(),
        execution_seconds=float(execution_seconds),
        status=status,
    )
    for name, value in metrics.items():
        if name in JSON_FIELDS:
            row[JSON_FIELDS[name]] = None if value is None else json.dumps(value, sort_keys=True)
        elif value is None:
            row[name] = None
        elif name in ("schema_version", "rule_version"):
            row[name] = str(value)
        elif name == "validation_enabled":
            row[name] = bool(value)
        else:
            row[name] = int(value)
    if error is not None:
        row["error_message"] = f"{type(error).__name__}: {error}"[:MAX_ERROR_LENGTH]
    return row


def write_run(spark: SparkSession, row: Mapping[str, Any], delta_root: str | Path) -> None:
    """Append one row to the monitoring table (created on first write)."""
    (
        spark.createDataFrame([dict(row)], PIPELINE_RUNS_SCHEMA)
        .coalesce(1)
        .write.format("delta")
        .mode("append")
        .save(str(Path(delta_root) / PIPELINE_RUNS))
    )


def record_run(
    spark: SparkSession,
    row: Mapping[str, Any],
    delta_root: str | Path,
    *,
    enabled: bool = True,
    error: BaseException | None = None,
) -> None:
    """Write a stage's row without ever hiding the stage's own outcome.

    ``error`` is the stage's exception on its failure path. If that write fails
    too, the stage error stays the one that propagates: this returns after
    annotating it, and the caller re-raises. If the stage succeeded, a failed
    write raises :class:`MonitoringWriteError`, because the row cannot be
    recomputed later. The row is printed to stderr in both cases.
    """
    if not enabled:
        return
    try:
        write_run(spark, row, delta_root)
    except Exception as monitoring_error:
        print(json.dumps(dict(row), default=str, sort_keys=True), file=sys.stderr, flush=True)
        if error is not None:
            error.add_note(
                f"monitoring write failed: {type(monitoring_error).__name__}: {monitoring_error}"
            )
            return
        raise MonitoringWriteError(row) from monitoring_error


def delta_output_stats(spark: SparkSession, path: str | Path) -> dict[str, int]:
    """Version, snapshot bytes and file count of a Delta table just written."""
    table = DeltaTable.forPath(spark, str(path))
    detail = table.detail().first()
    return {
        "output_version": int(table.history(1).first()["version"]),
        "output_bytes": int(detail.sizeInBytes),
        "output_files": int(detail.numFiles),
    }


def read_runs(spark: SparkSession, delta_root: str | Path) -> DataFrame:
    path = Path(delta_root) / PIPELINE_RUNS
    if not (path / "_delta_log").exists():
        raise RuntimeError(f"No monitoring table at {path}; run a pipeline stage first.")
    return spark.read.format("delta").load(str(path))


REPORT_QUERIES = {
    "validation_failures_by_target": "Which dataset fails validation most frequently?",
    "failure_codes_by_target": "Which validation rules reject the records?",
    "processing_time_by_target": "Which dataset requires the longest processing time?",
    "rejected_per_execution": "How many records were rejected during each execution?",
    "processing_time_trend": "How has processing time changed over multiple executions?",
}


def report_sql(name: str) -> str:
    if name not in REPORT_QUERIES:
        raise KeyError(f"Unknown monitoring query {name!r}; choose from {sorted(REPORT_QUERIES)}")
    return (SQL_DIRECTORY / f"{name}.sql").read_text(encoding="utf-8")


def run_report(
    spark: SparkSession,
    delta_root: str | Path,
    names: list[str] | None = None,
) -> dict[str, DataFrame]:
    """Register ``pipeline_runs`` as a view and return the selected ops queries."""
    read_runs(spark, delta_root).createOrReplaceTempView("pipeline_runs")
    return {name: spark.sql(report_sql(name)) for name in names or REPORT_QUERIES}


def import_legacy_runs(spark: SparkSession, delta_root: str | Path) -> dict[str, int]:
    """Copy Week 1/2 audit rows (``ingestion_runs``, ``product_refresh_runs``) into ``pipeline_runs``.

    Rows already present (same run_id, stage, target and start time) are skipped,
    so the import can be repeated. The legacy tables are left untouched.
    """
    root = Path(delta_root)
    sources = {
        "ingestion": (root / "metadata" / "ingestion_runs", _LEGACY_INGESTION_SQL),
        "product_refresh": (
            root / "analytics" / "metadata" / "product_refresh_runs",
            _LEGACY_REFRESH_SQL,
        ),
    }
    imported: dict[str, int] = {}
    for stage, (path, legacy_sql) in sources.items():
        if not (path / "_delta_log").exists():
            imported[stage] = 0
            continue
        spark.read.format("delta").load(str(path)).createOrReplaceTempView("legacy_runs")
        converted = spark.sql(legacy_sql)
        target = root / PIPELINE_RUNS
        if (target / "_delta_log").exists():
            existing = spark.read.format("delta").load(str(target)).select(
                "run_id", "stage", "target", "started_at"
            )
            converted = converted.join(
                existing, ["run_id", "stage", "target", "started_at"], "left_anti"
            )
        converted = converted.select(*[field.name for field in PIPELINE_RUNS_SCHEMA.fields])
        rows = converted.count()
        if rows:
            converted.coalesce(1).write.format("delta").mode("append").save(str(target))
        imported[stage] = rows
        spark.catalog.dropTempView("legacy_runs")
    return imported


_LEGACY_INGESTION_SQL = """SELECT run_id, 'ingestion' AS stage, dataset AS target, 'full' AS mode,
    started_at, finished_at, execution_seconds,
    input_count AS processed_count, accepted_count AS inserted_count,
    CAST(0 AS BIGINT) AS updated_count, CAST(0 AS BIGINT) AS duplicate_count,
    rejected_count, scope_excluded_count,
    CAST(NULL AS BIGINT) AS target_rows_before, accepted_count AS target_rows_after,
    TRUE AS validation_enabled,
    error_counts_json AS validation_failure_counts_json, quality_flag_counts_json,
    schema_version, rule_version, '[]' AS schema_changes_json,
    CAST(NULL AS STRING) AS input_paths_json, CAST(NULL AS STRING) AS source_versions_json,
    CAST(NULL AS BIGINT) AS output_version, CAST(NULL AS BIGINT) AS output_bytes,
    CAST(NULL AS BIGINT) AS output_files,
    status, error_message
FROM legacy_runs"""


_LEGACY_REFRESH_SQL = """SELECT run_id, 'product_refresh' AS stage, product_name AS target, 'full' AS mode,
    started_at, finished_at, execution_seconds,
    CAST(NULL AS BIGINT) AS processed_count, row_count AS inserted_count,
    CAST(0 AS BIGINT) AS updated_count, CAST(0 AS BIGINT) AS duplicate_count,
    CAST(0 AS BIGINT) AS rejected_count, CAST(NULL AS BIGINT) AS scope_excluded_count,
    CAST(NULL AS BIGINT) AS target_rows_before, row_count AS target_rows_after,
    CAST(NULL AS BOOLEAN) AS validation_enabled,
    CAST(NULL AS STRING) AS validation_failure_counts_json,
    CAST(NULL AS STRING) AS quality_flag_counts_json,
    schema_version, CAST(NULL AS STRING) AS rule_version, '[]' AS schema_changes_json,
    CAST(NULL AS STRING) AS input_paths_json,
    TO_JSON(MAP('integrated_taxi_trips', source_delta_version)) AS source_versions_json,
    CAST(NULL AS BIGINT) AS output_version, data_bytes AS output_bytes,
    data_files AS output_files,
    status, error_message
FROM legacy_runs"""
