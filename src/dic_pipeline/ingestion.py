from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any, Mapping, Sequence
from uuid import uuid4

import pyspark
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from .contracts import load_dataset_config
from .monitoring import delta_output_stats, record_run, run_row
from .preparation import PreparationResult, prepare
from .schemas import RAW_SCHEMAS


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = PROJECT_ROOT / "data" / "raw"
DEFAULT_DELTA_ROOT = PROJECT_ROOT / "data" / "delta"
DATASETS = ("taxi", "weather", "air_quality", "taxi_zones")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def create_spark(
    *,
    master: str = "local[4]",
    driver_memory: str = "4g",
    shuffle_partitions: int = 128,
) -> SparkSession:
    """Create the shared local Spark session with Delta enabled."""
    environment = PROJECT_ROOT / ".venv"
    hadoop_home = PROJECT_ROOT / ".hadoop"
    java_executable = "java.exe" if os.name == "nt" else "java"
    for java_home in (environment / "Library", environment):
        if (java_home / "bin" / java_executable).exists():
            os.environ.setdefault("JAVA_HOME", str(java_home))
            break
    if hadoop_home.exists():
        os.environ.setdefault("HADOOP_HOME", str(hadoop_home))
        os.environ.setdefault("hadoop.home.dir", str(hadoop_home))
        hadoop_bin = str(hadoop_home / "bin")
        path_entries = os.environ.get("PATH", "").split(os.pathsep)
        if hadoop_bin not in path_entries:
            os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ.setdefault("SPARK_HOME", str(Path(pyspark.__file__).parent))
    os.environ.setdefault(
        "PYSPARK_SUBMIT_ARGS",
        f"--driver-memory {driver_memory} pyspark-shell",
    )

    builder = (
        SparkSession.builder.appName("dic-ingestion")
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def read_source(
    spark: SparkSession,
    dataset: str,
    data_dir: str | Path = DEFAULT_DATA_DIR,
) -> DataFrame:
    """Read one configured source with its explicit raw schema when available."""
    config = load_dataset_config(dataset)
    reader = spark.read.format(str(config["source_format"]))
    schema = RAW_SCHEMAS.get(dataset)
    if schema is not None and config["source_format"] == "csv":
        reader = reader.schema(schema)
    for key, value in config.get("reader_options", {}).items():
        reader = reader.option(str(key), str(value))
    if config["source_format"] == "csv":
        reader = reader.option("enforceSchema", "false")
    source_pattern = str(config["source_path"])
    if any(character in source_pattern for character in "*?["):
        source_paths = sorted(Path(data_dir).glob(source_pattern))
        if not source_paths:
            raise FileNotFoundError(
                f"No files match {source_pattern!r} in {Path(data_dir)}"
            )
        return reader.load([str(path) for path in source_paths])
    return reader.load(str(Path(data_dir) / source_pattern))


def write_delta(
    dataframe: DataFrame,
    path: str | Path,
    *,
    mode: str = "overwrite",
    partition_by: Sequence[str] = (),
    num_files: int | None = None,
) -> None:
    """Shared Delta writer for standardized, integrated and benchmark tables."""
    if num_files is not None:
        if num_files < 1:
            raise ValueError("num_files must be at least 1")
        dataframe = dataframe.coalesce(num_files)
    writer = (
        dataframe.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(str(path))


def _metadata_record(
    *,
    run_id: str,
    dataset: str,
    started_at: datetime,
    elapsed: float,
    status: str,
    metrics: Mapping[str, Any] | None = None,
    error: Exception | None = None,
) -> dict[str, Any]:
    values = dict(metrics or {})
    return {
        "run_id": run_id,
        "dataset": dataset,
        "started_at": started_at,
        "finished_at": _utc_now(),
        "execution_seconds": elapsed,
        "raw_input_count": values.get("raw_input_count"),
        "scope_excluded_count": values.get("scope_excluded_count"),
        "input_count": values.get("input_count"),
        "accepted_count": values.get("accepted_count"),
        "rejected_count": values.get("rejected_count"),
        "duplicate_count": values.get("duplicate_count"),
        "schema_version": values.get("schema_version"),
        "rule_version": values.get("rule_version"),
        "status": status,
        "error_counts_json": json.dumps(values.get("error_counts", {}), sort_keys=True),
        "quality_flag_counts_json": json.dumps(
            values.get("quality_flag_counts", {}), sort_keys=True
        ),
        "error_message": None if error is None else f"{type(error).__name__}: {error}",
    }


def _monitoring_row(
    record: Mapping[str, Any],
    metrics: Mapping[str, Any],
    *,
    input_paths: list[str] | None,
    output: Mapping[str, int],
    error: Exception | None = None,
) -> dict[str, Any]:
    """Map an ingestion record onto ``pipeline_runs``: a full overwrite skips no existing keys.

    Duplicates inside the source are rejected rows here; ``duplicate_count`` in
    the monitoring table counts keys already present in the target instead.
    """
    succeeded = record["status"] == "success"
    return run_row(
        run_id=record["run_id"],
        stage="ingestion",
        target=record["dataset"],
        started_at=record["started_at"],
        execution_seconds=record["execution_seconds"],
        status=record["status"],
        error=error,
        processed_count=record["input_count"],
        inserted_count=record["accepted_count"],
        updated_count=0 if succeeded else None,
        duplicate_count=0 if succeeded else None,
        rejected_count=record["rejected_count"],
        scope_excluded_count=record["scope_excluded_count"],
        target_rows_after=record["accepted_count"] if succeeded else None,
        validation_enabled=True,
        validation_failure_counts=metrics.get("error_counts"),
        quality_flag_counts=metrics.get("quality_flag_counts"),
        schema_version=record["schema_version"],
        rule_version=record["rule_version"],
        schema_changes=[],
        input_paths=input_paths,
        **output,
    )


def ingest_dataset(
    spark: SparkSession,
    dataset: str,
    *,
    data_dir: str | Path = DEFAULT_DATA_DIR,
    delta_root: str | Path = DEFAULT_DELTA_ROOT,
    run_id: str | None = None,
    monitoring: bool = True,
) -> dict[str, Any]:
    """Read, prepare, write, verify and record one dataset.

    ``monitoring=False`` skips the ``pipeline_runs`` row and the metadata it
    alone needs (input file list, output table stats).
    """
    # A single-table rerun also invalidates the previous complete handoff.
    (Path(delta_root) / "metadata" / "completed_batch.json").unlink(missing_ok=True)
    current_run_id = run_id or str(uuid4())
    started_at = _utc_now()
    started = perf_counter()
    result: PreparationResult | None = None
    metrics: dict[str, Any] = {}
    input_paths: list[str] | None = None
    output: dict[str, int] = {}

    try:
        raw = read_source(spark, dataset, data_dir)
        if monitoring:
            input_paths = sorted(raw.inputFiles())
        config = load_dataset_config(dataset)
        result = prepare(raw, config, run_id=current_run_id)
        metrics = dict(result.metrics)
        standardized_path = Path(delta_root) / "standardized" / dataset
        rejected_path = Path(delta_root) / "rejected" / dataset
        write_delta(
            result.accepted,
            standardized_path,
            num_files=int(config.get("output_files", 1)),
        )
        write_delta(result.rejected, rejected_path, num_files=1)

        accepted_written = spark.read.format("delta").load(
            str(standardized_path)
        ).count()
        rejected_written = spark.read.format("delta").load(str(rejected_path)).count()
        if accepted_written != metrics["accepted_count"]:
            raise RuntimeError(
                f"{dataset}: wrote {accepted_written} accepted rows, "
                f"expected {metrics['accepted_count']}"
            )
        if rejected_written != metrics["rejected_count"]:
            raise RuntimeError(
                f"{dataset}: wrote {rejected_written} rejected rows, "
                f"expected {metrics['rejected_count']}"
            )
        if monitoring:
            output = delta_output_stats(spark, standardized_path)

        record = _metadata_record(
            run_id=current_run_id,
            dataset=dataset,
            started_at=started_at,
            elapsed=perf_counter() - started,
            status="success",
            metrics=metrics,
        )
    except Exception as error:
        record = _metadata_record(
            run_id=current_run_id,
            dataset=dataset,
            started_at=started_at,
            elapsed=perf_counter() - started,
            status="failed",
            metrics=metrics,
            error=error,
        )
        row = _monitoring_row(record, metrics, input_paths=input_paths, output=output, error=error)
        record_run(spark, row, delta_root, enabled=monitoring, error=error)
        raise
    finally:
        if result is not None:
            result.release()
    # Outside the try: a failed monitoring write must not be recorded as a failed ingestion.
    row = _monitoring_row(record, metrics, input_paths=input_paths, output=output)
    record_run(spark, row, delta_root, enabled=monitoring)
    return record


def ingest_batch(
    spark: SparkSession,
    *,
    data_dir: str | Path = DEFAULT_DATA_DIR,
    delta_root: str | Path = DEFAULT_DELTA_ROOT,
    run_id: str | None = None,
    monitoring: bool = True,
) -> list[dict[str, Any]]:
    """Publish a handoff only after all four datasets succeed (one writer at a time)."""
    current_run_id = run_id or str(uuid4())
    records = []
    versions = {}
    for dataset in DATASETS:
        record = ingest_dataset(
            spark, dataset, data_dir=data_dir, delta_root=delta_root,
            run_id=current_run_id, monitoring=monitoring,
        )
        records.append(record)
        path = Path(delta_root) / "standardized" / dataset
        versions[dataset] = DeltaTable.forPath(spark, str(path)).history(1).first()["version"]

    manifest = Path(delta_root) / "metadata" / "completed_batch.json"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    pending = manifest.with_suffix(".tmp")
    pending.write_text(
        json.dumps({"run_id": current_run_id, "versions": versions}, indent=2) + "\n",
        encoding="utf-8",
    )
    pending.replace(manifest)
    return records


def load_completed_batch(delta_root: str | Path = DEFAULT_DELTA_ROOT) -> dict[str, Any]:
    """Read the manifest published by the last successful four-table ingestion."""
    manifest = Path(delta_root) / "metadata" / "completed_batch.json"
    if not manifest.exists():
        raise RuntimeError("No completed batch. Run ingestion with --dataset all first.")
    batch = json.loads(manifest.read_text(encoding="utf-8"))
    if set(batch["versions"]) != set(DATASETS):
        raise ValueError("Completed batch must contain all four datasets.")
    return batch


def read_completed_batch(
    spark: SparkSession,
    delta_root: str | Path = DEFAULT_DELTA_ROOT,
) -> dict[str, DataFrame]:
    """Load the exact four Delta versions published by a successful full ingestion."""
    batch = load_completed_batch(delta_root)
    return {
        dataset: spark.read.format("delta")
        .option("versionAsOf", batch["versions"][dataset])
        .load(str(Path(delta_root) / "standardized" / dataset))
        for dataset in DATASETS
    }
