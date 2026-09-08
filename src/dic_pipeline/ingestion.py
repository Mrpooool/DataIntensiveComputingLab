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
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .contracts import load_dataset_config
from .preparation import PreparationResult, prepare
from .schemas import RAW_SCHEMAS


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = PROJECT_ROOT / "data" / "raw"
DEFAULT_DELTA_ROOT = PROJECT_ROOT / "data" / "delta"

METADATA_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("dataset", StringType(), False),
        StructField("started_at", TimestampType(), False),
        StructField("finished_at", TimestampType(), False),
        StructField("execution_seconds", DoubleType(), False),
        StructField("raw_input_count", LongType(), True),
        StructField("scope_excluded_count", LongType(), True),
        StructField("input_count", LongType(), True),
        StructField("accepted_count", LongType(), True),
        StructField("rejected_count", LongType(), True),
        StructField("duplicate_count", LongType(), True),
        StructField("schema_version", StringType(), True),
        StructField("rule_version", StringType(), True),
        StructField("status", StringType(), False),
        StructField("error_counts_json", StringType(), True),
        StructField("quality_flag_counts_json", StringType(), True),
        StructField("error_message", StringType(), True),
    ]
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


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


def write_metadata(
    spark: SparkSession,
    record: Mapping[str, Any],
    delta_root: str | Path = DEFAULT_DELTA_ROOT,
) -> None:
    """Append one success or failure record to the ingestion run table."""
    path = Path(delta_root) / "metadata" / "ingestion_runs"
    mode = "append" if (path / "_delta_log").exists() else "overwrite"
    (
        spark.createDataFrame([dict(record)], METADATA_SCHEMA)
        .coalesce(1)
        .write.format("delta")
        .mode(mode)
        .option("mergeSchema", "true")
        .save(str(path))
    )


def ingest_dataset(
    spark: SparkSession,
    dataset: str,
    *,
    data_dir: str | Path = DEFAULT_DATA_DIR,
    delta_root: str | Path = DEFAULT_DELTA_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Read, prepare, write, verify and record one dataset."""
    current_run_id = run_id or str(uuid4())
    started_at = _utc_now()
    started = perf_counter()
    result: PreparationResult | None = None
    metrics: dict[str, Any] = {}

    try:
        raw = read_source(spark, dataset, data_dir)
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

        record = _metadata_record(
            run_id=current_run_id,
            dataset=dataset,
            started_at=started_at,
            elapsed=perf_counter() - started,
            status="success",
            metrics=metrics,
        )
        write_metadata(spark, record, delta_root)
        return record
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
        write_metadata(spark, record, delta_root)
        raise
    finally:
        if result is not None:
            result.release()
