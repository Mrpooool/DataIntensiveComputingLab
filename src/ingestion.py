"""Minimal reusable CSV/Parquet -> Delta ingestion framework."""

from __future__ import annotations

import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Callable

import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from configs.datasets import (
    DATASETS,
    METADATA_PATH,
    REJECTED_ROOT,
    SCHEMA_VERSION,
    STANDARDIZED_ROOT,
)

Transform = Callable[[DataFrame], tuple[DataFrame, DataFrame]]
ROOT = Path(__file__).resolve().parents[1]


def create_spark() -> SparkSession:
    """Create Spark with settings shared by all datasets."""
    java_home = ROOT / ".venv" / "Library"
    hadoop_home = ROOT / ".hadoop"
    if java_home.exists():
        os.environ.setdefault("JAVA_HOME", str(java_home))
    if hadoop_home.exists():
        os.environ.setdefault("HADOOP_HOME", str(hadoop_home))
        os.environ.setdefault("hadoop.home.dir", str(hadoop_home))
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ.setdefault("SPARK_HOME", str(Path(pyspark.__file__).parent))

    builder = (
        SparkSession.builder.appName("urban-data-platform")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def snake_case(name: str) -> str:
    """Convert source column names to the project naming convention."""
    name = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name.strip())
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_").lower()


def read_dataset(spark: SparkSession, name: str) -> DataFrame:
    """Read CSV/Parquet, validate required columns and standardize names."""
    cfg = DATASETS[name]
    reader = spark.read.format(cfg["format"])
    for key, value in cfg.get("options", {}).items():
        reader = reader.option(key, value)
    dataframe = reader.load(str(ROOT / cfg["path"]))

    missing = set(cfg["required_columns"]) - set(dataframe.columns)
    if missing:
        raise ValueError(f"{name}: missing columns {sorted(missing)}")

    for old_name in dataframe.columns:
        dataframe = dataframe.withColumnRenamed(old_name, snake_case(old_name))
    return dataframe.withColumn("_source_file", F.input_file_name())


def _write_metadata(
    spark: SparkSession,
    name: str,
    processed: int,
    rejected: int,
    elapsed: float,
) -> None:
    metadata = spark.createDataFrame(
        [(
            name, datetime.utcnow(), processed, rejected, elapsed, SCHEMA_VERSION,
        )],
        [
            "dataset", "ingested_at", "processed_records", "rejected_records",
            "execution_seconds", "schema_version",
        ],
    )
    path = ROOT / METADATA_PATH
    mode = "append" if (path / "_delta_log").exists() else "overwrite"
    metadata.write.format("delta").mode(mode).save(str(path))


def ingest_dataset(
    spark: SparkSession,
    name: str,
    transform: Transform,
) -> None:
    """Read, transform, quality-check and write one dataset."""
    started = time.perf_counter()
    source = read_dataset(spark, name).cache()
    input_count = source.count()
    accepted, rejected = transform(source)

    if "error_reasons" not in rejected.columns:
        raise ValueError(f"{name}: rejected rows need an error_reasons column")

    accepted_count = accepted.count()
    rejected_count = rejected.count()
    if input_count != accepted_count + rejected_count:
        raise ValueError(
            f"{name}: input {input_count} != accepted {accepted_count} "
            f"+ rejected {rejected_count}"
        )

    accepted.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(str(ROOT / STANDARDIZED_ROOT / name))
    rejected.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(str(ROOT / REJECTED_ROOT / name))

    _write_metadata(
        spark, name, accepted_count, rejected_count,
        time.perf_counter() - started,
    )
    print(
        f"{name}: input={input_count}, accepted={accepted_count}, "
        f"rejected={rejected_count}"
    )
