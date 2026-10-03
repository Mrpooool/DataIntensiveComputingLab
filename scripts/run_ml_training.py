"""Train, evaluate, save and reload the Week 4 Role B Spark ML pipeline."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path
import platform
from uuid import uuid4

from src.dic_pipeline.ingestion import DEFAULT_DELTA_ROOT, create_spark
from src.dic_pipeline.ml_pipeline import (
    DEFAULT_ML_CONFIG,
    load_ml_config,
    load_training_dataset,
    train_and_evaluate,
    write_json,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--training-metadata",
        type=Path,
        default=DEFAULT_DELTA_ROOT / "ml" / "training_dataset_metadata.json",
        help="Metadata written by scripts.run_ml_dataset; names the Delta version to train on.",
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_ML_CONFIG)
    parser.add_argument(
        "--output-root", type=Path, default=Path("artifacts/w4/training")
    )
    parser.add_argument(
        "--run-id",
        help="Stable output subdirectory; defaults to a UTC timestamp plus random suffix.",
    )
    parser.add_argument("--master", default="local[4]")
    parser.add_argument("--driver-memory", default="4g")
    parser.add_argument("--shuffle-partitions", type=int, default=128)
    args = parser.parse_args()

    run_id = args.run_id or (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "-"
        + uuid4().hex[:8]
    )
    output = args.output_root / run_id
    model_path = output / "pipeline_model"
    config = load_ml_config(args.config)
    spark = create_spark(
        master=args.master,
        driver_memory=args.driver_memory,
        shuffle_partitions=args.shuffle_partitions,
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        source, training_data = load_training_dataset(spark, args.training_metadata)
        result = train_and_evaluate(source, config, model_path=model_path)
        report = {
            **result.report,
            "run_id": run_id,
            "training_data": training_data,
            "config_path": str(args.config),
            "environment": {
                "python": platform.python_version(),
                "spark": spark.version,
                "delta": version("delta-spark"),
                "java": spark.sparkContext._jvm.java.lang.System.getProperty(
                    "java.version"
                ),
                "master": spark.sparkContext.master,
                "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
                "session_timezone": spark.conf.get("spark.sql.session.timeZone"),
            },
        }
        write_json(output / "metrics.json", report)
        write_json(output / "config_snapshot.json", config)
        print(f"Training run: {run_id}")
        print(f"Model: {model_path}")
        print(f"Metrics: {output / 'metrics.json'}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
