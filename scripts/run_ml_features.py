"""Fit Week 4 Role B preprocessing on train only and materialize reusable features."""

from __future__ import annotations

import argparse
from pathlib import Path
from time import perf_counter

from src.dic_pipeline.ingestion import DEFAULT_DELTA_ROOT, create_spark
from src.dic_pipeline.ml_pipeline import (
    DEFAULT_ML_CONFIG,
    FEATURES_COLUMN,
    fit_feature_pipeline,
    load_ml_config,
    load_training_dataset,
    select_feature_output,
    write_json,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--training-data",
        type=Path,
        default=DEFAULT_DELTA_ROOT / "ml" / "training_dataset",
        help="Role A training dataset in Delta or Parquet format.",
    )
    parser.add_argument("--input-format", choices=("delta", "parquet"), default="delta")
    parser.add_argument("--config", type=Path, default=DEFAULT_ML_CONFIG)
    parser.add_argument(
        "--output-path",
        type=Path,
        default=DEFAULT_DELTA_ROOT / "ml" / "feature_dataset",
    )
    parser.add_argument(
        "--model-path",
        type=Path,
        default=Path("artifacts/w4/features/pipeline_model"),
    )
    parser.add_argument(
        "--metadata-path",
        type=Path,
        default=Path("artifacts/w4/features/metadata.json"),
    )
    parser.add_argument("--master", default="local[4]")
    parser.add_argument("--driver-memory", default="4g")
    parser.add_argument("--shuffle-partitions", type=int, default=128)
    args = parser.parse_args()

    config = load_ml_config(args.config)
    spark = create_spark(
        master=args.master,
        driver_memory=args.driver_memory,
        shuffle_partitions=args.shuffle_partitions,
    )
    spark.sparkContext.setLogLevel("WARN")
    started = perf_counter()
    featured = None
    try:
        source = load_training_dataset(
            spark, args.training_data, input_format=args.input_format
        )
        fit_started = perf_counter()
        model, split_counts = fit_feature_pipeline(source, config)
        fit_seconds = perf_counter() - fit_started
        featured = select_feature_output(model.transform(source), config).cache()
        featured.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(str(args.output_path))
        row_count = featured.count()
        first = featured.select(FEATURES_COLUMN).first()
        vector_size = 0 if first is None else int(first[FEATURES_COLUMN].size)
        model.write().overwrite().save(str(args.model_path))
        metadata = {
            "status": "success",
            "prediction_task": config["prediction_task"],
            "schema_version": config["schema_version"],
            "training_data": str(args.training_data),
            "input_format": args.input_format,
            "output_path": str(args.output_path),
            "model_path": str(args.model_path),
            "row_count": row_count,
            "split_counts": split_counts,
            "feature_vector_size": vector_size,
            "fit_seconds": fit_seconds,
            "total_seconds": perf_counter() - started,
        }
        write_json(args.metadata_path, metadata)
        print(f"Feature dataset: {args.output_path}")
        print(f"Feature model: {args.model_path}")
        print(f"Metadata: {args.metadata_path}")
    finally:
        if featured is not None:
            featured.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
