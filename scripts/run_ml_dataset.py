"""Generate the Week 4 Role A training Delta table from a fixed platform snapshot."""

from __future__ import annotations

import argparse
from pathlib import Path

from src.dic_pipeline.ingestion import DEFAULT_DELTA_ROOT, create_spark
from src.dic_pipeline.ml_dataset import materialize_training_dataset
from src.dic_pipeline.ml_pipeline import DEFAULT_ML_CONFIG, load_ml_config


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delta-root", type=Path, default=DEFAULT_DELTA_ROOT)
    parser.add_argument("--config", type=Path, default=DEFAULT_ML_CONFIG)
    parser.add_argument("--output-path", type=Path)
    parser.add_argument("--metadata-path", type=Path)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--master", default="local[4]")
    parser.add_argument("--driver-memory", default="4g")
    parser.add_argument("--shuffle-partitions", type=int, default=128)
    args = parser.parse_args()
    spark = create_spark(
        master=args.master, driver_memory=args.driver_memory,
        shuffle_partitions=args.shuffle_partitions,
    )
    try:
        report = materialize_training_dataset(
            spark, load_ml_config(args.config), delta_root=args.delta_root,
            output_path=args.output_path, metadata_path=args.metadata_path,
            overwrite=args.overwrite,
        )
        print(f"Training dataset: {report['output_path']} (version {report['output_version']})")
        print(f"Rows: {report['row_count']}; splits: {report['split_counts']}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
