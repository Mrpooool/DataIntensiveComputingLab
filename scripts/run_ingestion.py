"""Command-line entry point for raw-to-Delta ingestion."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from uuid import uuid4

from src.dic_pipeline.ingestion import DATASETS, create_spark, ingest_batch, ingest_dataset


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest raw datasets into Delta")
    parser.add_argument(
        "--dataset",
        choices=("all", *DATASETS),
        default="all",
    )
    parser.add_argument("--data-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--output-root", type=Path, default=Path("data/delta"))
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--master", default="local[4]")
    parser.add_argument("--driver-memory", default="4g")
    parser.add_argument("--shuffle-partitions", type=int, default=128)
    args = parser.parse_args()

    run_id = args.run_id or str(uuid4())
    spark = create_spark(
        master=args.master,
        driver_memory=args.driver_memory,
        shuffle_partitions=args.shuffle_partitions,
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        options = dict(data_dir=args.data_dir, delta_root=args.output_root, run_id=run_id)
        if args.dataset == "all":
            records = ingest_batch(spark, **options)
        else:
            records = [ingest_dataset(spark, args.dataset, **options)]
        for record in records:
            print(json.dumps(record, default=str, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
