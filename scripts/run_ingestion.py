"""Command-line entry point for the ingestion framework."""

import argparse

from configs.datasets import DATASETS
from src.ingestion import create_spark, ingest_dataset


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", choices=["all", *DATASETS], default="all")
    args = parser.parse_args()

    try:
        from src.transforms import TRANSFORMS
    except ImportError as error:
        raise SystemExit(
            "Missing src/transforms.py from Role B. It must define TRANSFORMS."
        ) from error

    names = DATASETS if args.dataset == "all" else [args.dataset]
    spark = create_spark()
    try:
        for name in names:
            if name not in TRANSFORMS:
                raise ValueError(f"No transform configured for {name}")
            ingest_dataset(spark, name, TRANSFORMS[name])
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
