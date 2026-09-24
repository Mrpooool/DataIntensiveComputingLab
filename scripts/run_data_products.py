"""Generate Week 2 analytical data products from a consistent source snapshot."""

from __future__ import annotations

import argparse
import json
from importlib import import_module
from pathlib import Path

from src.dic_pipeline.data_products import (
    DEFAULT_PRODUCT_BUILDERS,
    load_product_config,
    refresh_data_products,
)
from src.dic_pipeline.ingestion import create_spark


def _load_builders(module_name: str | None):
    if not module_name:
        return DEFAULT_PRODUCT_BUILDERS
    module = import_module(module_name)
    builders = getattr(module, "PRODUCT_BUILDERS", None)
    if not isinstance(builders, dict):
        raise TypeError(f"{module_name} must expose a PRODUCT_BUILDERS dict.")
    return builders


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delta-root", type=Path, default=Path("data/delta"))
    parser.add_argument("--output-root", type=Path, default=None)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/data_products.json"),
    )
    parser.add_argument(
        "--builders-module",
        default=None,
        help="Optional module exposing PRODUCT_BUILDERS (owned by role B).",
    )
    parser.add_argument(
        "--product",
        action="append",
        help="all or a product name; repeat to select multiple (default: all)",
    )
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--no-monitoring", action="store_true",
                        help="Do not write metadata/pipeline_runs (for overhead measurements).")
    parser.add_argument(
        "--mode",
        choices=("full", "auto"),
        default="full",
        help="full rebuilds every selected product; auto refreshes only those affected by apply_updates",
    )
    parser.add_argument("--master", default="local[4]")
    parser.add_argument("--driver-memory", default="4g")
    parser.add_argument("--shuffle-partitions", type=int, default=128)
    args = parser.parse_args()

    config = load_product_config(args.config)
    selected = args.product or ["all"]
    if "all" in selected:
        selected = list(config["products"])
    output_root = args.output_root or args.delta_root / "analytics"

    spark = create_spark(
        master=args.master,
        driver_memory=args.driver_memory,
        shuffle_partitions=args.shuffle_partitions,
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        records = refresh_data_products(
            spark,
            _load_builders(args.builders_module),
            delta_root=args.delta_root,
            output_root=output_root,
            config_path=args.config,
            selected=selected,
            run_id=args.run_id,
            monitoring=not args.no_monitoring,
            mode=args.mode,
        )
        print(json.dumps({"status": "success", "products": records}, default=str, indent=2))
    except Exception as error:
        print(json.dumps({"status": "failed", "error": f"{type(error).__name__}: {error}"}))
        raise SystemExit(1) from error
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
