"""Export analytical Delta products to single CSV files for Excel viewing."""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

from src.dic_pipeline.data_products import load_product_config
from src.dic_pipeline.ingestion import create_spark


def _export_one(spark, product_path: Path, csv_path: Path) -> int:
    if not (product_path / "_delta_log").exists():
        raise FileNotFoundError(f"Delta product not found: {product_path}")

    tmp_dir = csv_path.parent / f".tmp_{csv_path.stem}"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)

    frame = spark.read.format("delta").load(str(product_path))
    row_count = frame.count()
    (
        frame.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(str(tmp_dir))
    )

    parts = sorted(tmp_dir.glob("*.csv"))
    if not parts:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise RuntimeError(f"No CSV part written for {product_path.name}")

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if csv_path.exists():
        csv_path.unlink()
    shutil.move(str(parts[0]), str(csv_path))
    shutil.rmtree(tmp_dir, ignore_errors=True)
    return row_count


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delta-root", type=Path, default=Path("data/delta"))
    parser.add_argument(
        "--products-root",
        type=Path,
        default=None,
        help="Delta products root (default: <delta-root>/analytics)",
    )
    parser.add_argument("--export-root", type=Path, default=Path("data/exports"))
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/data_products.json"),
    )
    parser.add_argument(
        "--product",
        action="append",
        help="all or a product name; repeat to select multiple (default: all)",
    )
    parser.add_argument("--master", default="local[4]")
    parser.add_argument("--driver-memory", default="4g")
    parser.add_argument("--shuffle-partitions", type=int, default=128)
    args = parser.parse_args()

    config = load_product_config(args.config)
    selected = args.product or ["all"]
    if "all" in selected:
        selected = list(config["products"])

    products_root = args.products_root or args.delta_root / "analytics"
    export_root = args.export_root
    export_root.mkdir(parents=True, exist_ok=True)

    spark = create_spark(
        master=args.master,
        driver_memory=args.driver_memory,
        shuffle_partitions=args.shuffle_partitions,
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        exports: list[dict[str, object]] = []
        for name in selected:
            csv_path = export_root / f"{name}.csv"
            rows = _export_one(spark, products_root / name, csv_path)
            exports.append({"product": name, "rows": rows, "path": str(csv_path)})
        print(json.dumps({"status": "success", "exports": exports}, indent=2))
    except Exception as error:
        print(json.dumps({"status": "failed", "error": f"{type(error).__name__}: {error}"}))
        raise SystemExit(1) from error
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
