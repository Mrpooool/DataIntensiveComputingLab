"""Generate and apply Week 3 incremental update files."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.dic_pipeline.incremental import (
    UPDATE_DATASETS,
    apply_updates,
    generate_update,
)
from src.dic_pipeline.ingestion import create_spark


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=("generate", "apply"),
        help="generate update files or apply their manifests",
    )
    parser.add_argument("--delta-root", type=Path, default=Path("data/delta"))
    parser.add_argument("--out-dir", type=Path, default=Path("data/updates"))
    parser.add_argument(
        "--dataset",
        action="append",
        choices=[*UPDATE_DATASETS, "all"],
        help="dataset to generate (repeatable); default all three",
    )
    parser.add_argument(
        "--manifests",
        type=Path,
        default=None,
        help="JSON file with a list of update manifests for apply",
    )
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--no-monitoring", action="store_true")
    parser.add_argument(
        "--no-validation",
        action="store_true",
        help="Disable row business rules for controlled validation-overhead experiments only.",
    )
    parser.add_argument("--master", default="local[4]")
    parser.add_argument("--driver-memory", default="4g")
    parser.add_argument("--shuffle-partitions", type=int, default=128)
    args = parser.parse_args()

    spark = create_spark(
        master=args.master,
        driver_memory=args.driver_memory,
        shuffle_partitions=args.shuffle_partitions,
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        if args.command == "generate":
            selected = args.dataset or ["all"]
            datasets = list(UPDATE_DATASETS) if "all" in selected else list(selected)
            manifests = [
                generate_update(
                    spark, dataset, args.out_dir, delta_root=args.delta_root, seed=args.seed
                )
                for dataset in datasets
            ]
            manifest_path = args.out_dir / "update_manifests.json"
            args.out_dir.mkdir(parents=True, exist_ok=True)
            manifest_path.write_text(
                json.dumps(manifests, indent=2) + "\n", encoding="utf-8"
            )
            print(json.dumps({"status": "success", "manifests": manifests, "path": str(manifest_path)}, indent=2))
            return

        manifest_path = args.manifests or (args.out_dir / "update_manifests.json")
        updates = json.loads(manifest_path.read_text(encoding="utf-8"))
        records = apply_updates(
            spark,
            updates,
            delta_root=args.delta_root,
            run_id=args.run_id,
            validate=not args.no_validation,
            monitoring=not args.no_monitoring,
        )
        print(json.dumps({"status": "success", "records": records}, default=str, indent=2))
    except Exception as error:
        print(json.dumps({"status": "failed", "error": f"{type(error).__name__}: {error}"}))
        raise SystemExit(1) from error
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
