"""Run the Week 4 Task 4 route comparison and the feature-group experiment."""

import argparse
import csv
from datetime import datetime, timezone
from importlib.metadata import version
import json
from pathlib import Path
import platform

from src.dic_pipeline.ingestion import DEFAULT_DATA_DIR, DEFAULT_DELTA_ROOT, create_spark
from src.dic_pipeline.ml_pipeline import DEFAULT_ML_CONFIG, load_ml_config, load_training_dataset
from src.dic_pipeline.w4_evaluation import PHASES, compare_feature_groups, compare_routes

EXPERIMENTS = ("routes", "feature_groups")


def save_json(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2, default=str) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delta-root", type=Path, default=DEFAULT_DELTA_ROOT,
                        help="Published platform snapshot the platform route reads.")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR,
                        help="Original files the raw route reads.")
    parser.add_argument("--config", type=Path, default=DEFAULT_ML_CONFIG)
    parser.add_argument("--training-metadata", type=Path,
                        default=DEFAULT_DELTA_ROOT / "ml" / "training_dataset_metadata.json",
                        help="Published training dataset for the feature-group experiment.")
    parser.add_argument("--output-root", type=Path, default=Path("data/benchmark/w4"))
    parser.add_argument("--experiment", action="append", choices=EXPERIMENTS,
                        help="Repeat to select several (default: all).")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--master", default="local[4]")
    parser.add_argument("--driver-memory", default="4g")
    parser.add_argument("--shuffle-partitions", type=int, default=128)
    args = parser.parse_args()

    config = load_ml_config(args.config)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = args.output_root / run_id
    output.mkdir(parents=True)
    print(f"Evaluation output: {output}", flush=True)
    spark = create_spark(
        master=args.master, driver_memory=args.driver_memory,
        shuffle_partitions=args.shuffle_partitions,
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        result = {
            "run_id": run_id,
            "status": "running",
            "inputs": {"delta_root": str(args.delta_root), "data_dir": str(args.data_dir),
                       "config_path": str(args.config)},
            "environment": {
                "python": platform.python_version(), "spark": spark.version,
                "delta": version("delta-spark"), "os": platform.platform(),
                "java": spark.sparkContext._jvm.java.lang.System.getProperty("java.version"),
                "master": spark.sparkContext.master,
                "driver_memory": args.driver_memory,
                "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
            },
            "method": {
                "warmups_per_route": 1,
                "measured_repeats": args.repeats,
                "order": "routes alternated every repeat",
                "timer": "prepare = build and write the training Delta table; training = "
                         "train_and_evaluate on the written table (no model save)",
                "equality": "warm-up datasets of both routes must have no row difference; every "
                            "run must reproduce the first run's dataset hash and test metrics",
                "os_cache": "uncontrolled; the warm-up absorbs cold reads",
            },
            "experiments": [],
        }
        for experiment in args.experiment or EXPERIMENTS:
            print(experiment, flush=True)
            if experiment == "routes":
                def progress(entry):
                    seconds = entry["seconds"]
                    print(f"  {entry['route']:<9} {entry['phase']}{entry['repeat'] or '':<3} "
                          f"prepare {seconds['prepare']:7.1f}s  training "
                          f"{seconds['training_total']:6.1f}s", flush=True)

                outcome = compare_routes(
                    spark, delta_root=args.delta_root, data_dir=args.data_dir, config=config,
                    workspace_root=output / "workspace", run_prefix=run_id,
                    repeats=args.repeats, on_run=progress,
                )
                with (output / "timings.csv").open("w", encoding="utf-8", newline="") as stream:
                    writer = csv.writer(stream)
                    writer.writerow(["route", "phase", "repeat", "run_id", *PHASES])
                    for run in outcome["runs"]:
                        writer.writerow([run["route"], run["phase"], run["repeat"], run["run_id"],
                                         *[f"{run['seconds'][name]:.3f}" for name in PHASES]])
            else:
                dataset, training_data = load_training_dataset(spark, args.training_metadata)
                outcome = {**compare_feature_groups(dataset, config), "training_data": training_data}
                for group in outcome["groups"]:
                    metrics = group["test_metrics"]
                    print(f"  {group['name']:<17} test RMSE {metrics['rmse']:.3f}  "
                          f"MAE {metrics['mae']:.3f}  R2 {metrics['r2']:.4f}", flush=True)
            result["experiments"].append(outcome)
            save_json(output / "results.json", result)
            print(f"  {outcome['status']}", flush=True)

        failed = [item["name"] for item in result["experiments"] if item["status"] != "success"]
        result["status"] = "failed" if failed else "success"
        save_json(output / "results.json", result)
        print(f"Evaluation {result['status']}: {output / 'results.json'}", flush=True)
        if failed:
            raise SystemExit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
