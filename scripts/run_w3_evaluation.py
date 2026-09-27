"""Run the Week 3 production-readiness measurements on copies of a baseline Delta root."""

import argparse
from datetime import datetime, timezone
from importlib.metadata import version
import json
from pathlib import Path
import platform
from uuid import uuid4

from src.dic_pipeline.ingestion import DEFAULT_DATA_DIR, DEFAULT_DELTA_ROOT, create_spark
from src.dic_pipeline.w3_evaluation import (
    build_measurements,
    prepare_updates,
    remove_tree,
    run_measurement,
    storage_report,
)


def save_json(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2, default=str) + "\n", encoding="utf-8")


def read_manifest(path: Path):
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-root", type=Path, default=DEFAULT_DELTA_ROOT,
                        help="Published Delta root; copied per run, never modified.")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--output-root", type=Path, default=Path("data/benchmark/w3"))
    parser.add_argument("--measurement", action="append",
                        help="Measurement name; repeat to select several (default: all).")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--keep-workspaces", action="store_true",
                        help="Keep each run's Delta copy under <output>/workspace for inspection.")
    parser.add_argument("--list", action="store_true", help="Print measurements and exit.")
    parser.add_argument("--master", default="local[4]")
    parser.add_argument("--driver-memory", default="4g")
    parser.add_argument("--shuffle-partitions", type=int, default=128)
    args = parser.parse_args()

    measurements = build_measurements(args.data_dir)
    if args.list:
        for measurement in measurements:
            print(f"{measurement.name:<34} {measurement.metric:<26} {measurement.description}")
        return
    selected = args.measurement or [measurement.name for measurement in measurements]
    unknown = sorted(set(selected) - {measurement.name for measurement in measurements})
    if unknown:
        raise SystemExit(f"Unknown measurements {unknown}; choose from "
                         f"{[measurement.name for measurement in measurements]}")
    # Only the update-based measurements are pending before prepare_updates.
    needs_updates = any(m.pending for m in measurements if m.name in selected)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
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
            "baseline": {
                "root": str(args.baseline_root),
                "completed_batch": read_manifest(args.baseline_root / "metadata" / "completed_batch.json"),
                "completed_integration": read_manifest(
                    args.baseline_root / "metadata" / "completed_integration.json"
                ),
                "storage": storage_report(spark, args.baseline_root),
            },
            "environment": {
                "python": platform.python_version(), "spark": spark.version,
                "delta": version("delta-spark"), "os": platform.platform(),
                "java": spark.sparkContext._jvm.java.lang.System.getProperty("java.version"),
                "jvm_max_heap_bytes": spark.sparkContext._jvm.java.lang.Runtime.getRuntime().maxMemory(),
                "master": spark.sparkContext.master,
                "spark_conf": {key: spark.conf.get(key) for key in (
                    "spark.sql.shuffle.partitions", "spark.sql.adaptive.enabled",
                    "spark.sql.session.timeZone", "spark.sql.ansi.enabled",
                )},
            },
            "method": {
                "warmups_per_variant": 1,
                "measured_repeats": args.repeats,
                "order": "variants alternated every repeat",
                "workspace": "every run, warm-up included, starts from a fresh copy of the baseline "
                             "Delta root in its own directory; copying is not timed",
                "timer": "wall clock around the stage call and its output-signature read (the "
                         "same for both variants); per-stage times come from the run's own "
                         "pipeline_runs rows",
                "equality": "every run must reproduce the first run's output signature (row "
                            "counts; content hashes for product refreshes), or the measurement "
                            "is reported as a mismatch without timings",
                "os_cache": "uncontrolled; the warm-up absorbs cold reads",
            },
            "measurements": [],
        }
        updated_root = None
        if needs_updates:
            print("Preparing update files and the updated baseline (untimed)", flush=True)
            manifests, updated_root = prepare_updates(
                spark, args.baseline_root, output, f"{run_id}-setup"
            )
            measurements = build_measurements(args.data_dir, manifests)
            # Storage overhead: the updated copy against result["baseline"]["storage"].
            result["updates"] = {
                "manifests": manifests,
                "root": str(updated_root),
                "storage": storage_report(spark, updated_root),
            }
            save_json(output / "results.json", result)
        measurements = [m for m in measurements if m.name in selected]

        def progress(entry):
            print(f"  {entry['variant']:<16} {entry['phase']}{entry['repeat'] or '':<3} "
                  f"{entry['seconds']:8.2f}s", flush=True)

        for measurement in measurements:
            print(f"{measurement.name}", flush=True)
            outcome = run_measurement(
                spark, measurement, baseline_root=args.baseline_root,
                workspace_root=output / "workspace", run_prefix=run_id,
                updated_root=updated_root, repeats=args.repeats,
                keep_workspaces=args.keep_workspaces, on_run=progress,
            )
            result["measurements"].append(outcome)
            save_json(output / "results.json", result)
            if outcome["status"] == "pending":
                print(f"  pending: {outcome['requires']}", flush=True)
            elif outcome["status"] == "success" and "difference_seconds" in outcome:
                print(f"  difference {outcome['difference_seconds']:+.2f}s "
                      f"({outcome['relative_difference']:+.1%})", flush=True)

        mismatches = [m["name"] for m in result["measurements"] if m["status"] == "mismatch"]
        result["status"] = "failed" if mismatches else "success"
        result["mismatches"] = mismatches
        save_json(output / "results.json", result)
        if updated_root is not None and not args.keep_workspaces:
            remove_tree(updated_root)
        print(f"Evaluation {result['status']}: {output / 'results.json'}", flush=True)
        if mismatches:
            raise SystemExit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
