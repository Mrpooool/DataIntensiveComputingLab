"""Run the Week 2 query optimization experiments on the published Delta snapshot."""

import argparse
from datetime import datetime, timezone
from importlib.metadata import version
import json
from pathlib import Path
import platform
from uuid import uuid4

from src.dic_pipeline.data_products import register_analytics_inputs
from src.dic_pipeline.ingestion import DEFAULT_DELTA_ROOT, create_spark
from src.dic_pipeline.query_benchmark import (
    build_experiments,
    register_products,
    run_experiments,
)


def save_json(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2, default=str) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delta-root", type=Path, default=DEFAULT_DELTA_ROOT)
    parser.add_argument("--products-root", type=Path, default=None,
                        help="Product tables (default: <delta-root>/analytics).")
    parser.add_argument("--output-root", type=Path, default=Path("data/benchmark/w2"))
    parser.add_argument("--experiment", action="append",
                        help="Experiment name; repeat to select several (default: all).")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--start-date", default="2024-02-01",
                        help="Inclusive New York date for range experiments.")
    parser.add_argument("--end-date", default="2024-03-01",
                        help="Exclusive New York date for range experiments.")
    parser.add_argument("--skip-products", action="store_true")
    parser.add_argument("--list", action="store_true", help="Print experiment names and exit.")
    parser.add_argument("--master", default="local[4]")
    parser.add_argument("--driver-memory", default="4g")
    parser.add_argument("--shuffle-partitions", type=int, default=128)
    args = parser.parse_args()

    if args.list:
        for experiment in build_experiments(start_date=args.start_date, end_date=args.end_date,
                                            products={name: {"view": ""} for name in (
                                                "daily_mobility_summary", "taxi_zone_statistics",
                                                "weather_impact_summary", "air_quality_impact_summary")}):
            print(f"{experiment.name:<40} {experiment.technique:<18} {experiment.query_id}")
        return

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    output = args.output_root / run_id
    output.mkdir(parents=True)
    print(f"Benchmark output: {output}", flush=True)

    spark = create_spark(
        master=args.master, driver_memory=args.driver_memory,
        shuffle_partitions=args.shuffle_partitions,
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        snapshot = register_analytics_inputs(spark, args.delta_root)
        products = {} if args.skip_products else register_products(
            spark, args.delta_root, output_root=args.products_root,
        )
        experiments = build_experiments(
            start_date=args.start_date, end_date=args.end_date, products=products,
        )
        if args.experiment:
            known = {experiment.name for experiment in experiments}
            unknown = sorted(set(args.experiment) - known)
            if unknown:
                raise SystemExit(f"Unknown experiments {unknown}; choose from {sorted(known)}")
            experiments = [e for e in experiments if e.name in args.experiment]

        result = {
            "run_id": run_id,
            "status": "running",
            "snapshot": snapshot,
            "products": products,
            "environment": {
                "python": platform.python_version(), "spark": spark.version,
                "delta": version("delta-spark"), "os": platform.platform(),
                "java": spark.sparkContext._jvm.java.lang.System.getProperty("java.version"),
                "jvm_max_heap_bytes": spark.sparkContext._jvm.java.lang.Runtime.getRuntime().maxMemory(),
                "master": spark.sparkContext.master,
                "spark_conf": {key: spark.conf.get(key) for key in (
                    "spark.sql.shuffle.partitions", "spark.sql.adaptive.enabled",
                    "spark.sql.adaptive.coalescePartitions.enabled",
                    "spark.sql.autoBroadcastJoinThreshold", "spark.sql.session.timeZone",
                    "spark.sql.ansi.enabled",
                )},
            },
            "method": {
                "warmups_per_variant": 1,
                "measured_repeats": args.repeats,
                "order": "baseline/optimized alternated every repeat; cache experiments measure "
                         "the baseline before the cache is built because Spark substitutes cached "
                         "plans into any matching query",
                "timer": "spark.sql() + collect(); results compared to the warm-up answer every run",
                "equality": "exact for counts/keys, isclose(rel 1e-9, abs 1e-6) for floats; no speedup without equality",
                "cache": "cache build timed separately with count(); RDD storage read after the build; unpersisted afterwards",
                "range_experiments": [args.start_date, args.end_date],
                "os_cache": "uncontrolled; the first warm-up absorbs cold reads",
            },
            "experiments": [],
        }
        save_json(output / "results.json", result)

        def persist(experiment_result):
            result["experiments"].append(experiment_result)
            save_json(output / "results.json", result)
            status = "equal" if experiment_result["results_equal"] else "MISMATCH"
            speedup = experiment_result.get("speedup")
            timing = f"speedup {speedup:.2f}x" if speedup else "not timed"
            print(f"{experiment_result['name']:<40} {status:<9} {timing}", flush=True)

        run_experiments(spark, experiments, output, repeats=args.repeats, on_result=persist)
        mismatches = [e["name"] for e in result["experiments"] if not e["results_equal"]]
        result["status"] = "failed" if mismatches else "success"
        result["mismatches"] = mismatches
        save_json(output / "results.json", result)
        print(f"Benchmark {result['status']}: {output / 'results.json'}", flush=True)
        if mismatches:
            raise SystemExit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
