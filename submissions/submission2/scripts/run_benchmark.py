"""Run the reproducible Week 1 Taxi storage benchmark."""

import argparse
from datetime import datetime, timezone
import hashlib
from importlib.metadata import version
import json
from pathlib import Path
import platform
from statistics import median
from uuid import uuid4

from src.dic_pipeline.benchmark import LAYOUTS, QUERIES, benchmark_queries, ingest_layout
from src.dic_pipeline.contracts import load_dataset_config
from src.dic_pipeline.ingestion import create_spark, read_completed_batch


def save_json(path, value):
    path.write_text(json.dumps(value, indent=2, default=str) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--delta-root", type=Path, default=Path("data/delta"))
    parser.add_argument("--output-root", type=Path, default=Path("data/benchmark"))
    parser.add_argument("--master", default="local[4]")
    parser.add_argument("--driver-memory", default="4g")
    parser.add_argument("--shuffle-partitions", type=int, default=128)
    args = parser.parse_args()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    output = args.output_root / run_id
    output.mkdir(parents=True)
    print(f"Benchmark output: {output}", flush=True)
    config = load_dataset_config("taxi")
    sources = []
    for path in sorted(args.data_dir.glob(config["source_path"])):
        with path.open("rb") as source:
            digest = hashlib.file_digest(source, "sha256").hexdigest()
        sources.append({"file": path.name, "bytes": path.stat().st_size, "sha256": digest})
    spark = create_spark(
        master=args.master, driver_memory=args.driver_memory,
        shuffle_partitions=args.shuffle_partitions,
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
    try:
        zones = read_completed_batch(spark, args.delta_root)["taxi_zones"]
        if zones.count() != zones.select("location_id").distinct().count():
            raise ValueError("Zone lookup key must be unique")
        result = {
            "run_id": run_id, "sources": sources, "taxi_config": config,
            "zone_batch": json.loads((args.delta_root / "metadata/completed_batch.json").read_text()),
            "environment": {
                "python": platform.python_version(), "spark": spark.version,
                "delta": version("delta-spark"), "os": platform.platform(),
                "java": spark.sparkContext._jvm.java.lang.System.getProperty("java.version"),
                "jvm_max_heap_bytes": spark.sparkContext._jvm.java.lang.Runtime.getRuntime().maxMemory(),
                "master": spark.sparkContext.master,
                "spark_conf": {key: spark.conf.get(key) for key in (
                    "spark.sql.shuffle.partitions", "spark.sql.adaptive.enabled",
                    "spark.sql.autoBroadcastJoinThreshold", "spark.sql.session.timeZone",
                    "spark.sql.ansi.enabled", "spark.sql.parquet.compression.codec",
                )},
            },
            "method": {
                "ingestion_order": [["S0", "S1"], ["S1", "S0"]],
                "ingestion_timer": "raw read + prepare + accepted/rejected commits; excludes verification",
                "writer": "same coalesce(8); only S1 adds partitionBy(pickup_date)",
                "query_input": "second ingestion round; no Spark data cache; OS cache uncontrolled",
                "warmups_per_query_layout": 1, "measured_query_repeats": 3,
            },
            "ingestion_samples": [],
        }
        save_json(output / "results.json", result)
        for repeat in range(2):
            order = LAYOUTS if repeat == 0 else LAYOUTS[::-1]
            for position, layout in enumerate(order):
                print(f"Ingesting round {repeat + 1}: {layout}", flush=True)
                path = output / f"round_{repeat + 1}" / layout
                sample = ingest_layout(spark, args.data_dir, path, layout, run_id)
                sample.update(round=repeat + 1, position=position + 1)
                result["ingestion_samples"].append(sample)
                save_json(output / "results.json", result)
                print(f"Written {layout}: {sample['row_count']} rows, {sample['ingestion_seconds']:.2f}s", flush=True)
        counts = {s["row_count"] for s in result["ingestion_samples"]}
        if len(counts) != 1:
            raise AssertionError("Ingestion rounds contain different row counts")
        result["ingestion_median_seconds"] = {
            layout: median(s["ingestion_seconds"] for s in result["ingestion_samples"] if s["layout"] == layout)
            for layout in LAYOUTS
        }
        tables = {
            layout: spark.read.format("delta").load(str(output / "round_2" / layout / "taxi"))
            for layout in LAYOUTS
        }
        result["queries"] = benchmark_queries(spark, tables, zones, output)
        result["status"] = "success"
        save_json(output / "results.json", result)
        (output / "queries.sql").write_text(
            "\n\n".join(f"-- {name}\n{sql};" for name, sql in QUERIES.items()) + "\n", encoding="utf-8",
        )
        print(json.dumps(result["queries"]["median_seconds"], indent=2), flush=True)
        print(f"Benchmark complete: {output / 'results.json'}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
