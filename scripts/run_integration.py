"""Build the integrated trip Delta table from one completed input batch."""

import argparse
import json
from pathlib import Path

from src.dic_pipeline.ingestion import create_spark, read_completed_batch, write_delta
from src.dic_pipeline.integration import integrate


def main() -> None:
    parser = argparse.ArgumentParser(description="Integrate Taxi, zones, weather and PM2.5")
    parser.add_argument("--delta-root", type=Path, default=Path("data/delta"))
    args = parser.parse_args()
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        tables = read_completed_batch(spark, args.delta_root)
        integrated, stats = integrate(
            tables["taxi"], tables["weather"], tables["air_quality"], tables["taxi_zones"],
        )
        output = args.delta_root / "integrated" / "integrated_taxi_trips"
        write_delta(integrated.repartition("pickup_date"), output, partition_by=["pickup_date"])
        written_count = spark.read.format("delta").load(str(output)).count()
        if written_count != stats["output_count"]:
            raise RuntimeError("Written integrated row count differs from the verified result.")
        metrics_path = output.parent / "integration_metrics.json"
        metrics_path.write_text(json.dumps(stats, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(stats, sort_keys=True), flush=True)
        print(f"Integrated table: {output}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
