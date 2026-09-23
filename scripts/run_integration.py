"""Build the integrated trip Delta table from one completed batch and publish its snapshot."""

import argparse
import json
from pathlib import Path

from src.dic_pipeline.ingestion import create_spark
from src.dic_pipeline.integration import build_integrated_table


def main() -> None:
    parser = argparse.ArgumentParser(description="Integrate Taxi, zones, weather and PM2.5")
    parser.add_argument("--delta-root", type=Path, default=Path("data/delta"))
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--no-monitoring", action="store_true",
                        help="Do not write metadata/pipeline_runs (for overhead measurements).")
    args = parser.parse_args()
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        result = build_integrated_table(
            spark, args.delta_root, run_id=args.run_id, monitoring=not args.no_monitoring,
        )
        print(json.dumps(result["stats"], sort_keys=True), flush=True)
        print(f"Integrated table: {result['output']}", flush=True)
        print(
            "Published integration snapshot: "
            + json.dumps(result["snapshot"], sort_keys=True),
            flush=True,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
