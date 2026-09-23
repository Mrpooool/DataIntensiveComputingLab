"""Answer the Week 3 operational questions from metadata/pipeline_runs."""

import argparse
import json
from pathlib import Path

from src.dic_pipeline.ingestion import DEFAULT_DELTA_ROOT, create_spark
from src.dic_pipeline.monitoring import REPORT_QUERIES, import_legacy_runs, report_sql, run_report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delta-root", type=Path, default=DEFAULT_DELTA_ROOT)
    parser.add_argument("--query", action="append", choices=sorted(REPORT_QUERIES),
                        help="Query to run; repeat to select several (default: all).")
    parser.add_argument("--import-legacy", action="store_true",
                        help="First copy Week 1/2 ingestion_runs and product_refresh_runs rows in.")
    parser.add_argument("--show-sql", action="store_true")
    parser.add_argument("--limit", type=int, default=50, help="Rows printed per query.")
    parser.add_argument("--output", type=Path, default=None,
                        help="Also write every result as JSON to this file.")
    args = parser.parse_args()

    if args.show_sql:
        for name in args.query or REPORT_QUERIES:
            print(f"-- {name}\n{report_sql(name)}")
        return

    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        if args.import_legacy:
            print("Imported legacy rows: " + json.dumps(import_legacy_runs(spark, args.delta_root)))
        saved = {}
        for name, frame in run_report(spark, args.delta_root, args.query).items():
            print(f"\n== {name}: {REPORT_QUERIES[name]}")
            frame.show(args.limit, truncate=False)
            saved[name] = [row.asDict() for row in frame.collect()]
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(saved, indent=2, default=str) + "\n", encoding="utf-8")
            print(f"Report written to {args.output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
