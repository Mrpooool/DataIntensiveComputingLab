"""Run the Week 2 role-B Spark SQL queries against the Week 1 Delta snapshot."""

import argparse
from pathlib import Path

from src.dic_pipeline.data_products import register_analytics_inputs
from src.dic_pipeline.ingestion import DEFAULT_DELTA_ROOT, create_spark
from src.dic_pipeline.queries import (
    DEFAULT_QUERY_CONFIG,
    QUERY_DEFINITIONS,
    render_query,
    run_query,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delta-root", type=Path, default=DEFAULT_DELTA_ROOT)
    parser.add_argument("--config", type=Path, default=DEFAULT_QUERY_CONFIG)
    parser.add_argument(
        "--query",
        action="append",
        choices=["all", *QUERY_DEFINITIONS],
        help="Query ID; repeat to select several (default: all).",
    )
    parser.add_argument("--start-date", help="Inclusive New York date (YYYY-MM-DD).")
    parser.add_argument("--end-date", help="Exclusive New York date (YYYY-MM-DD).")
    parser.add_argument("--limit", type=int, default=100, help="Rows shown per query.")
    parser.add_argument("--show-sql", action="store_true")
    parser.add_argument("--explain", action="store_true")
    parser.add_argument("--master", default="local[4]")
    parser.add_argument("--driver-memory", default="4g")
    parser.add_argument("--shuffle-partitions", type=int, default=128)
    args = parser.parse_args()

    selected = args.query or ["all"]
    if "all" in selected:
        selected = list(QUERY_DEFINITIONS)

    spark = create_spark(
        master=args.master,
        driver_memory=args.driver_memory,
        shuffle_partitions=args.shuffle_partitions,
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        snapshot = register_analytics_inputs(spark, args.delta_root)
        print(
            f"Registered integrated Delta version {snapshot['integrated_version']} "
            f"from {snapshot['integrated_path']}"
        )
        for query_id in selected:
            definition = QUERY_DEFINITIONS[query_id]
            print(f"\n[{query_id.upper()}] {definition.title}")
            if args.show_sql:
                print(
                    render_query(
                        query_id,
                        config_path=args.config,
                        start_date=args.start_date,
                        end_date=args.end_date,
                    )
                )
            result = run_query(
                spark,
                query_id,
                config_path=args.config,
                start_date=args.start_date,
                end_date=args.end_date,
            )
            if args.explain:
                result.explain(mode="formatted")
            result.show(args.limit, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
