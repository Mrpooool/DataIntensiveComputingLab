"""Build and publish one WikiPulse baseline version from a pinned raw archive version."""

import argparse
import json
from datetime import datetime

from dic_pipeline.wikipulse import store
from dic_pipeline.wikipulse.baseline import run


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", default="hdfs://namenode:8020/wikipulse/raw_events")
    parser.add_argument("--version", type=int, required=True, help="Delta version of the archive")
    parser.add_argument("--history-end", type=datetime.fromisoformat,
                        help="UTC end of history, e.g. 2026-10-13T06:00+00:00; default: last event")
    parser.add_argument("--keyspace", default=store.KEYSPACE)
    parser.add_argument("--hosts", default="cassandra", help="Cassandra contact points, comma separated")
    parser.add_argument("--local", action="store_true",
                        help="use the host .venv Spark session (Windows) instead of spark-submit packages")
    args = parser.parse_args()
    if args.history_end is not None and args.history_end.tzinfo is None:
        parser.error("--history-end needs a UTC offset")
    if args.local:
        from dic_pipeline.ingestion import create_spark
        spark = create_spark(master="local[4]", shuffle_partitions=8)
    else:
        from dic_pipeline.wikipulse.archive import spark_session
        spark = spark_session("WikiPulse baseline")
    try:
        summary = run(spark, args.archive, args.version, history_end=args.history_end,
                      keyspace=args.keyspace, hosts=args.hosts)
    finally:
        spark.stop()
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
