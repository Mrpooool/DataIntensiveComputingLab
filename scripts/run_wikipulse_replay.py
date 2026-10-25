"""Replay a fixed raw Delta version to an isolated Kafka topic."""

import argparse
import math
from datetime import datetime

from dic_pipeline.wikipulse.archive import spark_session
from dic_pipeline.wikipulse.replay import replay


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", default="hdfs://namenode:8020/wikipulse/raw_events")
    parser.add_argument("--version", required=True, type=int)
    parser.add_argument("--bootstrap", default="kafka:29092")
    parser.add_argument("--topic", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--rate", required=True, type=float)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--from-event-time", type=datetime.fromisoformat,
                        help="start at this UTC event time and replay in event-time order (backfills)")
    args = parser.parse_args()
    if args.topic == "wikipulse.raw.live":
        parser.error("replay target must differ from live topic")
    if not math.isfinite(args.rate) or args.rate <= 0:
        parser.error("rate must be a finite positive number")
    if args.limit is not None and args.limit <= 0:
        parser.error("limit must be positive")
    spark = spark_session("WikiPulse fixed replay")
    try:
        replay(spark, args.archive, args.version, args.bootstrap, args.topic,
               args.run_id, args.rate, args.limit,
               int(args.from_event_time.timestamp()) if args.from_event_time else None)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
