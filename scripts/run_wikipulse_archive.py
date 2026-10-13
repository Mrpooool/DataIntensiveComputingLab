"""Archive all raw Kafka records to HDFS Delta."""

import argparse

from dic_pipeline.wikipulse.archive import run_archive, spark_session


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default="kafka:29092")
    parser.add_argument("--topic", default="wikipulse.raw.live")
    parser.add_argument("--output", default="hdfs://namenode:8020/wikipulse/raw_events")
    parser.add_argument("--checkpoint", default="hdfs://namenode:8020/wikipulse/checkpoints/raw_live")
    parser.add_argument("--starting-offsets", default="earliest", choices=["earliest", "latest"])
    parser.add_argument("--trigger-interval", default="10 seconds")
    args = parser.parse_args()
    if args.output.rstrip("/") == args.checkpoint.rstrip("/"):
        parser.error("output and checkpoint must be separate paths")
    spark = spark_session("WikiPulse raw archive")
    try:
        run_archive(spark, args.bootstrap, args.topic, args.output, args.checkpoint,
                    args.starting_offsets, args.trigger_interval)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
