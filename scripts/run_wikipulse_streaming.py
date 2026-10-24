"""Run role B's WikiPulse live or replay Structured Streaming analytics."""

import argparse

from dic_pipeline.wikipulse import store
from dic_pipeline.wikipulse.archive import spark_session
from dic_pipeline.wikipulse.streaming import kafka_source, start_queries, stop_queries


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default="kafka:29092")
    parser.add_argument("--topic", default="wikipulse.raw.live")
    parser.add_argument("--starting-offsets", choices=["earliest", "latest"], default="earliest")
    parser.add_argument("--max-offsets-per-trigger", type=int)
    parser.add_argument("--trigger-interval", default="10 seconds")
    parser.add_argument("--checkpoint-root",
                        default="hdfs://namenode:8020/wikipulse/checkpoints/analysis_live")
    parser.add_argument("--baseline-version", required=True)
    parser.add_argument("--keyspace", default=store.KEYSPACE,
                        help="keyspace containing the published baseline and live results")
    parser.add_argument("--hosts", default="cassandra", help="Cassandra contact points, comma separated")
    parser.add_argument("--run-id",
                        help="replay ID; writes to wikipulse_replay_<id> and an isolated checkpoint")
    args = parser.parse_args()

    if args.max_offsets_per_trigger is not None and args.max_offsets_per_trigger < 1:
        parser.error("--max-offsets-per-trigger must be positive")
    if args.run_id and args.topic == "wikipulse.raw.live":
        parser.error("--run-id requires a replay topic, not wikipulse.raw.live")
    if not args.run_id and args.topic != "wikipulse.raw.live":
        parser.error("a non-live topic requires --run-id so results cannot contaminate the live keyspace")

    spark = spark_session("WikiPulse role B analytics")
    queries = []
    try:
        baseline = store.read_baseline(spark, args.baseline_version,
                                       keyspace=args.keyspace, hosts=args.hosts).cache()
        baseline.count()  # Fix the selected version in memory before the streams start.
        result_keyspace = store.replay_keyspace(args.run_id) if args.run_id else args.keyspace
        store.create_schema(result_keyspace, args.hosts)
        checkpoint_root = args.checkpoint_root
        suffix = ""
        if args.run_id:
            checkpoint_root = f"{checkpoint_root.rstrip('/')}/replay/{args.run_id}"
            suffix = f"_{args.run_id}"
        raw = kafka_source(
            spark, args.bootstrap, args.topic, starting_offsets=args.starting_offsets,
            max_offsets_per_trigger=args.max_offsets_per_trigger,
        )
        queries = start_queries(
            raw, baseline, checkpoint_root=checkpoint_root, keyspace=result_keyspace,
            hosts=args.hosts, trigger_interval=args.trigger_interval, query_suffix=suffix,
        )
        spark.streams.awaitAnyTermination()
        failures = []
        for query in queries:
            failure = query.exception()
            if failure is not None:
                failures.append(failure)
        if failures:
            raise RuntimeError(f"WikiPulse streaming query failed: {failures[0]}")
        raise RuntimeError("a WikiPulse streaming query terminated unexpectedly")
    finally:
        stop_queries(queries)
        spark.stop()


if __name__ == "__main__":
    main()
