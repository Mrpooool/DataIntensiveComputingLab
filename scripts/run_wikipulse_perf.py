"""One WikiPulse performance run: replay a pinned archive slice, run B's queries, measure.

The script creates a fresh replay topic with the requested partition count, starts
A's rate-limited replay as a separate local-mode Spark process, runs B's three
queries on the cluster against it, and stops a drain period after the replay ends.

Latency of a closed window = Cassandra ``written_at`` minus the Kafka append time of
the first replayed event whose event time passed ``window_end`` + watermark, i.e.
the moment the window became closable. Watermark waiting is window semantics, not
processing delay, so it is excluded; trigger wait and processing are included.
"""

import argparse
import json
import subprocess
import sys
import time
from bisect import bisect_left
from datetime import datetime, timedelta, timezone
from pathlib import Path

from kafka.admin import KafkaAdminClient, NewTopic
from pyspark.sql import functions as F

from dic_pipeline.wikipulse import store
from dic_pipeline.wikipulse.archive import spark_session
from dic_pipeline.wikipulse.events import WATERMARK, analysis_rows
from dic_pipeline.wikipulse.streaming import kafka_source, start_queries, stop_queries

WATERMARK_SECONDS = int(WATERMARK.split()[0]) * 60
LATENCY_TABLES = ("hot_pages", "activity_5m")
PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,io.delta:delta-spark_2.13:4.4.0"


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, max(0, round(q * (len(ordered) - 1))))]


def progress_rows(query) -> list[dict]:
    return [p if isinstance(p, dict) else json.loads(p.json) for p in query.recentProgress]


def lag(progress: dict, topic: str) -> int:
    """Records available in Kafka but not yet read when this batch was planned."""
    source = progress["sources"][0]
    end, latest = (json.loads(v) if isinstance(v, str) else v
                   for v in (source.get("endOffset"), source.get("latestOffset")))
    if not end or not latest:
        return 0
    return sum(latest[topic][k] - end[topic].get(k, 0) for k in latest[topic])


def caught_up(queries, topic: str) -> bool:
    """Every query has read the whole topic and its latest trigger found no new rows."""
    for query in queries:
        progress = query.lastProgress
        if not progress or progress["numInputRows"] or progress["sources"][0].get("endOffset") in (None, "null"):
            return False
        if lag(progress, topic):
            return False
    return True


def close_times(spark, bootstrap: str, topic: str) -> tuple[list[int], list[datetime]]:
    """Per event second, the earliest Kafka append time at or after which it was seen."""
    raw = (spark.read.format("kafka").option("kafka.bootstrap.servers", bootstrap)
           .option("subscribe", topic).option("startingOffsets", "earliest").load()
           .selectExpr("CAST(value AS STRING) AS raw_json", "timestamp AS kafka_ts"))
    # Every copy of a repeated event advances the stream's watermark (it sits before the
    # de-duplication), so the rows are taken before de-duplication too.
    rows = (analysis_rows(raw).groupBy(F.unix_seconds("event_time").alias("event_second"))
            .agg(F.min("kafka_ts").alias("first_seen")).orderBy("event_second").collect())
    seconds = [row.event_second for row in rows]
    # Suffix minimum: the first moment any event at or beyond each second was appended.
    seen = [row.first_seen.replace(tzinfo=timezone.utc) for row in rows]
    for i in range(len(seen) - 2, -1, -1):
        seen[i] = min(seen[i], seen[i + 1])
    return seconds, seen


def window_latencies(keyspace: str, hosts: str, seconds: list[int], seen: list[datetime],
                     measure_from: datetime) -> dict:
    db = store.session(hosts)
    result = {}
    for table in LATENCY_TABLES:
        first_written: dict[datetime, datetime] = {}
        for row in db.execute(f"SELECT window_end, written_at FROM {keyspace}.{table}"):
            end = row.window_end.replace(tzinfo=timezone.utc)
            written = row.written_at.replace(tzinfo=timezone.utc)
            first_written[end] = min(written, first_written.get(end, written))
        latencies = []
        for end, written in first_written.items():
            i = bisect_left(seconds, int(end.timestamp()) + WATERMARK_SECONDS)
            if i < len(seconds) and seen[i] >= measure_from:
                latencies.append((written - seen[i]).total_seconds())
        result[table] = {"windows": len(latencies), "p50_s": percentile(latencies, 0.5),
                         "p95_s": percentile(latencies, 0.95), "max_s": max(latencies, default=None)}
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", default="hdfs://namenode:8020/wikipulse/raw_backfill")
    parser.add_argument("--version", required=True, type=int)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--rate", required=True, type=float, help="replay events per second")
    parser.add_argument("--events", required=True, type=int, help="archive rows to replay")
    parser.add_argument("--from-event-time", required=True,
                        help="UTC event time the replay starts at, in event-time order")
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--trigger-interval", default="10 seconds")
    parser.add_argument("--max-offsets-per-trigger", type=int)
    parser.add_argument("--preload", action="store_true",
                        help="finish the replay before starting the queries (drain throughput)")
    parser.add_argument("--warmup-seconds", type=int, default=60)
    parser.add_argument("--drain-seconds", type=int, default=90)
    parser.add_argument("--baseline-version", required=True)
    parser.add_argument("--bootstrap", default="kafka:29092")
    parser.add_argument("--hosts", default="cassandra")
    parser.add_argument("--checkpoint-root", default="hdfs://namenode:8020/wikipulse/checkpoints/perf")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    topic = f"wikipulse.raw.replay.{args.run_id}"
    keyspace = store.replay_keyspace(args.run_id)
    admin = KafkaAdminClient(bootstrap_servers=args.bootstrap)
    admin.create_topics([NewTopic(topic, num_partitions=args.partitions, replication_factor=1)])
    admin.close()
    replay_cmd = ["/usr/local/bin/spark-submit", "--master", "local[2]", "--driver-memory", "2g",
                  "--packages", PACKAGES, "scripts/run_wikipulse_replay.py",
                  "--archive", args.archive, "--version", str(args.version), "--bootstrap", args.bootstrap,
                  "--topic", topic, "--run-id", args.run_id, "--rate", str(args.rate),
                  "--limit", str(args.events), "--from-event-time", args.from_event_time]

    spark = spark_session(f"WikiPulse perf {args.run_id}")
    spark.conf.set("spark.sql.streaming.numRecentProgressUpdates", "100000")
    queries = []
    replay = None
    args.output.parent.mkdir(parents=True, exist_ok=True)
    replay_log = args.output.with_suffix(".replay.log")
    try:
        baseline = store.read_baseline(spark, args.baseline_version, hosts=args.hosts).cache()
        baseline.count()
        store.create_schema(keyspace, args.hosts)
        with replay_log.open("w") as log:
            replay = subprocess.Popen(replay_cmd, stdout=log, stderr=subprocess.STDOUT)
            if args.preload and replay.wait():
                raise RuntimeError(f"replay failed, see {replay_log}")
            started = datetime.now(timezone.utc)
            raw = kafka_source(spark, args.bootstrap, topic,
                               max_offsets_per_trigger=args.max_offsets_per_trigger)
            queries = start_queries(raw, baseline, checkpoint_root=f"{args.checkpoint_root}/{args.run_id}",
                                    keyspace=keyspace, hosts=args.hosts,
                                    trigger_interval=args.trigger_interval, query_suffix=f"_{args.run_id}")
            # A replay that has not finished well after its paced duration is hung (e.g. out of memory).
            replay_deadline = time.monotonic() + 3 * args.events / args.rate + 600
            while replay.poll() is None and all(q.isActive for q in queries):
                if time.monotonic() > replay_deadline:
                    raise RuntimeError(f"replay did not finish, see {replay_log}")
                time.sleep(2)
            if replay.returncode and all(q.isActive for q in queries):
                raise RuntimeError(f"replay failed, see {replay_log}")
            replay_done = datetime.now(timezone.utc)
            # A preloaded run lasts until the backlog is drained; a paced run drains for a fixed time.
            deadline = time.monotonic() + args.drain_seconds
            while all(q.isActive for q in queries) and (
                    time.monotonic() < deadline or (args.preload and not caught_up(queries, topic))):
                time.sleep(2)
        failures = [q.exception() for q in queries if q.exception() is not None]
        if failures:
            raise RuntimeError(f"streaming query failed: {failures[0]}")
        progress = {q.name: progress_rows(q) for q in queries}
    finally:
        if replay is not None and replay.poll() is None:
            replay.kill()
        stop_queries(queries)

    replay_result = next((json.loads(line) for line in replay_log.read_text().splitlines()
                          if line.startswith('{"archive_path"')), None)
    activity = next(rows for name, rows in progress.items() if name.startswith("wikipulse_activity"))
    busy = [p for p in activity if p["numInputRows"]]
    rows_in = sum(p["numInputRows"] for p in activity)
    span = None
    if busy:
        first = datetime.fromisoformat(busy[0]["timestamp"].replace("Z", "+00:00"))
        last = datetime.fromisoformat(busy[-1]["timestamp"].replace("Z", "+00:00"))
        span = (last - first).total_seconds() + busy[-1]["durationMs"]["triggerExecution"] / 1000

    seconds, seen = close_times(spark, args.bootstrap, topic)
    # A preloaded topic was closable before the queries started, so it yields no latency samples.
    measure_from = started + timedelta(seconds=args.warmup_seconds)
    summary = {
        "run_id": args.run_id, "archive": args.archive, "archive_version": args.version,
        "rate": args.rate, "events": args.events, "from_event_time": args.from_event_time,
        "partitions": args.partitions,
        "trigger_interval": args.trigger_interval, "max_offsets_per_trigger": args.max_offsets_per_trigger,
        "preload": args.preload, "started_at_utc": started.isoformat(),
        "replay_done_at_utc": replay_done.isoformat(), "replay": replay_result,
        "input_rows": rows_in, "busy_seconds": span,
        "throughput_rows_per_s": rows_in / span if span else None,
        "batches": {name: len({p["batchId"] for p in rows}) for name, rows in progress.items()},
        "batch_ms_p50": {name: percentile([p["durationMs"]["triggerExecution"] for p in rows if p["numInputRows"]], 0.5)
                         for name, rows in progress.items()},
        "batch_ms_p95": {name: percentile([p["durationMs"]["triggerExecution"] for p in rows if p["numInputRows"]], 0.95)
                         for name, rows in progress.items()},
        "max_lag_records": max((lag(p, topic) for p in activity), default=0),
        "final_lag_records": lag(activity[-1], topic) if activity else None,
        "latency": window_latencies(keyspace, args.hosts, seconds, seen, measure_from),
    }
    spark.stop()
    args.output.write_text(json.dumps(summary, indent=2, sort_keys=True))
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    sys.exit(main())
