"""Compare B's streamed 5-minute activity with a batch recount of the same replay topic.

Both sides read exactly the records in the replay topic and apply the shared rules in
``events.py``, so a closed window should match unless the stream dropped events
that arrived later than the 2-minute watermark. Windows the watermark has not closed
(``window_end`` > max event time - 2 minutes) are excluded.
"""

import argparse
import json
from datetime import timedelta, timezone

from pyspark.sql import functions as F

from dic_pipeline.wikipulse import store
from dic_pipeline.wikipulse.archive import spark_session
from dic_pipeline.wikipulse.events import analysis_events
from dic_pipeline.wikipulse.streaming import activity_windows

WINDOW_SECONDS = 300


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--topic", help="default: wikipulse.raw.replay.<run-id>")
    parser.add_argument("--bootstrap", default="kafka:29092")
    parser.add_argument("--hosts", default="cassandra")
    args = parser.parse_args()
    topic = args.topic or f"wikipulse.raw.replay.{args.run_id}"
    keyspace = store.replay_keyspace(args.run_id)

    spark = spark_session("WikiPulse batch/stream consistency")
    try:
        raw = (spark.read.format("kafka").option("kafka.bootstrap.servers", args.bootstrap)
               .option("subscribe", topic).option("startingOffsets", "earliest").load()
               .selectExpr("CAST(value AS STRING) AS raw_json"))
        events = analysis_events(raw).cache()
        max_event = events.agg(F.max("event_time")).first()[0].replace(tzinfo=timezone.utc)
        closed_until = max_event - timedelta(minutes=2)
        # B's own window code on a batch frame; the container runs in UTC, so collected
        # naive timestamps are UTC.
        batch = {(r.wiki, r.window_start.replace(tzinfo=timezone.utc)): (r.edit_count, r.bot_count)
                 for r in activity_windows(events).collect()}
    finally:
        spark.stop()
    batch = {k: v for k, v in batch.items() if k[1] + timedelta(seconds=WINDOW_SECONDS) <= closed_until}
    stream = {(r.wiki, r.window_start.replace(tzinfo=timezone.utc)): (r.edit_count, r.bot_count)
              for r in store.session(args.hosts).execute(
                  f"SELECT wiki, window_start, edit_count, bot_count FROM {keyspace}.activity_5m")}
    stream = {k: v for k, v in stream.items() if k[1] + timedelta(seconds=WINDOW_SECONDS) <= closed_until}
    keys = batch.keys() | stream.keys()
    mismatched = sorted((k for k in keys if batch.get(k) != stream.get(k)), key=lambda k: (k[1], k[0]))
    summary = {
        "run_id": args.run_id, "topic": topic, "closed_until_utc": closed_until.isoformat(),
        "windows_batch": len(batch), "windows_stream": len(stream),
        "windows_exact_match": len(keys) - len(mismatched),
        "edits_batch": sum(v[0] for v in batch.values()), "edits_stream": sum(v[0] for v in stream.values()),
        "missing_in_stream": sum(1 for k in batch if k not in stream),
        "mismatch_examples": [{"wiki": k[0], "window_start": k[1].isoformat(),
                               "batch": batch.get(k), "stream": stream.get(k)} for k in mismatched[:10]],
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
