"""Compare a bounded replay topic with the pinned raw Delta input."""

import argparse
import json

from kafka import KafkaConsumer

from dic_pipeline.wikipulse.archive import spark_session


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", default="hdfs://namenode:8020/wikipulse/raw_events")
    parser.add_argument("--version", required=True, type=int)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--limit", required=True, type=int)
    parser.add_argument("--bootstrap", default="kafka:29092")
    args = parser.parse_args()
    if args.limit <= 0:
        parser.error("limit must be positive")

    spark = spark_session("WikiPulse replay check")
    frame = (spark.read.format("delta").option("versionAsOf", args.version)
             .load(args.archive).orderBy("kafka_timestamp_utc", "kafka_topic",
                                          "kafka_partition", "kafka_offset")
             .limit(args.limit))
    expected = {
        (row.kafka_topic, row.kafka_partition, row.kafka_offset): (
            bytes(row.raw_value), bytes(row.kafka_key) if row.kafka_key else None,
            [(header.key, bytes(header.value) if header.value else b"")
             for header in (row.kafka_headers or [])],
        )
        for row in frame.select("kafka_topic", "kafka_partition", "kafka_offset",
                                "raw_value", "kafka_key", "kafka_headers").toLocalIterator()
    }
    spark.stop()
    if len(expected) != args.limit:
        raise ValueError("source version has too few rows or duplicate physical keys")

    consumer = KafkaConsumer(args.topic, bootstrap_servers=args.bootstrap.split(","),
                             auto_offset_reset="earliest", enable_auto_commit=False,
                             consumer_timeout_ms=10000)
    seen = set()
    try:
        for message in consumer:
            headers = dict(message.headers)
            if headers.get("replay_run_id") != args.run_id.encode("utf-8"):
                raise ValueError("unexpected replay run ID")
            if headers.get("archive_version") != str(args.version).encode("ascii"):
                raise ValueError("unexpected archive version")
            origin = (headers["origin_topic"].decode("utf-8"),
                      int(headers["origin_partition"]), int(headers["origin_offset"]))
            original_headers = [(key, value) for key, value in message.headers
                                if key not in {"replay_run_id", "archive_version",
                                               "origin_topic", "origin_partition", "origin_offset"}]
            if origin in seen or expected.get(origin) != (
                message.value, message.key, original_headers
            ):
                raise ValueError(f"duplicate or changed source event: {origin}")
            seen.add(origin)
    finally:
        consumer.close()
    if seen != expected.keys():
        raise ValueError(f"expected {len(expected)} events, found {len(seen)}")
    print(json.dumps({"run_id": args.run_id, "verified": len(seen),
                      "archive_version": args.version, "topic": args.topic}, sort_keys=True))


if __name__ == "__main__":
    main()
