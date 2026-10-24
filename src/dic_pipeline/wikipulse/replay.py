"""Deterministic, rate-limited replay from a pinned raw Delta version."""

from __future__ import annotations

import json
import math
import time
from datetime import datetime, timezone

from delta.tables import DeltaTable
from kafka import KafkaProducer
from pyspark.sql import SparkSession, functions as F

ACK_GROUP = 1000


def _binary(value) -> bytes | None:
    return None if value is None else bytes(value)


def _replay_headers(row, run_id: str, version: int) -> list[tuple[str, bytes]]:
    headers = [(entry["key"], _binary(entry["value"]) or b"")
               for entry in (row["kafka_headers"] or [])]
    headers.extend([
        ("replay_run_id", run_id.encode("utf-8")),
        ("archive_version", str(version).encode("ascii")),
        ("origin_topic", row["kafka_topic"].encode("utf-8")),
        ("origin_partition", str(row["kafka_partition"]).encode("ascii")),
        ("origin_offset", str(row["kafka_offset"]).encode("ascii")),
    ])
    return headers


def replay(spark: SparkSession, archive_path: str, version: int, bootstrap: str,
           topic: str, run_id: str, events_per_second: float,
           limit: int | None = None) -> dict:
    """Send exactly one archived Kafka row per rate slot in stable archive order."""
    try:
        valid_rate = math.isfinite(events_per_second) and events_per_second > 0
    except (TypeError, ValueError):
        valid_rate = False
    if (not isinstance(version, int) or version < 0
            or not archive_path or not bootstrap or not topic
            or topic == "wikipulse.raw.live" or not run_id
            or not valid_rate
            or (limit is not None and limit <= 0)):
        raise ValueError("invalid archive, target, run ID, version, rate or limit")
    history = DeltaTable.forPath(spark, archive_path).history()
    if not history.where(F.col("version") == version).limit(1).count():
        raise ValueError(f"Delta version {version} is unavailable")
    frame = (spark.read.format("delta").option("versionAsOf", version).load(archive_path)
             .orderBy("kafka_timestamp_utc", "kafka_topic", "kafka_partition", "kafka_offset"))
    if limit is not None:
        frame = frame.limit(limit)
    count = frame.count()
    if not count:
        raise ValueError("Pinned archive is empty")
    producer = KafkaProducer(bootstrap_servers=bootstrap.split(","), acks="all", retries=10,
                             max_in_flight_requests_per_connection=1, linger_ms=20)
    started = datetime.now(timezone.utc).isoformat()
    clock_start = time.monotonic()
    sent = 0
    pending = []
    try:
        for row in frame.toLocalIterator():
            target = clock_start + sent / events_per_second
            delay = target - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            pending.append(producer.send(topic, key=_binary(row["kafka_key"]),
                                         value=bytes(row["raw_value"]),
                                         headers=_replay_headers(row, run_id, version)))
            sent += 1
            # Waiting on each send caps the rate near 100/s; acknowledge in groups instead.
            if len(pending) >= ACK_GROUP:
                for future in pending:
                    future.get(timeout=60)
                pending.clear()
        for future in pending:
            future.get(timeout=60)
    finally:
        producer.flush(timeout=60)
        producer.close()
    result = {"run_id": run_id, "archive_path": archive_path, "archive_version": version,
              "target_topic": topic, "source_rows": count, "sent": sent,
              "rate_events_per_second": events_per_second, "started_at_utc": started,
              "finished_at_utc": datetime.now(timezone.utc).isoformat()}
    print(json.dumps(result, sort_keys=True))
    return result
