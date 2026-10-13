# WikiPulse A: input and raw archive contract

Version 1, 2026-10-07. This contract covers A's capture and archive only. B owns analytical parsing, filtering and deduplication; C owns historical analysis and serving.

## Live Kafka input

- Source: `https://stream.wikimedia.org/v2/stream/recentchange`, no project filter. Topic: `wikipulse.raw.live`. Value: original SSE `data` bytes joined with `\n` for multiline SSE frames. Every SSE frame with data is captured, regardless of event type. Key: UTF-8 `meta.id` when present, otherwise SHA-256 hex of the original value.
- Kafka headers: `sse_id`, `sse_event_type`, `source_id`, `source_event_unix` (epoch seconds), `received_at_utc` (UTC ISO 8601). Missing values are empty bytes. Kafka timestamp is broker/producer record time, not source event time.
- Producer waits for Kafka `acks=all` before advancing the local state file. On reconnect it sends `Last-Event-ID` with the last acknowledged SSE ID. A crash between acknowledgement and state persistence can duplicate an event. Kafka producer retries can also duplicate it. `meta.id` is the preferred logical deduplication key; hash fallback may merge distinct identical payloads, so consumers should inspect provenance. No continuous exactly-once claim is made.
- The state file must be persisted and exclusive to one producer. EventStreams retention and resume behavior are controlled by Wikimedia; `Last-Event-ID` does not guarantee gap-free recovery after a long outage. Producer logs UTC connection/interruption times and periodic counts; keep logs for coverage assessment. No event is interpreted as zero activity only when the interval is proven covered.

## HDFS Delta raw archive

- Path: `hdfs://namenode:8020/wikipulse/raw_events` (Delta table). Checkpoint: `hdfs://namenode:8020/wikipulse/checkpoints/raw_live`. One archive writer/checkpoint pair per topic; never share the checkpoint with a replay archive.
- Partition: `ingest_date_utc`, derived from Kafka timestamp in a UTC Spark session. It is an ingestion partition, not an event date. Kafka `topic`, `partition`, `offset` form the physical record identity and are stored in `kafka_topic`, `kafka_partition`, `kafka_offset`.
- `raw_value` is the unmodified Kafka binary value. `raw_json` is its UTF-8 decoded view for B/C. `kafka_key`, `kafka_timestamp_utc`, `kafka_timestamp_type`, `kafka_headers`, `sse_id`, `sse_event_type`, `source_id`, `source_event_unix`, and `received_at_utc` retain ingestion provenance. Invalid JSON remains a row; no filtering or schema enforcement occurs in A's archive.
- `startingOffsets=earliest` applies on first start only; subsequent starts follow the checkpoint. Spark Delta sink uses checkpointed microbatch commits. Preserve Kafka retention until the archive catches up. Batch ID and source offset ranges are in Spark checkpoint/Delta transaction history; pin a Delta version for reproducible reads. Do not delete/compact old versions before an experiment finishes.
- B/C should parse `raw_json`, reject malformed or out-of-scope records at their analytical boundary, and deduplicate by `source_id` where available. They must use source event time for windows, not Kafka/receive time. Empty IDs and late events require explicit treatment in their modules.

## Fixed replay

- Replay reads one immutable Delta `versionAsOf`, globally ordered by `(kafka_timestamp_utc, kafka_topic, kafka_partition, kafka_offset)`. It schedules one send per `1/rate` seconds on a monotonic clock; a slow Kafka broker may yield lower achieved throughput, which the output manifest exposes through start/end timestamps and count. There is no arbitrary sleep per partition or unfair partition priority except the original timestamp order.
- Each replay message preserves original key, raw value and headers, and adds `replay_run_id`, `archive_version`, `origin_topic`, `origin_partition`, `origin_offset`. Use a fresh target topic and run ID per run. Re-running into a populated topic creates duplicates. `--limit` chooses the first N records in the pinned order.
- Replay finishes after the pinned row count or limit has been acknowledged by Kafka; no synthetic event enters the raw topic. B's 2-minute event-time watermark may leave final windows open. Downstream evaluation must explicitly state its tail completion rule and should not count old source event age as system processing latency.
