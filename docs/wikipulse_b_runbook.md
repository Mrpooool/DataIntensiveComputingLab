# WikiPulse B runbook: Structured Streaming analytics

Role B consumes A's raw Kafka topic, reuses the shared rules in `events.py`, and writes four completed-window result tables through C's `store.py`. The exact fields and business definitions remain those in the [B/C contract](wikipulse_bc_contract.md).

## What the job runs

`scripts/run_wikipulse_streaming.py` starts three independent queries and checkpoints:

1. `hot_pages`: non-bot edits in a 10-minute window sliding every minute, ranked Top 10 per focus wiki and completed window.
2. `edit_war_candidates`: non-bot reverts in a 30-minute window sliding every 5 minutes; a candidate needs at least three reverts, two users and an A → other → A sequence.
3. `activity_anomalies`: 5-minute activity for every Wikipedia edition, plus `edit_count` and `revert_count` anomaly rows against one fixed baseline version.

The shared 2-minute event-time watermark closes windows. All three queries use append mode, so Cassandra never receives a provisional window. The first use of a checkpoint applies `--starting-offsets`; later starts resume the stored Kafka offsets and state.

## Prerequisites

- A's Kafka topic and Compose services are running.
- C has published a baseline version in `wikipulse.baseline_versions`. The 2026-10-13 handoff version is `20261013T0600Z`, but its 8.2 hours of history are below the 24-sample threshold, so anomaly rows correctly report `insufficient_samples`.
- Rebuild the Spark image after changing source code: `docker compose -f compose.wikipulse.yml build spark-client spark-master spark-worker`.

## Live run

Run this in its own terminal from the repository root:

```powershell
docker compose -f compose.wikipulse.yml exec spark-client /usr/local/bin/spark-submit `
  --master spark://spark-master:7077 `
  --driver-memory 768m `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,io.delta:delta-spark_2.13:4.4.0 `
  scripts/run_wikipulse_streaming.py `
  --baseline-version 20261013T0600Z
```

The default source is `wikipulse.raw.live`, the result keyspace is `wikipulse`, and the checkpoint root is `hdfs://namenode:8020/wikipulse/checkpoints/analysis_live`. Each completed micro-batch prints JSON with `batch_id` and rows written; save these lines together with Spark progress for C's performance experiment.

Changing `--baseline-version` requires stopping and restarting the job. Do not delete or reuse a live checkpoint for another topic or experiment.

## Isolated replay

A replay topic must have a lower-case `run_id`. The CLI refuses a non-live topic without one, and refuses a `run_id` on the live topic. Results go to `wikipulse_replay_<run_id>` and checkpoints to a run-specific subdirectory.

```powershell
docker compose -f compose.wikipulse.yml exec spark-client /usr/local/bin/spark-submit `
  --master spark://spark-master:7077 `
  --driver-memory 768m `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,io.delta:delta-spark_2.13:4.4.0 `
  scripts/run_wikipulse_streaming.py `
  --topic wikipulse.raw.replay.demo1 `
  --run-id demo1 `
  --baseline-version 20261013T0600Z `
  --max-offsets-per-trigger 200
```

A finite replay does not fabricate a future event to advance the watermark. Compare only windows whose `window_end` is no later than the maximum observed event time minus two minutes; exclude the unfinished tail.

## Tests

The six batch-fixture tests require Spark but not Kafka or Cassandra:

```powershell
$env:PYTHONPATH = "src"
.\.venv\Scripts\python.exe -m unittest tests.test_wikipulse_streaming.StreamingAnalyticsTests -v
```

The opt-in integration test creates a temporary keyspace, feeds three file micro-batches to advance the watermark, verifies all four Cassandra tables, and drops the temporary keyspace. Set `WIKIPULSE_CASSANDRA_HOSTS` to a reachable contact point before running `StreamingSinkTests`.

Verified on 2026-10-24 in the Compose Spark 4.2/Cassandra 4.1 environment: six transformation tests passed and the four-table streaming sink test passed. A combined A/events/baseline/B regression had 22 passing tests; two pre-existing Cassandra classes skipped inside the container because they default to host `localhost`, while B's opt-in integration used the Compose hostname `cassandra` successfully.

## Edit-war precision evaluation

Generate page-level evidence from a pinned archive version:

```powershell
docker compose -f compose.wikipulse.yml exec spark-client /usr/local/bin/spark-submit `
  --master spark://spark-master:7077 `
  --driver-memory 768m `
  --packages io.delta:delta-spark_2.13:4.4.0 `
  scripts/evaluate_wikipulse_edit_wars.py `
  --archive hdfs://namenode:8020/wikipulse/raw_events `
  --version <delta-version> `
  --output /tmp/edit_war_candidates.json

New-Item -ItemType Directory -Force artifacts/wikipulse | Out-Null
docker compose -f compose.wikipulse.yml cp `
  spark-client:/tmp/edit_war_candidates.json artifacts/wikipulse/edit_war_candidates.json
```

The script excludes the unfinished watermark tail, collapses overlapping windows to one strongest window per page, and exports the ordered revert summaries needed for human labels. The fixed 8.2-hour handoff produced 24 candidate windows across 4 pages. The complete page-level audit found one substantive content dispute and three anti-vandalism/sock-cleanup false positives, for precision `1/4 = 25%`; see [the evaluation record](wikipulse_b_edit_war_evaluation.md) for the rubric, labels and limitations.

## Handoff to C

C reads only completed rows from:

- `hot_pages` for latest per-wiki Top 10;
- `edit_war_candidates` for the latest overlapping window per `(wiki, title)`;
- `activity_5m` for activity series, language share and bot/human views;
- `anomalies` for detection status and confirmed `is_anomaly = true` rows.

The `written_at` and UTC `day` columns are added by `store.py`. Repeated processing of the same completed window overwrites the same Cassandra primary key, so a retry does not add counts. For the current short baseline, the dashboard must display `insufficient_samples` rather than presenting those rows as “normal”.
