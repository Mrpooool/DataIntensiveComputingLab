# WikiPulse C runbook: baseline, Cassandra, dashboard and experiments

C owns the shared event rules (`src/dic_pipeline/wikipulse/events.py`), the Cassandra schema and access module (`store.py`), the baseline batch job (`baseline.py`), the Grafana dashboard, and the performance, partition-pruning and batch/stream consistency experiments. Field semantics, table keys and the calls B makes are in the [B/C contract](wikipulse_bc_contract.md). Results are summarised in the [final report](wikipulse_final_report.md).

All commands run from the repository root in PowerShell. Rebuild the Spark image after changing code: `docker compose -f compose.wikipulse.yml build spark-client spark-master spark-worker`.

## Cassandra

Only Cassandra is needed for these steps. It takes about a minute to accept CQL after starting:

```powershell
docker compose -f compose.wikipulse.yml up -d cassandra
docker compose -f compose.wikipulse.yml exec cassandra cqlsh -e "describe keyspaces"
```

Writes go through the Python `cassandra-driver` (in `requirements-wikipulse.txt`), not a Spark connector, because no connector release supports Spark 4.2. `store.create_schema(keyspace, hosts)` creates the keyspace and all six tables. The baseline job calls it automatically. B calls it once for each replay keyspace (`store.replay_keyspace(run_id)`). Use host `cassandra` inside Compose containers and `localhost` on the host.

## Baseline

The job reads one pinned Delta version of A's raw archive. It counts covered 5-minute windows, computes the per-(wiki, UTC hour) mean and sample stddev, and publishes the version to `wikipulse.baseline_by_hour`, then `wikipulse.baseline_versions`. Its version is the history end, for example `20261023T1200Z`. Pass `--history-end` when the export window is known; otherwise the job uses the last complete window before the last event.

The Compose Spark worker has one core and 1 GiB, which makes a scan of a multi-day archive slow. C's batch jobs (inspection, baseline, edit-war evidence, pruning) therefore run in local mode inside `spark-client`, after raising that container's memory limit for the session. The limit returns to the Compose value when the container is recreated.

```powershell
docker update --memory 8g --memory-swap 8g wikipulse-spark-client-1
docker compose -f compose.wikipulse.yml exec spark-client /usr/local/bin/spark-submit --master "local[6]" --driver-memory 4g --conf spark.sql.shuffle.partitions=16 --packages io.delta:delta-spark_2.13:4.4.0 scripts/run_wikipulse_baseline.py --archive hdfs://namenode:8020/wikipulse/raw_backfill --version 348 --history-end 2026-10-23T12:00+00:00
```

On the host, against an unpacked handoff directory:

```powershell
$env:PYTHONPATH = "src"
.\.venv\Scripts\python.exe -m scripts.run_wikipulse_baseline --local --hosts localhost --archive data/wikipulse/wikipulse_bc_handoff_20261013_0800_compact --version 0 --history-end 2026-10-13T06:00+00:00
```

The printed summary lists the covered windows, any collection gaps, and the number of rows with at least `MIN_SAMPLES` (24) windows. B takes the version as a start-up parameter and loads it with `store.read_baseline`. Publishing a new version does not affect a running B job. Switch versions by restarting B. A replay experiment must use a baseline whose history ends no later than the replay start.

## Seven-day backfill archive (2026-10-24)

A's live capture had stopped, so C backfilled seven days from EventStreams into a separate topic and archive. The producer resumed from a `Last-Event-ID` positioned by timestamp for each data centre (`since` only returns codfw), with acknowledgements grouped per 1,000 sends (`--ack-every 1000`):

```powershell
.\.venv\Scripts\python.exe -m scripts.run_wikipulse_producer --bootstrap 127.0.0.1:9092 --topic wikipulse.raw.backfill --ack-every 1000 --state-file data/wikipulse/producer_backfill.json
docker compose -f compose.wikipulse.yml exec spark-client /usr/local/bin/spark-submit --master spark://spark-master:7077 --driver-memory 768m --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,io.delta:delta-spark_2.13:4.4.0 scripts/run_wikipulse_archive.py --topic wikipulse.raw.backfill --output hdfs://namenode:8020/wikipulse/raw_backfill --checkpoint hdfs://namenode:8020/wikipulse/checkpoints/raw_backfill --trigger-interval "1 minute"
```

On the host, use `127.0.0.1:9092`; `localhost` resolves to IPv6 first and the producer cannot connect. EventStreams keeps about seven days and disconnects about every two minutes. A second concurrent historical reader from the same address is cut off immediately.

- Pinned version **348** of `hdfs://namenode:8020/wikipulse/raw_backfill` has 20,398,815 rows and as many physical keys. That equals the topic's end offsets (6,795,964 + 6,801,099 + 6,801,752), so nothing was lost between Kafka and Delta. The table is 16.6 GiB in 1,777 files.
- Wikipedia article edits and new pages run from 2026-10-17 11:59:55 to 2026-10-24 18:15:25 UTC: 2,426,933 rows, 11.9% of the raw stream, with 6,569 repeated source IDs. Wikipedia projects as a whole are 28.9% of the raw stream, which averages 2.9 M events per day.
- The archive is partitioned by Kafka ingest date, so every backfilled event is in the 2026-10-24 partition. Backfill order is fetch order: sparse codfw events run days ahead of eqiad. Replays of this archive therefore use `--from-event-time`, which replays in event-time order. Without it, the first future codfw event would push the watermark past all later eqiad events.
- Baseline `20261023T1200Z` covers history `[2026-10-17 12:00, 2026-10-23 12:00)`. All 1,728 windows are covered, with no collection gaps. It has 8,040 rows (335 wikis × 24 hours), and every row has 72 samples (≥ 24).

## Dashboard

Grafana 12.2 with the HadesArchitect Cassandra data source 3.3.0 is provisioned from `docker/grafana` and starts with Compose:

```powershell
docker compose -f compose.wikipulse.yml up -d cassandra grafana
```

Open `http://localhost:3000`. Anonymous users have read-only access, and the WikiPulse dashboard is the home page. The variables are the keyspace (`wikipulse` or a `wikipulse_replay_<run_id>` keyspace), the UTC day, the focus wiki and the baseline version.

| Panel | Source | Notes |
| --- | --- | --- |
| Hot pages | `hot_pages`, newest window of the day | `LIMIT 10` on a `window_end DESC` partition |
| Edit-war candidates | `edit_war_candidates` | newest window per page; labelled as candidates, not confirmed wars |
| Bot vs human, bot share | `activity_5m` | selected wiki, completed 5-minute windows |
| Edits per 5 minutes, language share | `activity_5m` | focus wikis; top 15 of all editions by share |
| Language activity heatmap | `baseline_by_hour` | mean edits per 5 minutes by wiki and UTC hour |
| Activity and bot activity heatmaps | `activity_5m` | status-history rows per focus wiki on the UTC time axis |
| Anomaly status, detected anomalies | `anomalies` | z-score only for `ok` rows |

Every panel shows only rows written after the 2-minute watermark closed the window. Window times are UTC. The plugin renders a null z-score as 0, so the status panel shows `status` instead of the z-score. Grafana's `formatTime` transformation uses the browser time zone, so the hourly heatmaps use a UTC time axis instead of a formatted hour.

## Live end-to-end check (2026-10-24)

```powershell
.\.venv\Scripts\python.exe -m scripts.run_wikipulse_producer --bootstrap 127.0.0.1:9092 --state-file data/wikipulse/producer_live.json
docker compose -f compose.wikipulse.yml exec spark-client /usr/local/bin/spark-submit --master spark://spark-master:7077 --driver-memory 768m --conf spark.sql.shuffle.partitions=8 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,io.delta:delta-spark_2.13:4.4.0 scripts/run_wikipulse_streaming.py --baseline-version 20261023T1200Z
```

`spark.sql.shuffle.partitions=8` replaces Spark's default of 200, which would run 200 state-store tasks per stateful batch on the single executor core. The value is fixed in a query's checkpoint.

- **Capture:** the producer captured 137,698 events from 18:17 to 19:40 UTC, about 28 per second.
- **Results and dashboard:** B's job wrote all four tables to `wikipulse`, and the dashboard showed every panel except edit-war candidates, which had none in that hour. Detected anomalies included enwiki `revert_count` at 18:40 (151 against a baseline of about 17, z = 16.7).
- **Restart:** the driver was killed at 19:18:21 and restarted from the same checkpoint. Batch IDs continued, and `activity_5m` for enwiki stayed contiguous from 18:25 to 19:15 with no missing or duplicated window. Primary-key overwrites make a replayed batch idempotent.
- **Capture lag:** with its default `--ack-every 1`, the producer waits for every Kafka acknowledgement and peaks at about 32 events per second. That is below the evening rate of the feed, so its event-time lag grew steadily from 45 s at 18:29 to 938 s at 19:41. The dashboard was about 15 minutes behind for that reason; Spark was not the bottleneck, as the replay experiment below shows (it keeps up with 400 events/s). Run live capture with grouped acknowledgements, for example `--ack-every 100`. It stays at least once: the SSE ID is saved only after the whole group is acknowledged, and a restart re-sends at most one group, which `source_id` de-duplication absorbs.
- The first window (18:25) is partial because capture started at 18:17.

## Experiments

### Throughput and latency

`scripts/run_wikipulse_perf.py` performs one run:

1. It creates a fresh topic with the requested number of partitions.
2. It starts A's replay of the pinned archive as a separate local-mode Spark process: `--from-event-time 2026-10-23T12:00Z`, the same 50,000 rows every time, at a fixed rate.
3. It runs B's three queries on the cluster into `wikipulse_replay_<run_id>` and stops 90 s after the replay ends.

Each run writes the following measures to `/tmp/wikipulse_perf/<run_id>.json` in `spark-client`:

- **Throughput:** input rows divided by the busy span of the activity query, from `recentProgress`.
- **Batch duration:** p50 and p95 of `triggerExecution`.
- **Backlog:** the maximum and final Kafka backlog in records.
- **Per-window latency:** `written_at` minus the Kafka append time of the first replayed event whose event time passed `window_end + 2 min`. Windows that became closable in the first 20 s are excluded.

Watermark waiting is window semantics, so latency counts only trigger wait and processing. `scripts/run_wikipulse_perf_grid.sh` runs the whole grid:

- 3 partitions, rates 100/200/400 events/s × trigger 5 s/30 s;
- 1 or 3 partitions at 200 events/s with a 10 s trigger;
- two repetitions of each;
- one drain run per partition count: the same 50,000 rows preloaded, `maxOffsetsPerTrigger` 10,000.

```powershell
docker compose -f compose.wikipulse.yml exec -T spark-client sh scripts/run_wikipulse_perf_grid.sh 20261023T1200Z g2
```

The natural rate of this slice is about 34 raw events per second. Each replay uses a two-core local Spark with a 2 GiB driver; with 1 GiB, sorting the 50,000-row slice ran out of memory (300,000 rows did not fit in 2 GiB either). A replay that does not finish in three times its paced duration plus 10 minutes fails the run instead of hanging. The backlog fields in the JSON are meaningful only for the drain runs, because a paced run without `maxOffsetsPerTrigger` reads everything available in each batch.

Results (2026-10-24; one row per run in [wikipulse_perf_results.csv](wikipulse_perf_results.csv)). Values are means of two repetitions; latency is per closed window, with 15 hot-page windows and 3 activity windows per run. In the paced runs every configuration kept up with its input: the replay hit its target rate, throughput equalled the offered rate, and the last batch finished 14–34 s after the replay ended.

| Partitions | Rate (events/s) | Trigger | Activity batch p50 | Hot pages latency p50 / p95 | Activity latency p50 |
| --- | --- | --- | --- | --- | --- |
| 3 | 100 | 5 s | 11.4 s | 17.3 / 25.8 s | 28.2 s |
| 3 | 200 | 5 s | 11.7 s | 18.4 / 30.9 s | 37.0 s |
| 3 | 400 | 5 s | 12.1 s | 16.7 / 21.6 s | 26.9 s |
| 3 | 200 | 10 s | 9.0 s | 19.2 / 25.4 s | 24.7 s |
| 1 | 200 | 10 s | 7.0 s | 19.4 / 23.9 s | 14.6 s |
| 3 | 100 | 30 s | 9.3 s | 48.3 / 60.1 s | 47.0 s |
| 3 | 200 | 30 s | 12.6 s | 50.7 / 63.6 s | 48.0 s |
| 3 | 400 | 30 s | 11.4 s | 53.9 / 63.9 s | 45.3 s |

For the drain runs, all 50,000 rows were preloaded and read in 10,000-row batches. One partition drained at 737 events/s and three partitions at 634 events/s, about 20× the natural rate.

- **Trigger interval decides latency.** A window is emitted only by the batch after the one that advanced the watermark. With a 30 s trigger, a closable window therefore waits on average half an interval to be read, a full interval for the next batch, and that batch's processing: about 50 s. With 5 s or 10 s, batches (7–12 s) already take longer than the interval, so the queries run back to back and latency is about two batch durations, 17–19 s for hot pages.
- **Input rate barely matters** up to 400 events/s, because per-batch overhead dominates: three stateful queries, state-store commits and Cassandra writes from the driver. Bigger batches raise throughput without raising latency.
- **Kafka partitions make no measurable difference** on one executor core, and one partition was not slower. The spread between repetitions (for example activity latency p50 of 22 s and 34 s at 100 events/s with a 5 s trigger) is as large as the partition effect.
- **Bottleneck:** the stream's limit is the single core shared by three queries, roughly 650–750 events/s in large batches. The producer's per-event acknowledgement (about 32 events/s) is the tighter limit.

### Delta partition pruning

`scripts/run_wikipulse_pruning.py` copies pinned version 348 into `raw_backfill_by_event_date_v348`, partitioned by UTC event date. It then runs the same one-day job three times with and without a predicate on the partition column, alternating the order. The job is the baseline's covered-window 5-minute counts for 2026-10-20. Delta data skipping is off in both variants, so the difference is partition pruning alone. Files and bytes come from the scan nodes in the Spark UI REST API.

```powershell
docker compose -f compose.wikipulse.yml exec spark-client /usr/local/bin/spark-submit --master "local[4]" --driver-memory 4g --conf spark.sql.shuffle.partitions=16 --packages io.delta:delta-spark_2.13:4.4.0 scripts/run_wikipulse_pruning.py --archive hdfs://namenode:8020/wikipulse/raw_backfill --version 348 --day 2026-10-20 --runs 3 --output /tmp/pruning_v348.json
```

| Variant | Files read (all scans) | File bytes read | Median time (3 runs) |
| --- | --- | --- | --- |
| Pruning on (`event_date_utc = 2026-10-20`) | 292 | 9.4 GB | 51.4 s |
| Pruning off (event-time range only) | 4,580 | 71.3 GB | 60.9 s |

The two outputs were identical: 74,880 rows, i.e. 260 wikis × 288 windows. Pruning cut the files read by 94% and the file bytes by 87%, but the run time by only 16%. Two things limit the time saving:

- The off variant still benefits from Parquet row-group min/max filtering on `source_event_unix`. The copy keeps the archive's near-event-time order, so most row groups of other days are skipped after the footers are read.
- Each job has fixed costs: planning, the gap scan and the zero-filled window grid.

The "file bytes" figure counts whole files, not the columns actually read. The copied table has eight event-date partitions for 2026-10-17 to 10-24, plus 498 rows without an event time and a long tail of stray old timestamps going back to 2004.

### Batch/stream consistency

`scripts/check_wikipulse_consistency.py --run-id <id>` recounts a replay topic in batch with the same `events.py` rules and B's `activity_windows`. It then compares every closed window with `activity_5m` in the run's keyspace.

On run `g2_r100_t5_p3_a` the batch recount and the stream agreed on all 198 closed windows (`window_end` ≤ 12:15:23) and on 8,821 edits. No window was missing from the stream and no event was dropped as late, because the replay is in event-time order.

```powershell
docker compose -f compose.wikipulse.yml exec spark-client /usr/local/bin/spark-submit --master "local[2]" --driver-memory 1g --conf spark.sql.shuffle.partitions=8 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,io.delta:delta-spark_2.13:4.4.0 scripts/check_wikipulse_consistency.py --run-id g2_r100_t5_p3_a
```

### Edit-war precision

B's `scripts/evaluate_wikipulse_edit_wars.py` exports page-level evidence from version 348. It now caches only the columns it needs; the whole parsed archive did not fit in memory. It found 1,052 candidate windows on 209 pages: 185 enwiki, 10 dewiki, 8 frwiki and 6 zhwiki. A seeded random sample of 50 pages (`artifacts/wikipulse/edit_war_sample50.json`, seed 20261024) was labelled with B's rubric from the ordered edit summaries. Every label and its evidence are in [the sample record](wikipulse_c_edit_war_sample.md):

- **Labels:** 8 positive, 33 negative and 9 uncertain. Uncertain pages are mostly bare "Undo"/"Revert" exchanges.
- **Precision:** 8/41 = **19.5%** with uncertain pages excluded, Wilson 95% interval 10.2–34.0%. Counting uncertain pages as negative gives 8/50 = 16.0%.
- **Negatives:** 14 patrol-tool reverts (Huggle, AntiVandal and similar), 14 vandalism rollbacks and 5 sockpuppet or block-evasion cleanups.

The estimate agrees with B's 1/4 on the 8.2-hour snapshot. The labels come from a single AI-assisted rater using edit summaries only and have not had a second review. The sample has only 6 non-English pages.

## Tests

```powershell
$env:PYTHONPATH = "src"
.\.venv\Scripts\python.exe -m unittest tests.test_wikipulse_events tests.test_wikipulse_baseline tests.test_wikipulse_store -v
```

The store test and the baseline publish test need the local Cassandra. They are skipped when it is not reachable, and each run uses its own temporary keyspace.

## 2026-10-13 run on the handoff snapshot

- Input: `wikipulse_bc_handoff_20261013_0800_compact`, Delta version 0, history `[2026-10-12 21:50, 2026-10-13 06:00)` UTC.
- 98 of 98 windows were covered; there were no gaps in event time. The job published 1,926 rows (214 wikis × 9 UTC hours) as version `20261013T0600Z`.
- enwiki averaged 270–465 edits and 7–18 reverts per 5 minutes, depending on the hour.
- No row reaches 24 samples, because each hour has at most 12 windows. B therefore reports every anomaly as `insufficient_samples` on this version; use `20261023T1200Z` instead.
