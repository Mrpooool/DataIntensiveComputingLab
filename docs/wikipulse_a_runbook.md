# WikiPulse A runbook

Use Docker Desktop with at least 12 GiB available to containers. Python and Spark versions in the custom image are 3.11.9 and 4.2.0; Delta is 4.4.0. Kafka is 3.9.1; Cassandra is 4.1.9; HDFS images use Hadoop 3.2.1. This combination passed the A-chain smoke test below. The Compose limits total about 9.3 GiB, excluding Docker overhead. The Spark client has a 2 GiB limit; run jobs with a 768 MiB driver heap to leave room for Python and connector resolution. Large replays still need a small fixed `--limit` first to check memory use.

From the repository root in PowerShell:

```powershell
docker compose -f compose.wikipulse.yml build
docker compose -f compose.wikipulse.yml up -d
docker compose -f compose.wikipulse.yml exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic wikipulse.raw.live --partitions 3 --replication-factor 1
docker compose -f compose.wikipulse.yml exec spark-client /usr/local/bin/spark-submit --master spark://spark-master:7077 --driver-memory 768m --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,io.delta:delta-spark_2.13:4.4.0 scripts/run_wikipulse_archive.py
```

Before using Docker, run the local role A smoke suite. It exercises SSE framing, key and timestamp extraction, acknowledged state persistence, and the Spark archive projection without reading the Taxi data:

```powershell
$env:PYTHONPATH = "src"
.\.venv\Scripts\python.exe -m unittest tests.test_wikipulse_a -v
```

Run archive in its own terminal. Install the WikiPulse dependencies in the host virtual environment, then start the producer in a second terminal:

```powershell
$env:PYTHONPATH = "src"
.\.venv\Scripts\python.exe -m pip install -r requirements-wikipulse.txt
.\.venv\Scripts\python.exe -m scripts.run_wikipulse_producer
```

The producer state file defaults to `data/wikipulse/producer_A.json`; `data/` is excluded from Git. Its periodic JSON log includes event count, malformed JSON count, reconnect count, source event time range, start/report time, and last SSE ID. Save the log for collection coverage evidence. Kafka host access uses `localhost:9092`; containers use `kafka:29092`.

After stopping the archive job, inspect a fixed Delta version or the latest snapshot. `rows` and `physical_keys` should match for a single archive writer; the live topic count should agree after its offsets have caught up. Spark master UI is at `http://localhost:18080`.

```powershell
docker compose -f compose.wikipulse.yml exec spark-client /usr/local/bin/spark-submit --master spark://spark-master:7077 --driver-memory 768m --packages io.delta:delta-spark_2.13:4.4.0 scripts/inspect_wikipulse_archive.py
```

The 2026-10-13 08:00 Berlin time B/C handoff covers Kafka timestamps from `2026-10-12 21:48:48 UTC` up to but excluding `2026-10-13 06:00:00 UTC`. It excludes the malformed smoke fixture. Share the complete `wikipulse_bc_handoff_20261013_0800_compact` directory, including `_delta_log`, with B/C. This is one Delta table partitioned by `ingest_date_utc`, with one Parquet file in each of its two date partitions. The live HDFS archive continues to use small files from its 10-second microbatches; compact only a fixed snapshot after capture. The raw archive remains unchanged for audit and replay.

The verified compact handoff is `data/wikipulse/wikipulse_bc_handoff_20261013_0800_compact.zip` (SHA-256 `13C7477A3393DA7CB74C9DD14AB5E528502480434331EEB8BFBFA1A4025DDB91`). It contains Delta version 0. The companion manifest reports 783,315 rows and physical keys, 105,478 Wikipedia article edit/new rows, and 12 repeated source IDs. B/C should unpack the archive and read the directory as Delta, then deduplicate logical source IDs in their analytical layer. The one-off scripts used for this export have been removed. A later seven-day export needs a new tool that keeps one Delta table partitioned by UTC date and controls Parquet file size per partition.

For replay, stop archive at a known point and inspect the Delta version with Spark/Delta history. Create a fresh topic for each run. Example for version 0 at 200 events/second:

```powershell
docker compose -f compose.wikipulse.yml exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --topic wikipulse.raw.replay.demo1 --partitions 3 --replication-factor 1
docker compose -f compose.wikipulse.yml exec spark-client /usr/local/bin/spark-submit --master spark://spark-master:7077 --driver-memory 768m --packages io.delta:delta-spark_2.13:4.4.0 scripts/run_wikipulse_replay.py --version 0 --topic wikipulse.raw.replay.demo1 --run-id demo1 --rate 200 --limit 1000
```

Replay prints a JSON manifest with version, count, target topic, rate and timestamps. Record it with the Kafka partition count, B's trigger interval, code revision and input version for comparisons. Do not use a live topic as replay target. Because the replay stops at a finite input, B must specify how its final event-time windows are closed; replay itself inserts no fabricated recentchange event.

For a bounded replay, verify its payload, key and original headers against the pinned Delta rows:

```powershell
docker compose -f compose.wikipulse.yml exec spark-client /usr/local/bin/spark-submit --master spark://spark-master:7077 --driver-memory 768m --packages io.delta:delta-spark_2.13:4.4.0 scripts/check_wikipulse_replay.py --version 0 --topic wikipulse.raw.replay.demo1 --run-id demo1 --limit 1000
```

To check malformed input, send one fixture to the live topic while the archive job runs, then inspect the archive again. `malformed_smoke_rows` should increase by one and `rows` should still equal `physical_keys`.

```powershell
docker compose -f compose.wikipulse.yml exec spark-client python scripts/send_wikipulse_fixture.py
```

To stop, interrupt the producer and archive job, then run `docker compose -f compose.wikipulse.yml stop`. Named volumes keep Kafka, HDFS and Cassandra data across `stop`/`up`. `docker compose down -v` would delete them, so reserve it for deliberate disposal. Cassandra is provisioned for C's schemas; A does not create result tables. Full producer to dashboard acceptance requires B/C jobs and a running Docker daemon.

## 2026-10-07 A-chain smoke test

- Docker Desktop allocated 16.4 GB and all seven services started. The host's port 7077 was unavailable, so the Spark master RPC port remains internal; its UI uses host port 18080.
- A first producer run acknowledged 100 real EventStreams records, with zero malformed JSON and no reconnect. A second run resumed from the saved SSE state and acknowledged 20 more. The 100-record capture took about 13 seconds after the SSE connection opened. This short sample does not establish daily traffic or uninterrupted collection coverage.
- The HDFS Delta archive reached version 2 with 100 rows and 100 distinct physical keys. After restarting the Spark archive with the same checkpoint and the second producer run, version 3 had 120 rows and 120 distinct keys. One deliberately malformed JSON record was then archived; version 4 had 121 rows, 121 distinct keys, 120 populated source IDs and exactly one malformed smoke row.
- A replay of the first 20 records from Delta version 2 to a separate three-partition topic acknowledged 20 sends at a configured 20 events/s. The target partition end offsets were 4, 11 and 5. The bounded replay check verified all 20 payloads, Kafka keys, original headers and provenance against the pinned archive version.
- The 121-row sample contains 150,989 raw payload bytes, 34 Wikipedia-project records and 12 Wikipedia article edit/new records. These represent 28.1% and 9.9% of this short sample, respectively. `hdfs dfs -du -s -h /wikipulse/raw_events` reported 297.7 KiB logical table size. These figures do not establish daily traffic, long-term storage growth or a reliable historical baseline.
- The initial Spark submit failed because Kafka and Delta connector JARs must be supplied before Python starts. The run commands above use explicit `--packages` and the restarted archive successfully recovered its HDFS checkpoint. No Cassandra result write or dashboard was tested; B/C own those modules.

## 2026-10-12 Docker smoke rerun

- Docker Desktop 29.0.1 built the Spark image and started all seven services. This host refused the HDFS UI binding on port 9870, so the test used a temporary host-only mapping `19870:9870`; container-to-container HDFS traffic remained on 8020. The project Compose file was unchanged.
- A malformed fixture was sent to `wikipulse.raw.live` and the archive resumed from its HDFS checkpoint. Delta version 5 contained 122 rows and 122 distinct physical keys; `malformed_smoke_rows` was 2, including the newly sent fixture and one retained from the existing volume.
- Five rows from pinned version 5 were replayed to `wikipulse.raw.replay.smoke20261011` at 20 events/s. The manifest reported `sent=5` and `source_rows=5`; `check_wikipulse_replay.py` exited 0 and verified all 5 payloads, keys, original headers, and provenance fields.
