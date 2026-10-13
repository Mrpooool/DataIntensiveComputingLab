# WikiPulse C runbook: baseline and Cassandra

C owns the shared event rules (`src/dic_pipeline/wikipulse/events.py`), the Cassandra schema and access module (`store.py`), and the baseline batch job (`baseline.py`). Field semantics, table keys and the calls B makes are in the [B/C contract](wikipulse_bc_contract.md).

## Cassandra

Only Cassandra is needed for these steps. It takes about a minute to accept CQL after starting:

```powershell
docker compose -f compose.wikipulse.yml up -d cassandra
docker compose -f compose.wikipulse.yml exec cassandra cqlsh -e "describe keyspaces"
```

Writes go through the Python `cassandra-driver` (in `requirements-wikipulse.txt`), not a Spark connector, because no connector release supports Spark 4.2. `store.create_schema(keyspace, hosts)` creates the keyspace and all six tables. The baseline job calls it automatically. B calls it once for each replay keyspace (`store.replay_keyspace(run_id)`). Use host `cassandra` inside Compose containers and `localhost` on the host.

## Baseline

The job reads one pinned Delta version of A's raw archive. It counts covered 5-minute windows, computes the per-(wiki, UTC hour) mean and sample stddev, and publishes the version to `wikipulse.baseline_by_hour`, then `wikipulse.baseline_versions`. Its version is the history end, for example `20261013T0600Z`. Pass `--history-end` when the export window is known; otherwise the job uses the last complete window before the last event.

Inside Compose, against the HDFS archive:

```powershell
docker compose -f compose.wikipulse.yml exec spark-client /usr/local/bin/spark-submit --master spark://spark-master:7077 --driver-memory 768m --packages io.delta:delta-spark_2.13:4.4.0 scripts/run_wikipulse_baseline.py --version <delta-version>
```

On the host, against an unpacked handoff directory:

```powershell
$env:PYTHONPATH = "src"
.\.venv\Scripts\python.exe -m scripts.run_wikipulse_baseline --local --hosts localhost --archive data/wikipulse/wikipulse_bc_handoff_20261013_0800_compact --version 0 --history-end 2026-10-13T06:00+00:00
```

The printed summary lists the covered windows, any collection gaps, and the number of rows with at least `MIN_SAMPLES` (24) windows. B takes the version as a start-up parameter and loads it with `store.read_baseline`. Publishing a new version does not affect a running B job. Switch versions by restarting B.

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
- No row reaches 24 samples, because each hour has at most 12 windows. B therefore reports every anomaly as `insufficient_samples` until a multi-day export is available.
