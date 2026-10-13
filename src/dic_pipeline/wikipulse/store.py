"""Cassandra schema, result writes and baseline reads for WikiPulse.

Spark 4.2 has no released Cassandra connector, so rows are collected on the driver
and written with prepared INSERTs. Result volumes per micro-batch are small. An
INSERT with an existing primary key overwrites it, so a retried batch does not add
rows or counts.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

from cassandra.cluster import Cluster, Session
from cassandra.concurrent import execute_concurrent_with_args
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

KEYSPACE = "wikipulse"
DEFAULT_HOSTS = "localhost"  # containers use "cassandra"

# column -> CQL type, primary key, clustering order
TABLES: dict[str, tuple[dict[str, str], str, str | None]] = {
    "hot_pages": ({
        "wiki": "text", "day": "date", "window_start": "timestamp", "window_end": "timestamp",
        "rank": "int", "title": "text", "edit_count": "int", "distinct_editors": "int",
        "score": "double", "written_at": "timestamp",
    }, "(wiki, day), window_end, rank", "window_end DESC, rank ASC"),
    "edit_war_candidates": ({
        "wiki": "text", "day": "date", "window_start": "timestamp", "window_end": "timestamp",
        "title": "text", "revert_count": "int", "distinct_nonbot_editors": "int",
        "reverter_sequence": "list<text>", "user_switches": "int",
        "first_revert_at": "timestamp", "last_revert_at": "timestamp", "written_at": "timestamp",
    }, "(wiki, day), window_end, title", "window_end DESC, title ASC"),
    "activity_5m": ({
        "day": "date", "wiki": "text", "window_start": "timestamp", "window_end": "timestamp",
        "edit_count": "int", "revert_count": "int", "bot_count": "int", "human_count": "int",
        "written_at": "timestamp",
    }, "(day), wiki, window_start", None),
    "anomalies": ({
        "day": "date", "wiki": "text", "window_start": "timestamp", "window_end": "timestamp",
        "metric": "text", "observed_count": "int", "baseline_version": "text",
        "z_score": "double", "is_anomaly": "boolean", "status": "text", "written_at": "timestamp",
    }, "(day), wiki, window_start, metric", None),
    "baseline_by_hour": ({
        "baseline_version": "text", "wiki": "text", "hour_of_day": "int",
        "edit_mean": "double", "edit_std": "double", "revert_mean": "double", "revert_std": "double",
        "sample_count": "int", "history_start": "timestamp", "history_end": "timestamp",
    }, "(baseline_version), wiki, hour_of_day", None),
    "baseline_versions": ({
        "baseline_version": "text", "history_start": "timestamp", "history_end": "timestamp",
        "source_archive": "text", "source_version": "bigint", "row_count": "int",
        "published_at": "timestamp",
    }, "baseline_version", None),
}
RESULT_TABLES = ("hot_pages", "edit_war_candidates", "activity_5m", "anomalies")
# Columns the module adds to result rows; callers provide the rest.
ADDED_COLUMNS = ("day", "written_at")

BASELINE_SCHEMA = T.StructType([
    T.StructField("baseline_version", T.StringType()), T.StructField("wiki", T.StringType()),
    T.StructField("hour_of_day", T.IntegerType()),
    T.StructField("edit_mean", T.DoubleType()), T.StructField("edit_std", T.DoubleType()),
    T.StructField("revert_mean", T.DoubleType()), T.StructField("revert_std", T.DoubleType()),
    T.StructField("sample_count", T.IntegerType()),
    T.StructField("history_start", T.TimestampType()), T.StructField("history_end", T.TimestampType()),
])

_KEYSPACE_NAME = re.compile(r"^[a-z][a-z0-9_]{0,47}$")
_sessions: dict[str, Session] = {}


def session(hosts: str = DEFAULT_HOSTS) -> Session:
    """One driver session per process and host list, reused across micro-batches."""
    if hosts not in _sessions:
        _sessions[hosts] = Cluster(hosts.split(",")).connect()
    return _sessions[hosts]


def _keyspace(name: str) -> str:
    if not _KEYSPACE_NAME.match(name):
        raise ValueError(f"invalid keyspace name: {name!r}")
    return name


def replay_keyspace(run_id: str) -> str:
    """Separate keyspace for one replay experiment run."""
    return _keyspace(f"wikipulse_replay_{run_id}")


def create_schema(keyspace: str = KEYSPACE, hosts: str = DEFAULT_HOSTS) -> None:
    """Create the keyspace and all tables if they do not exist."""
    keyspace = _keyspace(keyspace)
    db = session(hosts)
    db.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = "
               "{'class': 'SimpleStrategy', 'replication_factor': 1}")
    for table, (columns, key, order) in TABLES.items():
        body = ", ".join(f"{name} {kind}" for name, kind in columns.items())
        clustering = f" WITH CLUSTERING ORDER BY ({order})" if order else ""
        db.execute(f"CREATE TABLE IF NOT EXISTS {keyspace}.{table} ({body}, PRIMARY KEY ({key})){clustering}")


def _utc(value):
    """Driver timestamps are naive UTC; Spark needs aware values to keep them UTC."""
    return value.replace(tzinfo=timezone.utc) if isinstance(value, datetime) else value


def _insert(keyspace: str, table: str, rows: list[dict], hosts: str) -> int:
    if not rows:
        return 0
    columns = list(TABLES[table][0])
    db = session(hosts)
    statement = db.prepare(f"INSERT INTO {_keyspace(keyspace)}.{table} ({', '.join(columns)}) "
                           f"VALUES ({', '.join('?' for _ in columns)})")
    execute_concurrent_with_args(db, statement, [[row[c] for c in columns] for row in rows],
                                 concurrency=50, raise_on_first_error=True)
    return len(rows)


def _collect(frame: DataFrame, columns: dict[str, str]) -> list[dict]:
    """Collect without local-time conversion: timestamps travel as epoch milliseconds."""
    selected = [F.unix_millis(name).alias(name) if columns[name] == "timestamp" else F.col(name)
                for name in frame.columns]
    rows = []
    for row in frame.select(*selected).collect():
        record = row.asDict()
        for name, kind in columns.items():
            if kind == "timestamp" and record.get(name) is not None:
                record[name] = datetime.fromtimestamp(record[name] / 1000, timezone.utc)
        rows.append(record)
    return rows


def write_rows(frame: DataFrame, table: str, *, keyspace: str = KEYSPACE,
               hosts: str = DEFAULT_HOSTS) -> int:
    """Write one batch of completed-window results; call from ``foreachBatch``.

    ``frame`` must have exactly the table's business columns; ``day`` (UTC date of
    ``window_start``) and ``written_at`` are added here.
    """
    if table not in RESULT_TABLES:
        raise ValueError(f"unknown result table: {table}")
    columns = TABLES[table][0]
    expected = set(columns) - set(ADDED_COLUMNS)
    if set(frame.columns) != expected:
        raise ValueError(f"{table} needs columns {sorted(expected)}, got {sorted(frame.columns)}")
    rows = _collect(frame.withColumn("day", F.to_date("window_start")), columns)
    written_at = datetime.now(timezone.utc)
    for row in rows:
        row["written_at"] = written_at
    return _insert(keyspace, table, rows, hosts)


def publish_baseline(frame: DataFrame, *, version: str, history_start: datetime,
                     history_end: datetime, source_archive: str, source_version: int,
                     keyspace: str = KEYSPACE, hosts: str = DEFAULT_HOSTS) -> int:
    """Write all baseline rows, then the version row that marks them published.

    A published version is immutable: rebuilding it from a different archive input
    is an error, so a restarted B job never sees changed statistics under one name.
    """
    existing = session(hosts).execute(
        f"SELECT source_archive, source_version FROM {_keyspace(keyspace)}.baseline_versions "
        "WHERE baseline_version = %s", (version,)).one()
    if existing is not None and (existing.source_archive, existing.source_version) != (source_archive, source_version):
        raise ValueError(f"baseline {version} is already published from "
                         f"{existing.source_archive}@{existing.source_version}")
    columns = TABLES["baseline_by_hour"][0]
    if set(frame.columns) != set(columns):
        raise ValueError(f"baseline needs columns {sorted(columns)}, got {sorted(frame.columns)}")
    rows = _collect(frame, columns)
    if any(row["baseline_version"] != version for row in rows):
        raise ValueError("baseline rows must all carry the published version")
    count = _insert(keyspace, "baseline_by_hour", rows, hosts)
    _insert(keyspace, "baseline_versions", [{
        "baseline_version": version, "history_start": history_start, "history_end": history_end,
        "source_archive": source_archive, "source_version": source_version, "row_count": count,
        "published_at": datetime.now(timezone.utc),
    }], hosts)
    return count


def read_baseline(spark: SparkSession, version: str, *, keyspace: str = KEYSPACE,
                  hosts: str = DEFAULT_HOSTS) -> DataFrame:
    """Load one published baseline version; unpublished versions are an error."""
    db = session(hosts)
    keyspace = _keyspace(keyspace)
    published = db.execute(f"SELECT row_count FROM {keyspace}.baseline_versions WHERE baseline_version = %s",
                           (version,)).one()
    if published is None:
        raise ValueError(f"baseline version {version} is not published in {keyspace}")
    rows = db.execute(f"SELECT {', '.join(BASELINE_SCHEMA.names)} FROM {keyspace}.baseline_by_hour "
                      "WHERE baseline_version = %s", (version,))
    data = [tuple(_utc(value) for value in row) for row in rows]
    if len(data) != published.row_count:
        raise ValueError(f"baseline {version} has {len(data)} rows, published {published.row_count}")
    return spark.createDataFrame(data, BASELINE_SCHEMA)
