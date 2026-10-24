"""Delta partition-pruning experiment on C's 5-minute count job.

A's archive is partitioned by the Kafka ingest date, so a backfill puts many event
days into one partition. This script copies one pinned archive version into a table
partitioned by UTC event date, then runs the same one-day 5-minute count job (the
baseline's ``window_counts`` over covered windows) with and without a predicate on
the partition column. Delta data skipping is disabled in both variants so the
difference is partition pruning alone. Files and bytes read come from the scan nodes
of each run's SQL execution in the Spark UI; the outputs must be identical.
"""

import argparse
import json
import statistics
import time
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from dic_pipeline.wikipulse.archive import spark_session
from dic_pipeline.wikipulse.baseline import collection_gaps, covered_windows, window_counts
from dic_pipeline.wikipulse.events import analysis_events

PARTITION = "event_date_utc"


def build_table(spark, archive: str, version: int, table: str) -> None:
    try:
        spark.read.format("delta").load(table).limit(1).count()
        return
    except AnalysisException:
        pass
    (spark.read.format("delta").option("versionAsOf", version).load(archive)
     .withColumn(PARTITION, F.to_date(F.timestamp_seconds("source_event_unix")))
     .write.format("delta").partitionBy(PARTITION).save(table))


def day_counts(spark, table: str, day: date, prune: bool):
    start = int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp())
    end = start + 86400
    raw = spark.read.format("delta").load(table).where(
        (F.col("source_event_unix") >= start) & (F.col("source_event_unix") < end))
    if prune:
        raw = raw.where(F.col(PARTITION) == F.lit(day))
    windows = covered_windows(start, end, collection_gaps(raw, start, end))
    return window_counts(spark, analysis_events(raw), windows)


def scan_metrics(spark, since_id: int) -> dict:
    """Sum file-scan metrics over the SQL executions started after ``since_id``.

    Both metrics are driver-side, so the UI renders one value such as "1,234" or "12.3 MiB".
    """
    base = f"{spark.sparkContext.uiWebUrl}/api/v1/applications/{spark.sparkContext.applicationId}"
    executions = json.load(urllib.request.urlopen(f"{base}/sql?details=true&length=10000"))
    totals = {"files_read": 0, "bytes_read": 0, "executions": 0}
    for execution in executions:
        if execution["id"] <= since_id:
            continue
        totals["executions"] += 1
        for node in execution.get("nodes", []):
            if not node["nodeName"].startswith("Scan"):
                continue
            for metric in node.get("metrics", []):
                value = metric["value"].replace(",", "")
                if metric["name"] == "number of files read":
                    totals["files_read"] += int(value)
                elif metric["name"] == "size of files read":
                    totals["bytes_read"] += _bytes(value)
    return totals


def _bytes(text: str) -> int:
    number, unit = text.split()
    scale = {"B": 1, "KiB": 1024, "MiB": 1024 ** 2, "GiB": 1024 ** 3, "TiB": 1024 ** 4}[unit]
    return int(float(number) * scale)


def last_execution(spark) -> int:
    base = f"{spark.sparkContext.uiWebUrl}/api/v1/applications/{spark.sparkContext.applicationId}"
    executions = json.load(urllib.request.urlopen(f"{base}/sql?length=10000"))
    return max((e["id"] for e in executions), default=-1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", default="hdfs://namenode:8020/wikipulse/raw_backfill")
    parser.add_argument("--version", required=True, type=int)
    parser.add_argument("--table", help="event-date partitioned copy (default: <archive>_by_event_date_v<N>)")
    parser.add_argument("--day", required=True, type=date.fromisoformat)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    table = args.table or f"{args.archive.rstrip('/')}_by_event_date_v{args.version}"

    spark = spark_session("WikiPulse partition pruning")
    spark.conf.set("spark.databricks.delta.stats.skipping", "false")
    try:
        build_table(spark, args.archive, args.version, table)
        partitions = (spark.read.format("delta").load(table).groupBy(PARTITION).count()
                      .orderBy(PARTITION).collect())
        runs, outputs = [], {}
        for i in range(args.runs):
            # Alternate the order so cache warm-up does not always favour one variant.
            for prune in ((True, False) if i % 2 == 0 else (False, True)):
                since = last_execution(spark)
                began = time.perf_counter()
                rows = day_counts(spark, table, args.day, prune).orderBy("wiki", "window_start").collect()
                seconds = time.perf_counter() - began
                time.sleep(1)  # let the UI store the last execution's metrics
                runs.append({"pruning": prune, "run": i, "seconds": seconds, "rows": len(rows),
                             **scan_metrics(spark, since)})
                outputs.setdefault(prune, [tuple(r) for r in rows])
        summary = {
            "archive": args.archive, "archive_version": args.version, "table": table,
            "day": args.day.isoformat(),
            "partitions": {str(r[PARTITION]): r["count"] for r in partitions},
            "outputs_identical": outputs[True] == outputs[False],
            "median_seconds": {str(p): statistics.median(r["seconds"] for r in runs if r["pruning"] == p)
                               for p in (True, False)},
            "runs": runs,
        }
    finally:
        spark.stop()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(summary, indent=2, sort_keys=True))
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
