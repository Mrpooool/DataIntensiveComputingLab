"""Historical 5-minute activity and the wiki x hour-of-day baseline.

Reads one pinned Delta version of A's raw archive, counts 5-minute windows only where
collection was continuous, zero-fills wikis without events in those windows, and
publishes mean/stddev per (wiki, UTC hour of day) to Cassandra.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from dic_pipeline.wikipulse import store
from dic_pipeline.wikipulse.events import analysis_events

WINDOW_SECONDS = 300
GAP_SECONDS = 60  # no raw event of any project for this long means a collection gap
HISTORY_DAYS = 7
MIN_SAMPLES = 24  # B marks rows with fewer windows as insufficient_samples


def _floor(seconds: int) -> int:
    return seconds // WINDOW_SECONDS * WINDOW_SECONDS


def history_bounds(events: DataFrame, history_end: datetime | None = None) -> tuple[int, int]:
    """Whole 5-minute range [start, end) in epoch seconds, at most HISTORY_DAYS long."""
    times = events.select(F.col("event_time").cast("long").alias("t"))
    last = times.select(F.max("t")).first()[0]
    if last is None:
        raise ValueError("archive has no analysable events")
    end = _floor(int(history_end.timestamp()) if history_end else last)
    # Old stray timestamps must not stretch the start back to a nominal 7 days.
    first = times.where(F.col("t") >= end - HISTORY_DAYS * 86400).select(F.min("t")).first()[0]
    if first is None:
        raise ValueError("no analysable events in the history range")
    start = _floor(first + WINDOW_SECONDS - 1)
    if start >= end:
        raise ValueError("history is shorter than one 5-minute window")
    return start, end


def collection_gaps(raw: DataFrame, start: int, end: int) -> list[tuple[int, int]]:
    """Intervals (a, b) overlapping [start, end) with no raw event time for more than GAP_SECONDS.

    Events just outside the range are included so a gap straddling either edge is found.
    """
    low, high = start - GAP_SECONDS, end + GAP_SECONDS
    seconds = (raw.select(F.col("source_event_unix").alias("t"))
               .where((F.col("t") >= low) & (F.col("t") < high)).distinct())
    gaps = (seconds.withColumn("previous", F.lag("t").over(Window.orderBy("t")))
            .where(F.col("t") - F.col("previous") > GAP_SECONDS).select("previous", "t").collect())
    found = [(row.previous, row.t) for row in gaps]
    first, last = seconds.select(F.min("t"), F.max("t")).first()
    if first is None:
        return [(start, end)]
    if first - start > GAP_SECONDS:
        found.append((start, first))
    if end - last > GAP_SECONDS:
        found.append((last, end))
    return sorted(found)


def covered_windows(start: int, end: int, gaps: list[tuple[int, int]]) -> list[int]:
    """Window starts whose [w, w + 300) does not overlap any gap."""
    return [w for w in range(start, end, WINDOW_SECONDS)
            if not any(a < w + WINDOW_SECONDS and b > w for a, b in gaps)]


def window_counts(spark: SparkSession, events: DataFrame, windows: list[int]) -> DataFrame:
    """Edit and revert counts per (wiki, covered window), zero-filled for wikis seen in those windows."""
    grid = spark.createDataFrame([(w,) for w in windows], "window_unix long")
    observed = (events.withColumn("window_unix", F.floor(F.col("event_time").cast("long") / WINDOW_SECONDS)
                                  * WINDOW_SECONDS)
                .join(grid, "window_unix")
                .groupBy("wiki", "window_unix")
                .agg(F.count("*").alias("edit_count"), F.sum(F.col("is_revert").cast("int")).alias("revert_count")))
    wikis = observed.select("wiki").distinct()
    return (wikis.crossJoin(grid).join(observed, ["wiki", "window_unix"], "left")
            .fillna(0, ["edit_count", "revert_count"])
            .withColumn("window_start", F.timestamp_seconds("window_unix")).drop("window_unix"))


def build_baseline(counts: DataFrame, version: str, start: int, end: int) -> DataFrame:
    """Mean and sample stddev of 5-minute counts per (wiki, UTC hour of day)."""
    return (counts.groupBy("wiki", F.hour("window_start").alias("hour_of_day"))
            .agg(F.avg("edit_count").alias("edit_mean"), F.stddev_samp("edit_count").alias("edit_std"),
                 F.avg("revert_count").alias("revert_mean"), F.stddev_samp("revert_count").alias("revert_std"),
                 F.count("*").cast("int").alias("sample_count"))
            .select(F.lit(version).alias("baseline_version"), "wiki", "hour_of_day", "edit_mean", "edit_std",
                    "revert_mean", "revert_std", "sample_count",
                    F.timestamp_seconds(F.lit(start)).alias("history_start"),
                    F.timestamp_seconds(F.lit(end)).alias("history_end")))


def baseline_version(end: int) -> str:
    return datetime.fromtimestamp(end, timezone.utc).strftime("%Y%m%dT%H%MZ")


def run(spark: SparkSession, archive_path: str, archive_version: int, *,
        history_end: datetime | None = None, keyspace: str = store.KEYSPACE,
        hosts: str = store.DEFAULT_HOSTS) -> dict:
    """Build and publish one baseline version from a pinned archive version."""
    raw = spark.read.format("delta").option("versionAsOf", archive_version).load(archive_path)
    # parse_events keeps the raw payload columns; caching them for a week of history runs out of memory.
    events = analysis_events(raw).select("wiki", "event_time", "is_revert").cache()
    start, end = history_bounds(events, history_end)
    gaps = collection_gaps(raw, start, end)
    windows = covered_windows(start, end, gaps)
    if not windows:
        raise ValueError("no fully covered 5-minute window in the history range")
    version = baseline_version(end)
    baseline = build_baseline(window_counts(spark, events, windows), version, start, end).cache()
    store.create_schema(keyspace, hosts)
    rows = store.publish_baseline(
        baseline, version=version,
        history_start=datetime.fromtimestamp(start, timezone.utc),
        history_end=datetime.fromtimestamp(end, timezone.utc),
        source_archive=archive_path, source_version=archive_version, keyspace=keyspace, hosts=hosts)
    usable = baseline.where(F.col("sample_count") >= MIN_SAMPLES).count()
    events.unpersist()
    return {"baseline_version": version, "archive_path": archive_path, "archive_version": archive_version,
            "history_start_utc": datetime.fromtimestamp(start, timezone.utc).isoformat(),
            "history_end_utc": datetime.fromtimestamp(end, timezone.utc).isoformat(),
            "windows_total": (end - start) // WINDOW_SECONDS, "windows_covered": len(windows),
            "gaps": [[datetime.fromtimestamp(a, timezone.utc).isoformat(),
                      datetime.fromtimestamp(b, timezone.utc).isoformat()] for a, b in gaps],
            "rows": rows, "rows_with_min_samples": usable, "keyspace": keyspace}
