"""WikiPulse Structured Streaming analytics owned by role B.

The transformation functions also accept batch DataFrames.  This keeps the
window, ranking, edit-war and anomaly rules directly testable without Kafka or
Cassandra; ``start_queries`` adds only the streaming source/sink plumbing.
"""

from __future__ import annotations

import json
from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession, Window, functions as F
from pyspark.sql.streaming import StreamingQuery

from dic_pipeline.wikipulse import store
from dic_pipeline.wikipulse.events import FOCUS_WIKIS, analysis_events

HOT_WINDOW = "10 minutes"
HOT_SLIDE = "1 minute"
EDIT_WAR_WINDOW = "30 minutes"
EDIT_WAR_SLIDE = "5 minutes"
ACTIVITY_WINDOW = "5 minutes"
TOP_PAGES = 10
MIN_BASELINE_SAMPLES = 24
ANOMALY_Z_THRESHOLD = 3.0

HOT_PAGE_COLUMNS = [
    "wiki", "window_start", "window_end", "rank", "title", "edit_count",
    "distinct_editors", "score",
]
EDIT_WAR_COLUMNS = [
    "wiki", "window_start", "window_end", "title", "revert_count",
    "distinct_nonbot_editors", "reverter_sequence", "user_switches",
    "first_revert_at", "last_revert_at",
]
ACTIVITY_COLUMNS = [
    "wiki", "window_start", "window_end", "edit_count", "revert_count",
    "bot_count", "human_count",
]
ANOMALY_COLUMNS = [
    "wiki", "window_start", "window_end", "metric", "observed_count",
    "baseline_version", "z_score", "is_anomaly", "status",
]


def hot_page_windows(events: DataFrame) -> DataFrame:
    """Aggregate non-bot edits for each 10-minute/1-minute page window."""
    grouped = (
        events.where(F.col("wiki").isin(*FOCUS_WIKIS) & ~F.coalesce("is_bot", F.lit(False)))
        .groupBy(F.window("event_time", HOT_WINDOW, HOT_SLIDE).alias("window"), "wiki", "title")
        .agg(
            F.count("*").cast("int").alias("edit_count"),
            F.size(F.collect_set("user")).cast("int").alias("distinct_editors"),
        )
    )
    return grouped.select(
        "wiki", F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"), "title", "edit_count", "distinct_editors",
        (F.col("edit_count") * F.log(F.lit(1.0) + F.col("distinct_editors"))).cast("double").alias("score"),
    )


def rank_hot_pages(windows: DataFrame, top_n: int = TOP_PAGES) -> DataFrame:
    """Rank one completed micro-batch and retain each wiki/window Top-N."""
    if top_n < 1:
        raise ValueError("top_n must be positive")
    ranking = Window.partitionBy("wiki", "window_start", "window_end").orderBy(
        F.desc("score"), F.desc("edit_count"), F.asc("title"),
    )
    return (windows.withColumn("rank", F.row_number().over(ranking).cast("int"))
            .where(F.col("rank") <= top_n).select(*HOT_PAGE_COLUMNS))


def edit_war_windows(events: DataFrame) -> DataFrame:
    """Collect ordered inputs for each 30-minute/5-minute revert window."""
    return (
        events.where(
            F.col("wiki").isin(*FOCUS_WIKIS)
            & F.col("is_revert")
            & ~F.coalesce("is_bot", F.lit(False))
            & F.col("user").isNotNull()
        )
        .groupBy(F.window("event_time", EDIT_WAR_WINDOW, EDIT_WAR_SLIDE).alias("window"), "wiki", "title")
        .agg(F.collect_list(F.struct("event_time", "revision_new", "user")).alias("reverters"))
        .select(
            "wiki", F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"), "title", "reverters",
        )
    )


def finalize_edit_wars(windows: DataFrame) -> DataFrame:
    """Sort revert sequences and retain windows containing an A-other-A pattern."""
    prepared = (
        windows.withColumn("reverters", F.sort_array("reverters"))
        .withColumn("reverter_sequence", F.transform("reverters", lambda item: item["user"]))
        .withColumn("revert_count", F.size("reverter_sequence").cast("int"))
        .withColumn(
            "distinct_nonbot_editors",
            F.size(F.array_distinct("reverter_sequence")).cast("int"),
        )
        .where((F.col("revert_count") >= 3) & (F.col("distinct_nonbot_editors") >= 2))
        .withColumn(
            "user_switches",
            F.expr(
                "aggregate(sequence(2, size(reverter_sequence)), 0, "
                "(total, i) -> total + if(element_at(reverter_sequence, i) "
                "<> element_at(reverter_sequence, i - 1), 1, 0))"
            ).cast("int"),
        )
        .withColumn(
            "has_alternation",
            F.expr(
                "exists(sequence(3, size(reverter_sequence)), i -> "
                "element_at(reverter_sequence, i) <> element_at(reverter_sequence, i - 1) "
                "AND array_contains(slice(reverter_sequence, 1, i - 2), "
                "element_at(reverter_sequence, i)))"
            ),
        )
        .where("has_alternation")
        .withColumn("first_revert_at", F.element_at("reverters", 1)["event_time"])
        .withColumn("last_revert_at", F.element_at("reverters", -1)["event_time"])
    )
    return prepared.select(*EDIT_WAR_COLUMNS)


def activity_windows(events: DataFrame) -> DataFrame:
    """Build 5-minute activity for every Wikipedia edition."""
    return (
        events.groupBy(F.window("event_time", ACTIVITY_WINDOW).alias("window"), "wiki")
        .agg(
            F.count("*").cast("int").alias("edit_count"),
            F.sum(F.when(F.col("is_revert"), 1).otherwise(0)).cast("int").alias("revert_count"),
            F.sum(F.when(F.col("is_bot") == F.lit(True), 1).otherwise(0)).cast("int").alias("bot_count"),
            F.sum(F.when(F.col("is_bot") == F.lit(True), 0).otherwise(1)).cast("int").alias("human_count"),
        )
        .select(
            "wiki", F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"), "edit_count", "revert_count",
            "bot_count", "human_count",
        )
    )


def detect_anomalies(activity: DataFrame, baseline: DataFrame) -> DataFrame:
    """Compare activity counts with the fixed wiki/hour baseline version."""
    measurements = (
        activity.select(
            "wiki", "window_start", "window_end",
            F.explode(F.array(
                F.struct(F.lit("edit_count").alias("metric"),
                         F.col("edit_count").cast("int").alias("observed_count")),
                F.struct(F.lit("revert_count").alias("metric"),
                         F.col("revert_count").cast("int").alias("observed_count")),
            )).alias("measurement"),
        )
        .select("wiki", "window_start", "window_end", "measurement.*")
        .withColumn("hour_of_day", F.hour("window_start"))
    )
    joined = (
        measurements.join(F.broadcast(baseline), ["wiki", "hour_of_day"], "left")
        .withColumn("baseline_mean", F.when(F.col("metric") == "edit_count", F.col("edit_mean"))
                    .otherwise(F.col("revert_mean")))
        .withColumn("baseline_std", F.when(F.col("metric") == "edit_count", F.col("edit_std"))
                    .otherwise(F.col("revert_std")))
        .withColumn(
            "status",
            F.when(F.col("baseline_version").isNull(), F.lit("no_baseline"))
            .when(F.col("sample_count") < MIN_BASELINE_SAMPLES, F.lit("insufficient_samples"))
            .when(F.col("baseline_std").isNull() | (F.col("baseline_std") == 0), F.lit("zero_std"))
            .otherwise(F.lit("ok")),
        )
        .withColumn(
            "z_score",
            F.when(
                F.col("status") == "ok",
                ((F.col("observed_count") - F.col("baseline_mean")) / F.col("baseline_std")).cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "is_anomaly",
            F.when(F.col("status") == "ok", F.col("z_score") > ANOMALY_Z_THRESHOLD)
            .otherwise(F.lit(None).cast("boolean")),
        )
    )
    return joined.select(*ANOMALY_COLUMNS)


def kafka_source(spark: SparkSession, bootstrap: str, topic: str, *,
                 starting_offsets: str = "earliest",
                 max_offsets_per_trigger: int | None = None) -> DataFrame:
    """Read Kafka payloads as the ``raw_json`` input required by ``events.py``."""
    if not bootstrap or not topic:
        raise ValueError("bootstrap and topic are required")
    if starting_offsets not in {"earliest", "latest"}:
        raise ValueError("starting_offsets must be earliest or latest")
    reader = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", bootstrap)
              .option("subscribe", topic)
              .option("startingOffsets", starting_offsets))
    if max_offsets_per_trigger is not None:
        if max_offsets_per_trigger < 1:
            raise ValueError("max_offsets_per_trigger must be positive")
        reader = reader.option("maxOffsetsPerTrigger", max_offsets_per_trigger)
    return reader.load().selectExpr("CAST(value AS STRING) AS raw_json")


def _checkpoint(root: str, query: str) -> str:
    if not root:
        raise ValueError("checkpoint_root is required")
    return f"{root.rstrip('/')}/{query}"


def _log_write(query: str, batch_id: int, **counts: int) -> None:
    print(json.dumps({"query": query, "batch_id": batch_id, **counts}, sort_keys=True), flush=True)


def start_queries(raw: DataFrame, baseline: DataFrame, *, checkpoint_root: str,
                  keyspace: str = store.KEYSPACE, hosts: str = store.DEFAULT_HOSTS,
                  trigger_interval: str = "10 seconds",
                  query_suffix: str = "") -> list[StreamingQuery]:
    """Start the three B queries and return them without blocking.

    The source must be streaming.  All sinks use append mode, therefore Spark
    publishes only windows closed by the shared event-time watermark.
    """
    if not raw.isStreaming:
        raise ValueError("raw must be a streaming DataFrame")
    if not trigger_interval:
        raise ValueError("trigger_interval is required")
    events = analysis_events(raw)

    def write_hot(batch: DataFrame, batch_id: int) -> None:
        count = store.write_rows(rank_hot_pages(batch), "hot_pages", keyspace=keyspace, hosts=hosts)
        _log_write("hot_pages", batch_id, rows_written=count)

    def write_wars(batch: DataFrame, batch_id: int) -> None:
        count = store.write_rows(finalize_edit_wars(batch), "edit_war_candidates",
                                 keyspace=keyspace, hosts=hosts)
        _log_write("edit_war_candidates", batch_id, rows_written=count)

    def write_activity(batch: DataFrame, batch_id: int) -> None:
        # Both tables read the same micro-batch; cache it so the stateful plan runs once.
        batch.persist()
        try:
            activity_count = store.write_rows(batch.select(*ACTIVITY_COLUMNS), "activity_5m",
                                              keyspace=keyspace, hosts=hosts)
            anomaly_count = store.write_rows(detect_anomalies(batch, baseline), "anomalies",
                                             keyspace=keyspace, hosts=hosts)
        finally:
            batch.unpersist()
        _log_write("activity_anomalies", batch_id, activity_rows=activity_count,
                   anomaly_rows=anomaly_count)

    definitions: list[tuple[str, DataFrame, Callable[[DataFrame, int], None]]] = [
        ("hot_pages", hot_page_windows(events), write_hot),
        ("edit_wars", edit_war_windows(events), write_wars),
        ("activity_anomalies", activity_windows(events), write_activity),
    ]
    queries: list[StreamingQuery] = []
    try:
        for name, frame, handler in definitions:
            query_name = f"wikipulse_{name}{query_suffix}"
            query = (frame.writeStream.queryName(query_name).outputMode("append")
                     .option("checkpointLocation", _checkpoint(checkpoint_root, name))
                     .trigger(processingTime=trigger_interval).foreachBatch(handler).start())
            queries.append(query)
    except Exception:
        stop_queries(queries)
        raise
    return queries


def stop_queries(queries: list[StreamingQuery]) -> None:
    """Stop every still-active B query, normally from a ``finally`` block."""
    for query in queries:
        if query.isActive:
            query.stop()
