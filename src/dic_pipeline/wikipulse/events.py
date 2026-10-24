"""Shared WikiPulse event parsing, analysis scope, deduplication and revert detection.

Batch jobs and Structured Streaming jobs both call ``analysis_events`` so that the
filter, deduplication and revert rules are the same text on both sides.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, functions as F, types as T

FOCUS_WIKIS = ("enwiki", "dewiki", "frwiki", "zhwiki", "svwiki", "jawiki")
WATERMARK = "2 minutes"

# Case-insensitive, Unicode word boundaries; CJK keywords match as substrings.
REVERT_PATTERN = "(?iU)" + "|".join([
    r"\brevert\w*", r"\brvv?\b", r"\bundid\b", r"\bundo\b", r"\brollback\b",  # en
    r"\bzurückgesetzt\b", r"\brückgängig\b",  # de
    r"\bannulation\b", r"\brévocation\b", r"\brévoqu\w*",  # fr
    r"\båterställ\w*", r"\bångra\w*",  # sv, not yet observed in captured data
    "回退", "撤销", "撤銷", "还原", "還原",  # zh
    "取り消し", "差し戻し", "巻き戻し",  # ja
])

_PAIR = T.StructType([T.StructField("old", T.LongType()), T.StructField("new", T.LongType())])
EVENT_SCHEMA = T.StructType([
    T.StructField("meta", T.StructType([T.StructField("id", T.StringType())])),
    T.StructField("type", T.StringType()),
    T.StructField("namespace", T.LongType()),
    T.StructField("title", T.StringType()),
    T.StructField("comment", T.StringType()),
    T.StructField("timestamp", T.LongType()),
    T.StructField("user", T.StringType()),
    T.StructField("bot", T.BooleanType()),
    T.StructField("minor", T.BooleanType()),
    T.StructField("length", _PAIR),
    T.StructField("revision", _PAIR),
    T.StructField("server_name", T.StringType()),
    T.StructField("wiki", T.StringType()),
])


def is_revert(event_type: Column, comment: Column) -> Column:
    """Edit summaries matching a revert keyword; new pages are never reverts."""
    return (event_type == "edit") & comment.rlike(REVERT_PATTERN)


def parse_events(frame: DataFrame) -> DataFrame:
    """Append parsed event columns to a frame with a ``raw_json`` string column."""
    event = F.from_json("raw_json", EVENT_SCHEMA)
    comment = F.coalesce(event["comment"], F.lit(""))
    return frame.withColumns({
        "source_id": event["meta"]["id"],
        "event_time": F.timestamp_seconds(event["timestamp"]),
        "wiki": event["wiki"],
        "server_name": event["server_name"],
        "type": event["type"],
        "namespace": event["namespace"],
        "title": event["title"],
        "user": event["user"],
        "comment": comment,
        "is_bot": event["bot"],
        "minor": event["minor"],
        "bytes_changed": event["length"]["new"] - F.coalesce(event["length"]["old"], F.lit(0)),
        "revision_new": event["revision"]["new"],
        "is_revert": is_revert(event["type"], comment),
    })


def analysis_rows(frame: DataFrame) -> DataFrame:
    """Parse and keep Wikipedia article edits/new pages, before de-duplication."""
    return parse_events(frame).where(
        F.col("server_name").endswith(".wikipedia.org")
        & F.col("type").isin("edit", "new")
        & (F.col("namespace") == 0)
        & F.col("source_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("wiki").isNotNull()
        & F.col("title").isNotNull()
    )


def analysis_events(frame: DataFrame) -> DataFrame:
    """Parse, keep Wikipedia article edits/new pages, and drop repeated source IDs.

    A streaming frame gets the shared event-time watermark here; downstream window
    aggregations reuse it.
    """
    events = analysis_rows(frame)
    if events.isStreaming:
        return events.withWatermark("event_time", WATERMARK).dropDuplicatesWithinWatermark(["source_id"])
    return events.dropDuplicates(["source_id"])
