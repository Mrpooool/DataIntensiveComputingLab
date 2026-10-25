"""Extract page-level edit-war candidates and revert evidence from a pinned Delta archive."""

import argparse
import json
from datetime import timedelta
from pathlib import Path

from pyspark.sql import Window, functions as F

from dic_pipeline.wikipulse.archive import spark_session
from dic_pipeline.wikipulse.events import analysis_events
from dic_pipeline.wikipulse.streaming import edit_war_windows, finalize_edit_wars


def utc_text(value) -> str:
    """Spark timestamps are naive UTC because every WikiPulse session uses UTC."""
    return f"{value.isoformat()}Z"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", required=True, help="Delta archive path")
    parser.add_argument("--version", type=int, required=True, help="pinned Delta version")
    parser.add_argument("--output", type=Path, help="JSON path; default prints JSON")
    args = parser.parse_args()

    spark = spark_session("WikiPulse B edit-war evaluation")
    spark.sparkContext.setLogLevel("WARN")
    try:
        raw = spark.read.format("delta").option("versionAsOf", args.version).load(args.archive)
        # Cache only what the candidates and evidence need; the raw payload columns do
        # not fit in memory for a multi-day archive.
        events = analysis_events(raw).select("wiki", "title", "event_time", "revision_new", "user",
                                             "comment", "is_revert", "is_bot").cache()
        max_event = events.select(F.max("event_time")).first()[0]
        if max_event is None:
            raise ValueError("archive has no analysable events")
        completed_before = max_event - timedelta(minutes=2)
        candidates = finalize_edit_wars(edit_war_windows(events)).where(
            F.col("window_end") <= F.lit(completed_before)
        ).cache()

        choice = Window.partitionBy("wiki", "title").orderBy(
            F.desc("revert_count"), F.desc("distinct_nonbot_editors"),
            F.desc("user_switches"), F.desc("window_end"),
        )
        representatives = (candidates.withColumn("choice", F.row_number().over(choice))
                           .where("choice = 1").drop("choice"))
        output = []
        for candidate in representatives.orderBy("wiki", "title").collect():
            evidence = (
                events.where(
                    (F.col("wiki") == candidate.wiki)
                    & (F.col("title") == candidate.title)
                    & F.col("is_revert")
                    & ~F.coalesce("is_bot", F.lit(False))
                    & F.col("user").isNotNull()
                    & (F.col("event_time") >= F.lit(candidate.window_start))
                    & (F.col("event_time") < F.lit(candidate.window_end))
                )
                .orderBy("event_time", "revision_new")
                .select("event_time", "revision_new", "user", "comment")
                .collect()
            )
            output.append({
                "wiki": candidate.wiki,
                "title": candidate.title,
                "window_start": utc_text(candidate.window_start),
                "window_end": utc_text(candidate.window_end),
                "revert_count": candidate.revert_count,
                "distinct_nonbot_editors": candidate.distinct_nonbot_editors,
                "reverter_sequence": candidate.reverter_sequence,
                "user_switches": candidate.user_switches,
                "evidence": [{
                    "event_time": utc_text(row.event_time),
                    "revision_new": row.revision_new,
                    "user": row.user,
                    "comment": row.comment,
                } for row in evidence],
            })

        result = json.dumps({
            "archive": args.archive,
            "archive_version": args.version,
            "max_event_time": utc_text(max_event),
            "completed_before": utc_text(completed_before),
            "candidate_windows": candidates.count(),
            "unique_candidate_pages": len(output),
            "candidates": output,
        }, ensure_ascii=False, indent=2)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(result, encoding="utf-8")
        else:
            print(result)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
