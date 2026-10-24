"""Role B window analytics, edit-war rules and anomaly states."""

import json
import math
import os
import tempfile
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import functions as F

from dic_pipeline.ingestion import create_spark
from dic_pipeline.wikipulse import store
from dic_pipeline.wikipulse.streaming import (
    ACTIVITY_COLUMNS,
    ANOMALY_COLUMNS,
    EDIT_WAR_COLUMNS,
    HOT_PAGE_COLUMNS,
    activity_windows,
    detect_anomalies,
    edit_war_windows,
    finalize_edit_wars,
    hot_page_windows,
    kafka_source,
    rank_hot_pages,
    start_queries,
    stop_queries,
)

UTC = timezone.utc
START = datetime(2026, 10, 13, 10, 0, tzinfo=UTC)
EVENT_SCHEMA = (
    "event_time timestamp, wiki string, title string, user string, is_bot boolean, "
    "is_revert boolean, revision_new long"
)


class StreamingAnalyticsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = create_spark(master="local[2]", shuffle_partitions=2)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def events(self, rows: list[tuple]):
        return self.spark.createDataFrame(rows, EVENT_SCHEMA)

    def test_hot_pages_exclude_bots_count_editors_and_rank(self) -> None:
        at = START + timedelta(minutes=5)
        events = self.events([
            (at, "enwiki", "Alpha", "Alice", False, False, 1),
            (at, "enwiki", "Alpha", "Bob", False, False, 2),
            (at, "enwiki", "Alpha", "Alice", False, False, 3),
            (at, "enwiki", "Alpha", "Robot", True, False, 4),
            (at, "enwiki", "Beta", "Alice", False, False, 5),
            (at, "enwiki", "Beta", "Bob", False, False, 6),
            (at, "commonswiki", "Ignored", "Alice", False, False, 7),
        ])
        selected = hot_page_windows(events).where(
            (F.col("window_start") == START) &
            (F.col("window_end") == START + timedelta(minutes=10))
        )
        rows = rank_hot_pages(selected).collect()
        self.assertEqual(HOT_PAGE_COLUMNS, rank_hot_pages(selected).columns)
        self.assertEqual([row.title for row in rows], ["Alpha", "Beta"])
        self.assertEqual((rows[0].rank, rows[0].edit_count, rows[0].distinct_editors), (1, 3, 2))
        self.assertAlmostEqual(rows[0].score, 3 * math.log(3))

    def test_hot_page_ties_are_resolved_by_title(self) -> None:
        frame = self.spark.createDataFrame([
            ("enwiki", START, START + timedelta(minutes=10), "Zulu", 2, 1, 1.0),
            ("enwiki", START, START + timedelta(minutes=10), "Alpha", 2, 1, 1.0),
        ], "wiki string, window_start timestamp, window_end timestamp, title string, "
           "edit_count int, distinct_editors int, score double")
        rows = rank_hot_pages(frame, top_n=1).collect()
        self.assertEqual((rows[0].title, rows[0].rank), ("Alpha", 1))
        with self.assertRaisesRegex(ValueError, "positive"):
            rank_hot_pages(frame, top_n=0)

    def test_edit_war_requires_a_different_user_then_return(self) -> None:
        rows = []
        for index, user in enumerate(["Alice", "Bob", "Alice"], start=1):
            rows.append((START + timedelta(minutes=index), "enwiki", "Conflict", user,
                         False, True, index))
        for index, user in enumerate(["Alice", "Bob", "Carol"], start=1):
            rows.append((START + timedelta(minutes=index), "enwiki", "Patrol", user,
                         False, True, 10 + index))
        rows.extend([
            (START + timedelta(minutes=4), "enwiki", "Conflict", "Robot", True, True, 20),
            (START + timedelta(minutes=5), "enwiki", "Conflict", None, False, True, 21),
            (START + timedelta(minutes=4), "commonswiki", "Other", "Alice", False, True, 22),
        ])
        candidates = finalize_edit_wars(edit_war_windows(self.events(rows))).where(
            (F.col("window_start") == START) &
            (F.col("window_end") == START + timedelta(minutes=30))
        ).collect()
        self.assertEqual(EDIT_WAR_COLUMNS, finalize_edit_wars(edit_war_windows(self.events(rows))).columns)
        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        self.assertEqual(candidate.title, "Conflict")
        self.assertEqual(candidate.reverter_sequence, ["Alice", "Bob", "Alice"])
        self.assertEqual((candidate.revert_count, candidate.distinct_nonbot_editors,
                          candidate.user_switches), (3, 2, 2))
        self.assertEqual(candidate.first_revert_at, (START + timedelta(minutes=1)).replace(tzinfo=None))
        self.assertEqual(candidate.last_revert_at, (START + timedelta(minutes=3)).replace(tzinfo=None))

    def test_activity_covers_all_wikis_and_bot_plus_human_equals_total(self) -> None:
        at = START + timedelta(minutes=1)
        frame = activity_windows(self.events([
            (at, "enwiki", "A", "Alice", False, False, 1),
            (at, "enwiki", "B", "Robot", True, True, 2),
            (at, "enwiki", "C", None, None, False, 3),
            (at, "cebwiki", "D", "Cebuano", False, False, 4),
        ]))
        self.assertEqual(ACTIVITY_COLUMNS, frame.columns)
        rows = {row.wiki: row for row in frame.collect()}
        self.assertEqual((rows["enwiki"].edit_count, rows["enwiki"].revert_count,
                          rows["enwiki"].bot_count, rows["enwiki"].human_count), (3, 1, 1, 2))
        self.assertEqual(rows["cebwiki"].edit_count, 1)
        for row in rows.values():
            self.assertEqual(row.bot_count + row.human_count, row.edit_count)

    def test_anomaly_ok_no_baseline_insufficient_and_zero_std(self) -> None:
        end = START + timedelta(minutes=5)
        activity = self.spark.createDataFrame([
            ("enwiki", START, end, 20, 2, 5, 15),
            ("svwiki", START, end, 4, 0, 1, 3),
            ("jawiki", START, end, 3, 0, 0, 3),
        ], "wiki string, window_start timestamp, window_end timestamp, edit_count int, "
           "revert_count int, bot_count int, human_count int")
        history_start = START - timedelta(days=7)
        baseline = self.spark.createDataFrame([
            ("v1", "enwiki", 10, 10.0, 2.0, 1.0, 0.0, 24, history_start, START),
            ("v1", "svwiki", 10, 4.0, 1.0, 0.0, 1.0, 12, history_start, START),
        ], store.BASELINE_SCHEMA)
        result = detect_anomalies(activity, baseline)
        self.assertEqual(ANOMALY_COLUMNS, result.columns)
        rows = {(row.wiki, row.metric): row for row in result.collect()}

        edit = rows[("enwiki", "edit_count")]
        self.assertEqual(edit.status, "ok")
        self.assertEqual(edit.baseline_version, "v1")
        self.assertEqual(edit.z_score, 5.0)
        self.assertTrue(edit.is_anomaly)

        zero = rows[("enwiki", "revert_count")]
        self.assertEqual(zero.status, "zero_std")
        self.assertIsNone(zero.z_score)
        self.assertIsNone(zero.is_anomaly)
        self.assertEqual(rows[("svwiki", "edit_count")].status, "insufficient_samples")
        self.assertEqual(rows[("jawiki", "edit_count")].status, "no_baseline")
        self.assertIsNone(rows[("jawiki", "edit_count")].baseline_version)

    def test_kafka_source_rejects_invalid_options_before_start(self) -> None:
        with self.assertRaisesRegex(ValueError, "required"):
            kafka_source(self.spark, "", "topic")
        with self.assertRaisesRegex(ValueError, "earliest or latest"):
            kafka_source(self.spark, "broker:9092", "topic", starting_offsets="middle")
        with self.assertRaisesRegex(ValueError, "positive"):
            kafka_source(self.spark, "broker:9092", "topic", max_offsets_per_trigger=0)


def raw_event(source_id: str, at: datetime, user: str, *, title: str = "Conflict",
              comment: str = "Undid revision 1", bot: bool = False, revision: int = 1) -> str:
    return json.dumps({
        "meta": {"id": source_id}, "type": "edit", "namespace": 0, "title": title,
        "comment": comment, "timestamp": int(at.timestamp()), "user": user, "bot": bot,
        "minor": False, "length": {"old": 100, "new": 110},
        "revision": {"old": revision - 1, "new": revision},
        "server_name": "en.wikipedia.org", "wiki": "enwiki",
    })


def utc(value: datetime) -> datetime:
    """Cassandra returns UTC timestamps without tzinfo; make comparisons explicit."""
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


class StreamingSinkTests(unittest.TestCase):
    """File-stream to Cassandra smoke test; opt in with a reachable host."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.hosts = os.environ.get("WIKIPULSE_CASSANDRA_HOSTS")
        if not cls.hosts:
            raise unittest.SkipTest("set WIKIPULSE_CASSANDRA_HOSTS for the Cassandra streaming smoke test")
        try:
            cls.db = store.session(cls.hosts)
        except Exception as error:  # noqa: BLE001 - unavailable integration dependency means skip
            raise unittest.SkipTest(f"Cassandra not reachable at {cls.hosts}: {error}")
        cls.keyspace = f"wikipulse_test_{uuid.uuid4().hex[:8]}"
        store.create_schema(cls.keyspace, cls.hosts)
        cls.spark = create_spark(master="local[2]", shuffle_partitions=2)

    @classmethod
    def tearDownClass(cls) -> None:
        if hasattr(cls, "spark"):
            cls.spark.stop()
        if hasattr(cls, "keyspace"):
            cls.db.execute(f"DROP KEYSPACE IF EXISTS {cls.keyspace}")

    def test_completed_windows_reach_all_four_tables(self) -> None:
        history_start = START - timedelta(days=7)
        baseline = self.spark.createDataFrame([
            ("integration-v1", "enwiki", 10, 0.0, 1.0, 0.0, 1.0, 24, history_start, START),
        ], store.BASELINE_SCHEMA)
        initial = [
            raw_event("id-a1", START + timedelta(minutes=1), "Alice", revision=1),
            raw_event("id-b", START + timedelta(minutes=2), "Bob", revision=2),
            raw_event("id-a2", START + timedelta(minutes=3), "Alice", revision=3),
            raw_event("id-bot", START + timedelta(minutes=4), "Robot", title="Bot edit",
                      comment="routine", bot=True, revision=4),
        ]
        future_one = raw_event("id-future-1", START + timedelta(minutes=40), "Later",
                               title="Watermark one", comment="routine", revision=5)
        future_two = raw_event("id-future-2", START + timedelta(minutes=50), "Later",
                               title="Watermark two", comment="routine", revision=6)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source"
            source.mkdir()
            stream = self.spark.readStream.text(str(source)).withColumnRenamed("value", "raw_json")
            queries = start_queries(
                stream, baseline, checkpoint_root=str(root / "checkpoints"),
                keyspace=self.keyspace, hosts=self.hosts, trigger_interval="1 second",
                query_suffix="_integration",
            )
            try:
                (source / "001.json").write_text("\n".join(initial), encoding="utf-8")
                for query in queries:
                    query.processAllAvailable()
                (source / "002.json").write_text(future_one, encoding="utf-8")
                for query in queries:
                    query.processAllAvailable()
                (source / "003.json").write_text(future_two, encoding="utf-8")
                for query in queries:
                    query.processAllAvailable()
            finally:
                stop_queries(queries)

        day = START.date()
        hot = list(self.db.execute(
            f"SELECT window_start, title, edit_count, distinct_editors FROM {self.keyspace}.hot_pages "
            "WHERE wiki = %s AND day = %s", ("enwiki", day)))
        wars = list(self.db.execute(
            f"SELECT window_start, title, reverter_sequence FROM {self.keyspace}.edit_war_candidates "
            "WHERE wiki = %s AND day = %s", ("enwiki", day)))
        activity = list(self.db.execute(
            f"SELECT wiki, window_start, edit_count, revert_count, bot_count, human_count "
            f"FROM {self.keyspace}.activity_5m WHERE day = %s", (day,)))
        anomalies = list(self.db.execute(
            f"SELECT wiki, window_start, metric, baseline_version, status, is_anomaly "
            f"FROM {self.keyspace}.anomalies WHERE day = %s", (day,)))

        target_hot = [row for row in hot if row.title == "Conflict" and utc(row.window_start) == START]
        self.assertEqual([(row.edit_count, row.distinct_editors) for row in target_hot], [(3, 2)])
        target_wars = [row for row in wars if row.title == "Conflict" and utc(row.window_start) == START]
        self.assertEqual([row.reverter_sequence for row in target_wars], [["Alice", "Bob", "Alice"]])
        target_activity = [row for row in activity if row.wiki == "enwiki" and utc(row.window_start) == START]
        self.assertEqual([(row.edit_count, row.revert_count, row.bot_count, row.human_count)
                          for row in target_activity], [(4, 3, 1, 3)])
        target_anomalies = {(row.metric): row for row in anomalies
                            if row.wiki == "enwiki" and utc(row.window_start) == START}
        self.assertEqual(set(target_anomalies), {"edit_count", "revert_count"})
        self.assertEqual(target_anomalies["edit_count"].baseline_version, "integration-v1")
        self.assertEqual(target_anomalies["edit_count"].status, "ok")
        self.assertTrue(target_anomalies["edit_count"].is_anomaly)


if __name__ == "__main__":
    unittest.main()
