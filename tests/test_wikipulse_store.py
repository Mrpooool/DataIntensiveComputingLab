"""Cassandra result writes and baseline publication against a local Cassandra.

Needs ``docker compose -f compose.wikipulse.yml up -d cassandra``; skipped otherwise.
"""

import unittest
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F

from dic_pipeline.ingestion import create_spark
from dic_pipeline.wikipulse import store

UTC = timezone.utc


class StoreTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            cls.db = store.session()
        except Exception as error:  # noqa: BLE001 - any connection failure means no local Cassandra
            raise unittest.SkipTest(f"Cassandra not reachable on localhost: {error}")
        cls.keyspace = f"wikipulse_test_{uuid.uuid4().hex[:8]}"
        store.create_schema(cls.keyspace)
        cls.spark = create_spark(master="local[2]", shuffle_partitions=2)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.db.execute(f"DROP KEYSPACE IF EXISTS {cls.keyspace}")
        cls.spark.stop()

    def test_rejects_wrong_columns(self) -> None:
        frame = self.spark.createDataFrame([("enwiki",)], "wiki string")
        with self.assertRaisesRegex(ValueError, "activity_5m needs columns"):
            store.write_rows(frame, "activity_5m", keyspace=self.keyspace)
        with self.assertRaisesRegex(ValueError, "unknown result table"):
            store.write_rows(frame, "baseline_by_hour", keyspace=self.keyspace)

    def test_activity_retry_overwrites_and_day_is_utc(self) -> None:
        # 23:55 UTC is already the next day in Asia and Europe local time.
        start = datetime(2026, 10, 12, 23, 55, tzinfo=UTC)
        frame = self.spark.createDataFrame(
            [("enwiki", start, datetime(2026, 10, 13, 0, 0, tzinfo=UTC), 12, 1, 4, 8)],
            "wiki string, window_start timestamp, window_end timestamp, edit_count int, "
            "revert_count int, bot_count int, human_count int")
        self.assertEqual(store.write_rows(frame, "activity_5m", keyspace=self.keyspace), 1)
        store.write_rows(frame, "activity_5m", keyspace=self.keyspace)
        rows = list(self.db.execute(f"SELECT day, window_start, edit_count FROM {self.keyspace}.activity_5m"))
        self.assertEqual(len(rows), 1)
        self.assertEqual(str(rows[0].day), "2026-10-12")
        self.assertEqual(rows[0].window_start.replace(tzinfo=UTC), start)
        self.assertEqual(rows[0].edit_count, 12)

    def test_hot_pages_latest_window_first(self) -> None:
        rows = [("dewiki", datetime(2026, 10, 13, 1, m, tzinfo=UTC), datetime(2026, 10, 13, 1, m + 10, tzinfo=UTC),
                 rank, f"T{m}-{rank}", 5, 2, 5.49) for m in (0, 1) for rank in (1, 2)]
        frame = self.spark.createDataFrame(
            rows, "wiki string, window_start timestamp, window_end timestamp, rank int, title string, "
                  "edit_count int, distinct_editors int, score double")
        store.write_rows(frame, "hot_pages", keyspace=self.keyspace)
        latest = list(self.db.execute(
            f"SELECT title FROM {self.keyspace}.hot_pages WHERE wiki = 'dewiki' AND day = '2026-10-13' LIMIT 2"))
        self.assertEqual([r.title for r in latest], ["T1-1", "T1-2"])

    def test_edit_war_sequence_and_anomaly_nulls(self) -> None:
        start = datetime(2026, 10, 13, 2, 0, tzinfo=UTC)
        end = datetime(2026, 10, 13, 2, 30, tzinfo=UTC)
        war = self.spark.createDataFrame(
            [("enwiki", start, end, "Page", 3, 2, ["A", "B", "A"], 2, start, end)],
            "wiki string, window_start timestamp, window_end timestamp, title string, revert_count int, "
            "distinct_nonbot_editors int, reverter_sequence array<string>, user_switches int, "
            "first_revert_at timestamp, last_revert_at timestamp")
        store.write_rows(war, "edit_war_candidates", keyspace=self.keyspace)
        row = self.db.execute(f"SELECT reverter_sequence FROM {self.keyspace}.edit_war_candidates").one()
        self.assertEqual(row.reverter_sequence, ["A", "B", "A"])
        anomaly = self.spark.createDataFrame(
            [("svwiki", start, end, "revert_count", 0, None, None, None, "no_baseline")],
            "wiki string, window_start timestamp, window_end timestamp, metric string, observed_count int, "
            "baseline_version string, z_score double, is_anomaly boolean, status string")
        store.write_rows(anomaly, "anomalies", keyspace=self.keyspace)
        row = self.db.execute(f"SELECT z_score, is_anomaly, status FROM {self.keyspace}.anomalies").one()
        self.assertEqual((row.z_score, row.is_anomaly, row.status), (None, None, "no_baseline"))

    def test_baseline_publish_and_read(self) -> None:
        version = "20261013T0600Z"
        with self.assertRaisesRegex(ValueError, "not published"):
            store.read_baseline(self.spark, version, keyspace=self.keyspace)
        history_start = datetime(2026, 10, 6, 6, 0, tzinfo=UTC)
        history_end = datetime(2026, 10, 13, 6, 0, tzinfo=UTC)
        frame = self.spark.createDataFrame(
            [(version, "enwiki", 10, 120.0, 15.5, 12.0, 3.0, 84, history_start, history_end),
             (version, "svwiki", 10, 4.0, None, 0.0, None, 1, history_start, history_end)],
            store.BASELINE_SCHEMA)
        count = store.publish_baseline(frame, version=version, history_start=history_start,
                                       history_end=history_end, source_archive="handoff",
                                       source_version=0, keyspace=self.keyspace)
        self.assertEqual(count, 2)
        # Rerunning from the same input is allowed; reusing the name for other input is not.
        store.publish_baseline(frame, version=version, history_start=history_start, history_end=history_end,
                               source_archive="handoff", source_version=0, keyspace=self.keyspace)
        with self.assertRaisesRegex(ValueError, "already published from handoff@0"):
            store.publish_baseline(frame, version=version, history_start=history_start,
                                   history_end=history_end, source_archive="handoff",
                                   source_version=1, keyspace=self.keyspace)
        baseline = store.read_baseline(self.spark, version, keyspace=self.keyspace)
        rows = {r.wiki: r for r in baseline.withColumn("end_ms", F.unix_millis("history_end")).collect()}
        self.assertEqual(rows["enwiki"].edit_mean, 120.0)
        self.assertIsNone(rows["svwiki"].edit_std)
        self.assertEqual(rows["enwiki"].end_ms, int(history_end.timestamp() * 1000))

    def test_replay_keyspace_name(self) -> None:
        self.assertEqual(store.replay_keyspace("demo1"), "wikipulse_replay_demo1")
        with self.assertRaises(ValueError):
            store.replay_keyspace("Demo-1")


if __name__ == "__main__":
    unittest.main()
