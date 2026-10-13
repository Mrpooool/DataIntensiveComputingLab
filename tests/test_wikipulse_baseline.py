"""Collection gaps, zero-filled 5-minute counts and the hour-of-day baseline."""

import json
import tempfile
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from dic_pipeline.ingestion import create_spark
from dic_pipeline.wikipulse import store
from dic_pipeline.wikipulse.baseline import (
    baseline_version, build_baseline, collection_gaps, covered_windows, history_bounds, run, window_counts,
)
from dic_pipeline.wikipulse.events import analysis_events

T0 = int(datetime(2026, 10, 13, 10, 0, tzinfo=timezone.utc).timestamp())


def raw_row(n: int, t: int, wiki="enwiki", comment="copyedit", server=None) -> tuple:
    payload = {"meta": {"id": f"id-{n}"}, "type": "edit", "namespace": 0, "title": f"P{n % 3}",
               "comment": comment, "timestamp": t, "user": f"U{n % 2}", "bot": False,
               "server_name": server or f"{wiki[:-4]}.wikipedia.org", "wiki": wiki}
    return json.dumps(payload), t


class BaselineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = create_spark(master="local[2]", shuffle_partitions=2)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def raw(self, rows):
        return self.spark.createDataFrame(rows, "raw_json string, source_event_unix long")

    def test_covered_windows_drop_any_overlap(self) -> None:
        windows = covered_windows(T0, T0 + 1800, [(T0 + 610, T0 + 700)])
        self.assertEqual([w - T0 for w in windows], [0, 300, 900, 1200, 1500])

    def test_collection_gaps_include_edges(self) -> None:
        # Events every 10 s from +30 to +500, a hole to +700, then every 10 s to +880.
        times = list(range(T0 + 30, T0 + 501, 10)) + list(range(T0 + 700, T0 + 881, 10))
        raw = self.raw([raw_row(i, t, server="commons.wikimedia.org") for i, t in enumerate(times)])
        gaps = collection_gaps(raw, T0, T0 + 1200)
        self.assertEqual([(a - T0, b - T0) for a, b in gaps], [(500, 700), (880, 1200)])

    def test_collection_gaps_straddling_edges(self) -> None:
        # 70 s holes across both edges of [T0, T0 + 600), with events on the far side.
        times = [T0 - 40] + list(range(T0 + 30, T0 + 571, 10)) + [T0 + 640]
        raw = self.raw([raw_row(i, t, server="commons.wikimedia.org") for i, t in enumerate(times)])
        gaps = collection_gaps(raw, T0, T0 + 600)
        self.assertEqual([(a - T0, b - T0) for a, b in gaps], [(-40, 30), (570, 640)])
        # An export that simply ends just before history_end is not a gap.
        ending = self.raw([raw_row(i, t) for i, t in enumerate(range(T0, T0 + 596, 5))])
        self.assertEqual(collection_gaps(ending, T0, T0 + 600), [])

    def test_history_bounds_align_and_cap(self) -> None:
        events = analysis_events(self.raw([raw_row(1, T0 + 17), raw_row(2, T0 + 86400 + 100),
                                           raw_row(3, T0 + 8 * 86400 + 299)]))
        start, end = history_bounds(events)
        self.assertEqual(end, T0 + 8 * 86400)
        self.assertEqual(start, end - 7 * 86400 + 300)  # first in-range event, rounded up
        events = analysis_events(self.raw([raw_row(1, T0 + 17), raw_row(2, T0 + 1000)]))
        self.assertEqual(history_bounds(events), (T0 + 300, T0 + 900))
        # A stray 2011-style timestamp does not stretch the start back to 7 days.
        events = analysis_events(self.raw([raw_row(0, T0 - 400 * 86400), raw_row(1, T0 + 17),
                                           raw_row(2, T0 + 1000)]))
        self.assertEqual(history_bounds(events), (T0 + 300, T0 + 900))

    def test_zero_fill_and_statistics(self) -> None:
        rows = [raw_row(1, T0 + 5), raw_row(2, T0 + 6, comment="Undid revision 5 by X"),
                raw_row(3, T0 + 305), raw_row(4, T0 + 310), raw_row(5, T0 + 320),
                raw_row(6, T0 + 900, wiki="svwiki"),
                raw_row(7, T0 + 1300)]  # outside the covered windows below
        events = analysis_events(self.raw(rows))
        counts = window_counts(self.spark, events, [T0, T0 + 300, T0 + 600, T0 + 900])
        got = {(r.wiki, r.unix - T0): (r.edit_count, r.revert_count)
               for r in counts.withColumn("unix", F.col("window_start").cast("long")).collect()}
        self.assertEqual(got[("enwiki", 0)], (2, 1))
        self.assertEqual(got[("enwiki", 300)], (3, 0))
        self.assertEqual(got[("enwiki", 600)], (0, 0))
        self.assertEqual(got[("svwiki", 0)], (0, 0))
        self.assertEqual(counts.count(), 8)
        baseline = {r.wiki: r for r in build_baseline(counts, "v", T0, T0 + 1200).collect()}
        en = baseline["enwiki"]
        self.assertEqual((en.hour_of_day, en.sample_count), (10, 4))
        self.assertAlmostEqual(en.edit_mean, 1.25)
        self.assertAlmostEqual(en.edit_std, (sum((x - 1.25) ** 2 for x in [2, 3, 0, 0]) / 3) ** 0.5)
        self.assertAlmostEqual(en.revert_mean, 0.25)
        self.assertEqual(baseline_version(T0 + 1200), "20261013T1020Z")


class BaselinePublishTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            cls.db = store.session()
        except Exception as error:  # noqa: BLE001 - any connection failure means no local Cassandra
            raise unittest.SkipTest(f"Cassandra not reachable on localhost: {error}")
        cls.keyspace = f"wikipulse_test_{uuid.uuid4().hex[:8]}"
        cls.spark = create_spark(master="local[2]", shuffle_partitions=2)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.db.execute(f"DROP KEYSPACE IF EXISTS {cls.keyspace}")
        cls.spark.stop()

    def test_run_publishes_pinned_archive_version(self) -> None:
        times = range(T0, T0 + 900, 5)
        rows = [raw_row(i, t) for i, t in enumerate(times)]
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "raw")
            self.spark.createDataFrame(rows, "raw_json string, source_event_unix long") \
                .write.format("delta").save(path)
            # A later version must not leak into a run pinned to version 0.
            self.spark.createDataFrame([raw_row(9999, T0 + 3600)], "raw_json string, source_event_unix long") \
                .write.format("delta").mode("append").save(path)
            summary = run(self.spark, path, 0, keyspace=self.keyspace)
            baseline = store.read_baseline(self.spark, summary["baseline_version"], keyspace=self.keyspace)
            row = baseline.first()
        self.assertEqual(summary["baseline_version"], "20261013T1010Z")
        self.assertEqual(summary["windows_covered"], 2)
        self.assertEqual(summary["gaps"], [])
        self.assertEqual(summary["rows"], 1)
        self.assertEqual((row.wiki, row.sample_count, row.edit_mean), ("enwiki", 2, 60.0))


if __name__ == "__main__":
    unittest.main()
