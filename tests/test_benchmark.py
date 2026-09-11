"""Regression checks for benchmark correctness, not timing thresholds."""

from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest

from dic_pipeline.benchmark import assert_same_results, benchmark_queries, ingest_layout, table_stats
from dic_pipeline.ingestion import create_spark, write_delta
from dic_pipeline.schemas import TAXI_RAW_SCHEMA
from tests.test_preparation import taxi_row


class BenchmarkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark(master="local[2]", shuffle_partitions=2)
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_result_comparison_preserves_exact_counts(self):
        assert_same_results([(None, 2, 10.0)], [(None, 2, 10.00000001)])
        for different in ([(None, 3, 10.0)], [(None, 2, 11.0)], []):
            with self.assertRaises(AssertionError):
                assert_same_results([(None, 2, 10.0)], different)

    def test_raw_to_layouts_queries_and_current_file_statistics(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw = root / "raw"
            raw.mkdir()
            feb = taxi_row(
                tpep_pickup_datetime=datetime(2024, 2, 1, 0, 0, tzinfo=timezone.utc),
                tpep_dropoff_datetime=datetime(2024, 2, 1, 0, 10, tzinfo=timezone.utc),
                PULocationID=264, fare_amount=30.0,
            )
            rows = [taxi_row(), taxi_row(), feb]
            self.spark.createDataFrame(rows, TAXI_RAW_SCHEMA).write.parquet(
                str(raw / "yellow_tripdata_2024-test.parquet"),
            )
            tables = {}
            for layout in ("S0", "S1"):
                stats = ingest_layout(self.spark, raw, root / layout, layout, "fixture")
                self.assertEqual(stats["row_count"], 2)
                self.assertEqual(stats["quality"]["rejected_count"], 1)
                self.assertEqual(stats["partition_columns"], ["pickup_date"] if layout == "S1" else [])
                tables[layout] = self.spark.read.format("delta").load(str(root / layout / "taxi"))
            zones = self.spark.createDataFrame([(161, "Manhattan")], "location_id int, borough string")
            query_result = benchmark_queries(self.spark, tables, zones, root)
            self.assertEqual(len(query_result["samples"]), 24)
            self.assertEqual(query_result["results"]["trips_per_borough"], [
                {"borough": None, "trip_count": 1}, {"borough": "Manhattan", "trip_count": 1},
            ])
            self.assertEqual(query_result["results"]["one_week"][0]["avg_fare"], 30.0)
            self.assertEqual([r["avg_duration_seconds"] for r in query_result["results"]["duration_per_day"]], [1200.0, 600.0])
            plan = (root / "plans/one_week_S1.txt").read_text()
            self.assertIn("PartitionFilters:", plan)
            # A second write leaves obsolete files: detail must count only the latest snapshot.
            overwritten = root / "overwritten"
            write_delta(tables["S0"], overwritten)
            write_delta(tables["S0"].where("pickup_location_id = 161"), overwritten, num_files=1)
            current = table_stats(self.spark, overwritten)
            self.assertEqual(current["row_count"], 1)
            self.assertEqual(current["data_files"], 1)
            self.assertGreater(len(list(overwritten.glob("*.parquet"))), current["data_files"])


if __name__ == "__main__":
    unittest.main()
