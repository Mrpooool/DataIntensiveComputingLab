import json
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
import tempfile
import unittest

from pyspark.sql import functions as F

from dic_pipeline.data_products import (
    DEFAULT_PRODUCT_BUILDERS,
    integration_snapshot,
    product_table_stats,
    publish_integration_snapshot,
    refresh_data_products,
    refresh_product,
    register_analytics_inputs,
)
from dic_pipeline.ingestion import DATASETS, create_spark, write_delta


TRIP_SCHEMA = (
    "record_id string, pickup_hour_utc timestamp, pickup_location_id int, "
    "pickup_zone string, pickup_borough string, trip_distance double, "
    "trip_duration_seconds long, fare_amount double, weather_matched boolean, "
    "weather_coco int, air_quality_matched boolean, air_quality_pm25 double"
)


class DataProductTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark(master="local[2]", shuffle_partitions=2)
        cls.spark.conf.set("spark.sql.shuffle.partitions", "2")
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _write_trips(self, root: Path, rows) -> None:
        versions = {}
        for dataset in DATASETS:
            path = root / "standardized" / dataset
            write_delta(
                self.spark.createDataFrame([(dataset,)], "value string"),
                path,
                num_files=1,
            )
            versions[dataset] = 0
        write_delta(
            self.spark.createDataFrame(rows, TRIP_SCHEMA),
            root / "integrated" / "integrated_taxi_trips",
            num_files=1,
        )
        (root / "metadata").mkdir(parents=True, exist_ok=True)
        (root / "metadata" / "completed_batch.json").write_text(
            json.dumps({"run_id": "batch-1", "versions": versions}),
            encoding="utf-8",
        )
        publish_integration_snapshot(
            self.spark,
            root,
            source_batch={"run_id": "batch-1", "versions": versions},
        )

    def _fixture(self, root: Path) -> None:
        hour_dst_first = datetime(2024, 11, 3, 5, tzinfo=timezone.utc)
        hour_dst_second = datetime(2024, 11, 3, 6, tzinfo=timezone.utc)
        hour_january = datetime(2024, 1, 15, 17, tzinfo=timezone.utc)
        hour_february = datetime(2024, 2, 1, 15, tzinfo=timezone.utc)
        self._write_trips(
            root,
            [
                ("t1", hour_dst_first, 161, "Midtown Center", "Manhattan", 10.0, 600, 20.0, True, 1, True, 8.0),
                ("t2", hour_dst_second, 161, "Midtown Center", "Manhattan", 5.0, 300, 10.0, True, 1, True, 9.0),
                ("t3", hour_january, 999, None, None, None, 100, None, False, None, False, None),
                ("t4", hour_february, 236, "Upper East Side North", "Manhattan", 2.0, 120, 8.0, True, 4, True, 12.0),
            ],
        )

    def test_registers_exact_published_versions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._fixture(root)
            write_delta(
                self.spark.createDataFrame(
                    [(datetime(2024, 3, 1, 12),)], "pickup_hour_utc timestamp"
                ).selectExpr("'new' as record_id", "pickup_hour_utc"),
                root / "integrated" / "integrated_taxi_trips",
                num_files=1,
            )
            snapshot = register_analytics_inputs(self.spark, root)
            self.assertEqual(snapshot["integrated_version"], 0)
            self.assertEqual(snapshot["completed_batch"]["run_id"], "batch-1")
            self.assertEqual(
                sorted(row.record_id for row in self.spark.table("integrated_taxi_trips").collect()),
                ["t1", "t2", "t3", "t4"],
            )
            self.assertEqual(self.spark.table("standardized_taxi").first().value, "taxi")

    def test_four_products_schema_grain_and_null_handling(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._fixture(root)
            records = refresh_data_products(self.spark, DEFAULT_PRODUCT_BUILDERS, delta_root=root)
            self.assertEqual({row["status"] for row in records}, {"success"})
            self.assertEqual(len(records), 4)

            daily = self.spark.read.format("delta").load(str(root / "analytics" / "daily_mobility_summary"))
            self.assertEqual(daily.count(), 4)
            self.assertEqual(
                daily.select("pickup_hour_utc").distinct().count(),
                4,
            )
            unmatched = daily.where(F.col("local_pickup_date") == "2024-01-15").first()
            self.assertEqual(unmatched.trip_count, 1)
            self.assertIsNone(unmatched.distance_sum)
            self.assertEqual(unmatched.valid_distance_count, 0)
            self.assertEqual(unmatched.valid_duration_count, 1)

            dst_hours = daily.where(F.col("local_pickup_date") == "2024-11-03")
            self.assertEqual(dst_hours.count(), 2)
            expected_hours = {
                datetime(2024, 11, 3, 5, tzinfo=timezone.utc)
                .astimezone(ZoneInfo("America/New_York"))
                .hour,
                datetime(2024, 11, 3, 6, tzinfo=timezone.utc)
                .astimezone(ZoneInfo("America/New_York"))
                .hour,
            }
            self.assertEqual({row.local_pickup_hour for row in dst_hours.collect()}, expected_hours)

            zones = self.spark.read.format("delta").load(str(root / "analytics" / "taxi_zone_statistics"))
            unknown = zones.where(F.col("pickup_location_id") == 999).first()
            self.assertEqual(unknown.trip_count, 1)
            self.assertIsNone(unknown.pickup_zone)
            self.assertIsNone(unknown.fare_sum)
            self.assertEqual(unknown.valid_fare_count, 0)

            weather = self.spark.read.format("delta").load(str(root / "analytics" / "weather_impact_summary"))
            self.assertIn("unmatched", {row.weather_category for row in weather.collect()})
            unmatched_weather = weather.where(F.col("weather_category") == "unmatched").first()
            self.assertEqual(unmatched_weather.trip_count, 1)
            self.assertIsNone(unmatched_weather.distance_sum)

            air = self.spark.read.format("delta").load(str(root / "analytics" / "air_quality_impact_summary"))
            unmatched_air = air.where(F.col("local_pickup_date") == "2024-01-15").first()
            self.assertEqual(unmatched_air.match_status, "unmatched")
            self.assertIsNone(unmatched_air.air_quality_pm25)
            self.assertEqual(unmatched_air.unmatched_trip_count, 1)

            for record in records:
                self.assertGreater(record["data_files"], 0)
                self.assertGreater(record["data_bytes"], 0)
                stats = product_table_stats(self.spark, root / "analytics" / record["product_name"])
                self.assertEqual(record["data_files"], stats["data_files"])
                self.assertEqual(record["data_bytes"], stats["data_bytes"])

    def test_refresh_overwrites_verifies_and_preserves_creation_time(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._fixture(root)
            first = refresh_data_products(
                self.spark,
                DEFAULT_PRODUCT_BUILDERS,
                delta_root=root,
                selected=["daily_mobility_summary"],
            )[0]
            created_after_first = (
                self.spark.read.format("delta")
                .load(str(root / "analytics" / "daily_mobility_summary"))
                .agg(F.min("created_at_utc"))
                .first()[0]
            )
            second = refresh_data_products(
                self.spark,
                DEFAULT_PRODUCT_BUILDERS,
                delta_root=root,
                selected=["daily_mobility_summary"],
            )[0]
            self.assertEqual(first["status"], "success")
            self.assertEqual(second["status"], "success")
            output = self.spark.read.format("delta").load(
                str(root / "analytics" / "daily_mobility_summary")
            )
            created_after_second = output.agg(F.min("created_at_utc")).first()[0]
            self.assertEqual(output.count(), 4)
            self.assertEqual(output.select("pickup_hour_utc").distinct().count(), 4)
            self.assertEqual(output.first().source_delta_version, 0)
            self.assertEqual(created_after_first, created_after_second)
            self.assertEqual({row.created_at_utc for row in output.collect()}, {created_after_first})
            metadata = self.spark.read.format("delta").load(
                str(root / "analytics" / "metadata" / "product_refresh_runs")
            )
            self.assertEqual(metadata.count(), 2)
            self.assertEqual({row.status for row in metadata.collect()}, {"success"})

    def test_failed_refresh_is_recorded_and_missing_snapshot_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(RuntimeError, "No completed integration snapshot or completed batch"):
                integration_snapshot(self.spark, directory)
            self._fixture(root)
            snapshot = register_analytics_inputs(self.spark, root)
            def broken(_spark):
                raise RuntimeError("broken product")

            with self.assertRaisesRegex(RuntimeError, "broken product"):
                refresh_product(
                    self.spark,
                    "daily_mobility_summary",
                    broken,
                    snapshot=snapshot,
                    settings={"schema_version": "1.0.0", "keys": ["pickup_hour_utc"]},
                    delta_root=root,
                )
            metadata = self.spark.read.format("delta").load(
                str(root / "analytics" / "metadata" / "product_refresh_runs")
            )
            row = metadata.first()
            self.assertEqual(row.status, "failed")
            self.assertIn("broken product", row.error_message)


if __name__ == "__main__":
    unittest.main()
