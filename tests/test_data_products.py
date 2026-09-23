import json
from datetime import datetime, timedelta, timezone
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
from dic_pipeline.monitoring import PIPELINE_RUNS


TRIP_SCHEMA = (
    "record_id string, run_id string, pickup_hour_utc timestamp, pickup_location_id int, "
    "pickup_zone string, pickup_borough string, environment_in_scope boolean, "
    "trip_distance double, trip_duration_seconds long, fare_amount double, "
    "weather_matched boolean, weather_coco int, air_quality_matched boolean, "
    "air_quality_pm25 double"
)
BATCH = {"run_id": "batch-1", "versions": {dataset: 0 for dataset in DATASETS}}
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def utc(*args):
    return datetime(*args, tzinfo=timezone.utc)


def micros(value):
    return (value - EPOCH) // timedelta(microseconds=1)


HOUR_DST_FIRST = utc(2024, 11, 3, 5)
HOUR_DST_SECOND = utc(2024, 11, 3, 6)
HOUR_JANUARY = utc(2024, 1, 15, 17)
HOUR_FEBRUARY = utc(2024, 2, 1, 15)
HOUR_MARCH = utc(2024, 3, 1, 15)


def fixture_rows(run_id="batch-1"):
    return [
        ("t1", run_id, HOUR_DST_FIRST, 161, "Midtown Center", "Manhattan", True, 10.0, 600, 20.0, True, 1, True, 8.0),
        ("t2", run_id, HOUR_DST_SECOND, 161, "Midtown Center", "Manhattan", True, 5.0, 300, 10.0, True, 1, True, 9.0),
        # Unknown zone: kept by the all-trip products, outside the NYC environment scope.
        ("t3", run_id, HOUR_JANUARY, 999, None, None, False, None, 100, None, False, None, False, None),
        ("t4", run_id, HOUR_FEBRUARY, 236, "Upper East Side North", "Manhattan", True, 2.0, 120, 8.0, True, 4, True, 12.0),
        # In scope but no environment observation for that hour.
        ("t5", run_id, HOUR_MARCH, 161, "Midtown Center", "Manhattan", True, None, 240, 9.0, False, None, False, None),
    ]


class DataProductTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark(master="local[2]", shuffle_partitions=2)
        cls.spark.conf.set("spark.sql.shuffle.partitions", "2")
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _write_trips(self, root: Path, rows, *, publish: bool = True) -> None:
        for dataset in DATASETS:
            write_delta(
                self.spark.createDataFrame([(dataset,)], "value string"),
                root / "standardized" / dataset,
                num_files=1,
            )
        write_delta(
            self.spark.createDataFrame(rows, TRIP_SCHEMA),
            root / "integrated" / "integrated_taxi_trips",
            num_files=1,
        )
        (root / "metadata").mkdir(parents=True, exist_ok=True)
        (root / "metadata" / "completed_batch.json").write_text(
            json.dumps(BATCH), encoding="utf-8"
        )
        if publish:
            publish_integration_snapshot(self.spark, root, source_batch=BATCH)

    def _fixture(self, root: Path, *, run_id: str = "batch-1", publish: bool = True) -> None:
        self._write_trips(root, fixture_rows(run_id), publish=publish)

    def _read(self, root: Path, product: str):
        return self.spark.read.format("delta").load(str(root / "analytics" / product))

    def _runs(self, root: Path):
        return self.spark.read.format("delta").load(str(root / PIPELINE_RUNS))

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
                ["t1", "t2", "t3", "t4", "t5"],
            )
            self.assertEqual(self.spark.table("standardized_taxi").first().value, "taxi")

    def test_bootstrap_publishes_only_when_the_table_came_from_the_batch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._fixture(root, publish=False)
            manifest = root / "metadata" / "completed_integration.json"
            self.assertFalse(manifest.exists())
            snapshot = integration_snapshot(self.spark, root)
            self.assertTrue(manifest.exists())
            self.assertEqual(snapshot["run_id"], "batch-1")
            self.assertEqual(snapshot["integrated_version"], 0)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            # The integrated table predates the batch that is now marked complete.
            self._fixture(root, run_id="batch-0", publish=False)
            with self.assertRaisesRegex(RuntimeError, "carries ingestion run"):
                integration_snapshot(self.spark, root)
            self.assertFalse((root / "metadata" / "completed_integration.json").exists())
            with self.assertRaisesRegex(RuntimeError, "carries ingestion run"):
                publish_integration_snapshot(self.spark, root, source_batch=BATCH)

    def test_four_products_schema_grain_and_null_handling(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._fixture(root)
            records = refresh_data_products(self.spark, DEFAULT_PRODUCT_BUILDERS, delta_root=root)
            self.assertEqual({row["status"] for row in records}, {"success"})
            self.assertEqual(len(records), 4)

            daily = self._read(root, "daily_mobility_summary")
            self.assertEqual(daily.count(), 5)
            self.assertEqual(daily.select("pickup_hour_utc").distinct().count(), 5)
            unmatched = daily.where(F.col("local_pickup_date") == "2024-01-15").first()
            self.assertEqual(unmatched.trip_count, 1)
            self.assertIsNone(unmatched.distance_sum)
            self.assertEqual(unmatched.valid_distance_count, 0)
            self.assertEqual(unmatched.valid_duration_count, 1)

            dst_hours = daily.where(F.col("local_pickup_date") == "2024-11-03")
            self.assertEqual(dst_hours.count(), 2)
            expected_hours = {
                HOUR_DST_FIRST.astimezone(ZoneInfo("America/New_York")).hour,
                HOUR_DST_SECOND.astimezone(ZoneInfo("America/New_York")).hour,
            }
            self.assertEqual({row.local_pickup_hour for row in dst_hours.collect()}, expected_hours)

            zones = self._read(root, "taxi_zone_statistics")
            unknown = zones.where(F.col("pickup_location_id") == 999).first()
            self.assertEqual(unknown.trip_count, 1)
            self.assertIsNone(unknown.pickup_zone)
            self.assertIsNone(unknown.fare_sum)
            self.assertEqual(unknown.valid_fare_count, 0)

            weather = self._read(root, "weather_impact_summary")
            weather_rows = {row.weather_category: row for row in weather.collect()}
            # Role B labels, NYC scope only: t3 is out of scope, t5 is in scope but unmatched.
            self.assertEqual(set(weather_rows), {"clear_or_fair", "cloudy_or_overcast", "unmatched"})
            self.assertEqual(weather_rows["unmatched"].trip_count, 1)
            self.assertIsNone(weather_rows["unmatched"].distance_sum)
            self.assertEqual(weather_rows["clear_or_fair"].trip_count, 2)
            self.assertEqual(weather_rows["clear_or_fair"].observed_hour_count, 2)
            self.assertEqual(weather_rows["clear_or_fair"].pickup_borough, "Manhattan")

            air = self._read(root, "air_quality_impact_summary")
            self.assertEqual(air.count(), 4)
            self.assertEqual(air.where(F.col("local_pickup_date") == "2024-01-15").count(), 0)
            unmatched_air = air.where(F.col("local_pickup_date") == "2024-03-01").first()
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
                self._read(root, "daily_mobility_summary").agg(F.min("created_at_utc")).first()[0]
            )
            second = refresh_data_products(
                self.spark,
                DEFAULT_PRODUCT_BUILDERS,
                delta_root=root,
                selected=["daily_mobility_summary"],
            )[0]
            self.assertEqual(first["status"], "success")
            self.assertEqual(second["status"], "success")
            output = self._read(root, "daily_mobility_summary")
            created_after_second = output.agg(F.min("created_at_utc")).first()[0]
            self.assertEqual(output.count(), 5)
            self.assertEqual(output.select("pickup_hour_utc").distinct().count(), 5)
            self.assertEqual(output.first().source_delta_version, 0)
            self.assertEqual(created_after_first, created_after_second)
            self.assertEqual({row.created_at_utc for row in output.collect()}, {created_after_first})
            self.assertEqual(first["created_at_utc"], second["created_at_utc"])
            runs = self._runs(root).collect()
            self.assertEqual(len(runs), 2)
            self.assertEqual({row.status for row in runs}, {"success"})
            self.assertEqual({(row.stage, row.target) for row in runs},
                             {("product_refresh", "daily_mobility_summary")})
            self.assertEqual({row.inserted_count for row in runs}, {5})
            self.assertEqual({row.output_version for row in runs}, {0, 1})
            self.assertEqual(json.loads(runs[0].source_versions_json)["integrated_taxi_trips"], 0)

    def test_metadata_timestamps_are_real_utc_instants(self):
        # On a host that is not in UTC, naive datetimes used to land shifted by the host offset.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._fixture(root)
            before = micros(datetime.now(timezone.utc))
            record = refresh_data_products(
                self.spark,
                DEFAULT_PRODUCT_BUILDERS,
                delta_root=root,
                selected=["daily_mobility_summary"],
            )[0]
            after = micros(datetime.now(timezone.utc))

            self.assertIsNotNone(record["created_at_utc"].tzinfo)
            self.assertLessEqual(before, micros(record["created_at_utc"]))
            self.assertLessEqual(micros(record["refreshed_at_utc"]), after)

            product = self._read(root, "daily_mobility_summary").select(
                F.unix_micros("created_at_utc").alias("created"),
                F.unix_micros("refreshed_at_utc").alias("refreshed"),
            ).distinct().collect()
            self.assertEqual(len(product), 1)
            self.assertTrue(before <= product[0].created <= product[0].refreshed <= after)

            audit = self._runs(root).select(
                F.unix_micros("started_at").alias("started"),
                F.unix_micros("finished_at").alias("finished"),
            ).first()
            self.assertTrue(
                before <= audit.started <= micros(record["created_at_utc"]) <= audit.finished <= after
            )

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
            row = self._runs(root).first()
            self.assertEqual((row.stage, row.target), ("product_refresh", "daily_mobility_summary"))
            self.assertEqual(row.status, "failed")
            self.assertIn("broken product", row.error_message)


if __name__ == "__main__":
    unittest.main()
