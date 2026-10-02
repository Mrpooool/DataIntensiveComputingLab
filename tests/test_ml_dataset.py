"""Role A zone-hour dataset and pinned-snapshot regression tests."""

import json
import tempfile
import unittest
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from pyspark.sql import functions as F

from dic_pipeline.ingestion import create_spark
from dic_pipeline.ml_dataset import (
    build_training_dataset, materialize_training_dataset, pinned_inputs,
    source_file_identifiers,
)
from dic_pipeline.ml_pipeline import (
    load_ml_config, load_training_dataset, validate_training_dataset,
)


def utc(year, month, day, hour):
    return datetime(year, month, day, hour, tzinfo=timezone.utc)


class TrainingDatasetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark(master="local[2]", shuffle_partitions=2)
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.conf.set("spark.databricks.delta.snapshotPartitions", "1")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.config = deepcopy(load_ml_config())
        self.config["training_dataset"] = {
            "coverage_start_utc": "2024-03-09 00:00:00",
            "coverage_end_utc_exclusive": "2024-03-12 00:00:00",
            "train_end_utc_exclusive": "2024-03-11 00:00:00",
            "validation_end_utc_exclusive": "2024-03-11 12:00:00",
            "expected_source_files": {
                "taxi": ["taxi.parquet"], "weather": ["weather.csv"],
                "air_quality": ["air.csv"], "taxi_zones": ["zones.csv"],
            },
        }
        start = utc(2024, 3, 9, 0)
        self.integrated = self.spark.createDataFrame(
            [(161, start), (161, start + timedelta(hours=24)),
             (236, start + timedelta(hours=2))],
            "pickup_location_id int, pickup_hour_utc timestamp",
        )
        self.zones = self.spark.createDataFrame(
            [(161, "Midtown", "Manhattan"), (236, "East Side", "Manhattan"),
             (1, "Newark Airport", "EWR")],
            "location_id int, zone string, borough string",
        )
        self.weather = self.spark.createDataFrame(
            [(start, 12.0, 0.5, 3)],
            "weather_hour_utc timestamp, temp double, prcp double, coco int",
        )
        self.air = self.spark.createDataFrame(
            [("site-1", start, 8.0, None, "88101", "Micrograms/cubic meter (LC)",
              "FEM", "636")],
            "site_id string, air_quality_hour_utc timestamp, measurement_value double, "
            "qualifier string, parameter_code string, measurement_unit string, "
            "method_type string, method_code string",
        )

    def test_zero_hours_past_only_features_and_chronological_splits(self):
        frame = build_training_dataset(
            self.spark, self.integrated, self.zones, self.weather, self.air, self.config,
        )
        self.assertEqual(frame.count(), 2 * 72)
        self.assertEqual(frame.where(F.col("pickup_location_id") == 1).count(), 0)
        start = utc(2024, 3, 9, 0)
        def at(zone, hour):
            return frame.where(
                (F.col("pickup_location_id") == zone)
                & (F.col("target_hour_utc") == F.lit(hour))
            ).first()

        zero = at(161, start + timedelta(hours=1))
        self.assertEqual(zero.trip_count, 0)
        self.assertEqual(zero.demand_lag_1h, 1)
        self.assertEqual(zero.demand_rolling_mean_24h, 1)
        self.assertEqual(zero.weather_temp_lag_1h, 12)
        self.assertEqual(zero.weather_coco_lag_1h, "3")
        self.assertEqual(zero.air_quality_pm25_lag_1h, 8)
        self.assertIsNone(at(161, start).demand_lag_1h)
        hour_24 = at(161, start + timedelta(hours=24))
        self.assertEqual(hour_24.demand_lag_24h, 1)
        self.assertEqual(hour_24.demand_lag_1h, 0)
        self.assertEqual(hour_24.demand_rolling_mean_24h, 1 / 24)
        self.assertIsNone(at(236, start + timedelta(hours=2)).weather_prcp_lag_1h)
        self.assertEqual(at(161, utc(2024, 3, 10, 7)).split, "train")
        self.assertEqual({row.split for row in frame.where(F.col("target_hour_utc") ==
                                                        F.lit(utc(2024, 3, 11, 0))).collect()},
                         {"validation"})
        self.assertEqual(validate_training_dataset(frame, self.config),
                         {"train": 96, "validation": 24, "test": 24})

    def test_modified_target_hour_does_not_change_earlier_features(self):
        original = build_training_dataset(
            self.spark, self.integrated, self.zones, self.weather, self.air, self.config,
        )
        changed = build_training_dataset(
            self.spark, self.integrated.unionByName(
                self.integrated.where(F.col("pickup_hour_utc") == F.lit(utc(2024, 3, 9, 0)))
                .where(F.col("pickup_location_id") == 161)
            ),
            self.zones, self.weather, self.air, self.config,
        )
        key = (F.col("pickup_location_id") == 161) & (
            F.col("target_hour_utc") == F.lit(utc(2024, 3, 9, 0))
        )
        self.assertEqual(original.where(key).first().demand_rolling_mean_24h,
                         changed.where(key).first().demand_rolling_mean_24h)
        self.assertEqual(changed.where(key).first().trip_count, 2)
        next_hour = (F.col("pickup_location_id") == 161) & (
            F.col("target_hour_utc") == F.lit(utc(2024, 3, 9, 1))
        )
        self.assertEqual(original.where(next_hour).first().demand_lag_1h, 1)
        self.assertEqual(changed.where(next_hour).first().demand_lag_1h, 2)

    def test_rejects_expanded_snapshot_and_mismatched_batch(self):
        snapshot = {
            "run_id": "one", "standardized_versions": {"taxi": 0},
            "integrated_version": 0,
            "completed_batch": {
                "run_id": "two", "versions": {"taxi": 0},
                "coverage_window": {
                    "valid_pickup_start_utc": "2024-03-09 00:00:00",
                    "valid_pickup_end_utc_exclusive": "2024-03-12 00:00:00",
                },
            },
        }
        with patch("dic_pipeline.ml_dataset.integration_snapshot", return_value=snapshot):
            with self.assertRaisesRegex(ValueError, "run IDs differ"):
                pinned_inputs(self.spark, "unused", self.config)
        snapshot["completed_batch"]["run_id"] = "one"
        snapshot["completed_batch"]["coverage_window"]["valid_pickup_end_utc_exclusive"] = (
            "2024-03-13 00:00:00"
        )
        snapshot["coverage_window"] = snapshot["completed_batch"]["coverage_window"]
        with patch("dic_pipeline.ml_dataset.integration_snapshot", return_value=snapshot):
            with self.assertRaisesRegex(ValueError, "Snapshot coverage"):
                pinned_inputs(self.spark, "unused", self.config)

    def test_missing_original_source_file_is_rejected(self):
        frames = {
            "taxi": self.integrated.withColumn("source_file", F.lit("raw/taxi.parquet")),
            "integrated": self.integrated.withColumn("source_file", F.lit("raw/taxi.parquet")),
            "weather": self.weather.withColumn("source_file", F.lit("raw/weather.csv")),
            "air_quality": self.air.withColumn("source_file", F.lit("raw/air.csv")),
            "taxi_zones": self.zones.withColumn("source_file", F.lit("raw/zones.csv")),
        }
        actual = source_file_identifiers(frames, self.config)
        self.assertEqual(actual["taxi"], ["taxi.parquet"])
        self.assertEqual(actual["weather"], ["weather.csv"])
        self.config["training_dataset"]["expected_source_files"]["taxi"].append("feb.parquet")
        with self.assertRaisesRegex(ValueError, "taxi source files"):
            source_file_identifiers(frames, self.config)

    def test_delta_readback_and_source_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tables = {
                "integrated": self.integrated.withColumn("source_file", F.lit("raw/taxi.parquet")),
                "taxi": self.integrated.withColumn("source_file", F.lit("raw/taxi.parquet")),
                "taxi_zones": self.zones.withColumn("source_file", F.lit("raw/zones.csv")),
                "weather": self.weather.withColumn("source_file", F.lit("raw/weather.csv")),
                "air_quality": self.air.withColumn("source_file", F.lit("raw/air.csv")),
            }
            for name, frame in tables.items():
                path = root / ("integrated/integrated_taxi_trips" if name == "integrated"
                               else f"standardized/{name}")
                frame.write.format("delta").save(str(path))
            versions = {name: 0 for name in ("taxi", "taxi_zones", "weather", "air_quality")}
            manifest = root / "metadata"
            manifest.mkdir()
            # An original batch, as run_ingestion writes it, carries no coverage window.
            (manifest / "completed_batch.json").write_text(json.dumps({
                "run_id": "baseline", "versions": versions,
            }), encoding="utf-8")
            (manifest / "completed_integration.json").write_text(json.dumps({
                "run_id": "baseline", "standardized_versions": versions,
                "integrated_version": 0,
            }), encoding="utf-8")
            window = {
                "valid_pickup_start_utc": "2024-03-09 00:00:00",
                "valid_pickup_end_utc_exclusive": "2024-03-12 00:00:00",
            }
            with patch("dic_pipeline.queries.load_dataset_config", return_value=window):
                report = materialize_training_dataset(self.spark, self.config, delta_root=root)
            self.assertEqual(report["coverage_window"], window)
            self.assertEqual(report["row_count"], 144)
            self.assertEqual(report["source_versions"]["integrated"], 0)
            self.assertEqual(report["source_files"]["taxi"], ["taxi.parquet"])
            self.assertEqual(report["source_files"]["weather"], ["weather.csv"])
            self.assertEqual(report["zero_demand_rows"], 141)
            self.assertEqual(report["included_nyc_trip_count"], 3)
            self.assertEqual(report["label_distribution"]["total"], 3)
            self.assertEqual(self.spark.read.format("delta").load(report["output_path"]).count(), 144)

            # A later rewrite of the table does not change what the metadata hands to training.
            self.spark.range(1).write.format("delta").mode("overwrite").option(
                "overwriteSchema", "true"
            ).save(report["output_path"])
            frame, training_data = load_training_dataset(
                self.spark, root / "ml/training_dataset_metadata.json"
            )
            self.assertEqual(training_data["version"], 0)
            self.assertEqual(training_data["source_run_id"], "baseline")
            self.assertEqual(frame.count(), 144)


if __name__ == "__main__":
    unittest.main()
