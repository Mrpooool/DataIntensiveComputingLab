"""Focused Week 4 Role B tests for leakage-safe features and model lifecycle."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import tempfile
import unittest
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.sql import functions as F

from dic_pipeline.ingestion import create_spark
from dic_pipeline.ml_pipeline import (
    FEATURES_COLUMN,
    PREDICTION_COLUMN,
    fit_feature_pipeline,
    load_ml_config,
    required_training_columns,
    select_feature_output,
    train_and_evaluate,
    validate_training_dataset,
)


SCHEMA = """
    pickup_location_id int,
    target_hour_utc timestamp,
    trip_count double,
    split string,
    pickup_zone string,
    pickup_borough string,
    weather_coco_lag_1h string,
    demand_lag_1h double,
    demand_lag_24h double,
    demand_rolling_mean_24h double,
    weather_temp_lag_1h double,
    weather_prcp_lag_1h double,
    air_quality_pm25_lag_1h double
"""


class MLPipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark(master="local[2]", shuffle_partitions=2)
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.config = load_ml_config()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def training_frame(self):
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        boroughs = {1: "Manhattan", 2: "Manhattan", 3: "Queens", 4: "Queens"}
        rows = []
        for hour in range(24):
            split = "train" if hour < 16 else "validation" if hour < 20 else "test"
            for zone in range(1, 5):
                demand = float(3 + 2 * hour + zone + (hour % 3) * zone)
                zone_name = f"Zone {zone}"
                if hour == 16 and zone == 1:
                    zone_name = "Previously unseen zone"
                train_temperature = None if (hour + zone) % 13 == 0 else 12.0
                later_temperature = None if zone == 1 else 999.0
                rows.append(
                    (
                        zone,
                        start + timedelta(hours=hour),
                        demand,
                        split,
                        zone_name,
                        boroughs[zone],
                        None if (hour + zone) % 11 == 0 else str(hour % 3),
                        max(0.0, demand - 2.0),
                        max(0.0, demand - 1.0),
                        max(0.0, demand - 1.5),
                        train_temperature if hour < 16 else later_temperature,
                        None if (hour + zone) % 10 == 0 else float(hour % 2),
                        None if (hour + zone) % 9 == 0 else float(5 + zone),
                    )
                )
        return self.spark.createDataFrame(rows, SCHEMA)

    def test_contract_declares_every_model_input(self):
        columns = required_training_columns(self.config)
        self.assertEqual(len(columns), len(set(columns)))
        self.assertTrue(
            {
                "pickup_location_id",
                "target_hour_utc",
                "trip_count",
                "split",
                "demand_lag_24h",
            }.issubset(columns)
        )

    def test_features_fit_on_train_and_accept_unseen_categories(self):
        frame = self.training_frame()
        model, counts = fit_feature_pipeline(frame, self.config)
        transformed = model.transform(frame)
        validation_missing = (
            transformed.where(
                (F.col("split") == "validation")
                & (F.col("pickup_location_id") == 1)
            )
            .orderBy("target_hour_utc")
            .first()
        )

        self.assertEqual(counts, {"train": 64, "validation": 16, "test": 16})
        self.assertEqual(validation_missing["weather_temp_lag_1h__imputed"], 12.0)
        self.assertEqual(validation_missing["weather_temp_lag_1h__missing"], 1.0)
        self.assertGreater(validation_missing[FEATURES_COLUMN].size, 15)
        selected = select_feature_output(transformed, self.config)
        self.assertEqual(
            selected.columns,
            [
                "pickup_location_id",
                "target_hour_utc",
                "trip_count",
                "split",
                FEATURES_COLUMN,
            ],
        )
        self.assertEqual(selected.where(F.col("split") == "validation").count(), 16)

    def test_contract_rejects_duplicate_key_and_overlapping_time_blocks(self):
        frame = self.training_frame()
        duplicate = frame.unionByName(frame.limit(1))
        with self.assertRaisesRegex(ValueError, "must be unique"):
            validate_training_dataset(duplicate, self.config)
        overlapping = frame.withColumn(
            "split",
            F.when(
                (F.col("split") == "train") & (F.col("pickup_location_id") == 1),
                F.lit("test"),
            ).otherwise(F.col("split")),
        )
        with self.assertRaisesRegex(ValueError, "chronological blocks"):
            validate_training_dataset(overlapping, self.config)

    def test_train_evaluate_save_and_reload(self):
        config = deepcopy(self.config)
        config["model"]["reg_params"] = [0.1]
        config["model"]["elastic_net_params"] = [0.0]
        config["model"]["max_iter"] = 20
        config["reload_check_rows"] = 5
        frame = self.training_frame()
        with tempfile.TemporaryDirectory() as directory:
            model_path = Path(directory) / "pipeline_model"
            result = train_and_evaluate(frame, config, model_path=model_path)
            loaded = PipelineModel.load(str(model_path))
            prediction = loaded.transform(
                frame.where(F.col("split") == "test").limit(1)
            ).select(PREDICTION_COLUMN).first()[PREDICTION_COLUMN]

            self.assertEqual(result.report["status"], "success")
            self.assertEqual(result.report["candidate_count"], 1)
            self.assertEqual(set(result.report["test_metrics"]), {"rmse", "mae", "r2"})
            self.assertEqual(result.report["baseline"]["test"]["row_count"], 16)
            self.assertEqual(
                result.report["reload_check"], {"verified": True, "row_count": 5}
            )
            self.assertIsInstance(prediction, float)


if __name__ == "__main__":
    unittest.main()
