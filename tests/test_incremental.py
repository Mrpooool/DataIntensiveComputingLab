"""Week 3 role A: incremental update generation, apply, and product refresh modes."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from pyspark.sql import functions as F

from dic_pipeline.data_products import refresh_data_products
from dic_pipeline.incremental import apply_updates, generate_update
from dic_pipeline.ingestion import create_spark, ingest_batch
from dic_pipeline.integration import build_integrated_table
from dic_pipeline.monitoring import PIPELINE_RUNS
from dic_pipeline.validation import check_schema
from tests.test_preparation import air_row, taxi_row, weather_row


def _write_raw(data_dir: Path) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    spark_rows = [
        taxi_row(
            tpep_pickup_datetime=datetime(2024, 1, 15, 12, 0),
            tpep_dropoff_datetime=datetime(2024, 1, 15, 12, 20),
            PULocationID=161,
            DOLocationID=236,
        ),
        taxi_row(
            tpep_pickup_datetime=datetime(2024, 1, 15, 13, 0),
            tpep_dropoff_datetime=datetime(2024, 1, 15, 13, 30),
            PULocationID=161,
            DOLocationID=161,
            trip_distance=3.0,
            fare_amount=18.0,
            total_amount=22.0,
        ),
        taxi_row(
            tpep_pickup_datetime=datetime(2024, 2, 1, 10, 0),
            tpep_dropoff_datetime=datetime(2024, 2, 1, 10, 15),
            PULocationID=236,
            DOLocationID=161,
        ),
    ]
    # Build a tiny parquet via Spark outside this helper once the session exists.
    weather = [
        weather_row(year=2024, month=1, day=15, hour=17),
        weather_row(year=2024, month=1, day=15, hour=18),
        weather_row(year=2024, month=2, day=1, hour=15),
    ]
    air = [
        air_row(**{
            "Date GMT": "2024-01-15", "Time GMT": "17:00",
            "Date Local": "2024-01-15", "Time Local": "12:00",
        }),
        air_row(**{
            "Date GMT": "2024-01-15", "Time GMT": "18:00",
            "Date Local": "2024-01-15", "Time Local": "13:00",
        }),
        air_row(**{
            "Date GMT": "2024-02-01", "Time GMT": "15:00",
            "Date Local": "2024-02-01", "Time Local": "10:00",
        }),
    ]
    zones = "LocationID,Borough,Zone,service_zone\n161,Manhattan,Midtown Center,Yellow Zone\n236,Manhattan,Upper East Side North,Yellow Zone\n"
    (data_dir / "taxi_zone_lookup.csv").write_text(zones, encoding="utf-8")

    import csv

    with (data_dir / "weather.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(weather[0]))
        writer.writeheader()
        writer.writerows(weather)
    with (data_dir / "hourly_88101_2024.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(air[0]))
        writer.writeheader()
        writer.writerows(air)
    return spark_rows


class IncrementalTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark(master="local[2]", shuffle_partitions=2)
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _seed_platform(self, root: Path) -> None:
        data_dir = root / "raw"
        delta_root = root / "delta"
        taxi_rows = _write_raw(data_dir)
        frame = self.spark.createDataFrame(taxi_rows)
        frame.write.mode("overwrite").parquet(str(data_dir / "yellow_tripdata_2024-01.parquet"))
        ingest_batch(
            self.spark, data_dir=data_dir, delta_root=delta_root, run_id="seed-batch", monitoring=False
        )
        build_integrated_table(self.spark, delta_root, run_id="seed-integration", monitoring=False)

    def test_public_defaults_match_assignment_bands(self):
        from dic_pipeline.incremental import (
            DEFAULT_NEW_HOURS,
            TAXI_DUP_FRACTION,
            TAXI_NEW_FRACTION,
        )

        self.assertGreaterEqual(TAXI_NEW_FRACTION, 0.05)
        self.assertLessEqual(TAXI_NEW_FRACTION, 0.10)
        self.assertGreaterEqual(TAXI_DUP_FRACTION, 0.01)
        self.assertLessEqual(TAXI_DUP_FRACTION, 0.02)
        self.assertGreaterEqual(DEFAULT_NEW_HOURS, 24)

    def test_check_schema_accepts_humidity(self):
        from dic_pipeline.schemas import REQUIRED_RAW_COLUMNS

        cols = list(REQUIRED_RAW_COLUMNS["weather"]) + ["humidity"]
        accepted, unsupported = check_schema(
            cols, REQUIRED_RAW_COLUMNS["weather"], policy={"allow_add": ["humidity"]}
        )
        self.assertEqual(unsupported, [])
        self.assertEqual(accepted[0]["column"], "humidity")

    def test_taxi_generate_and_apply_inserts_and_skips_duplicates(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._seed_platform(root)
            delta_root = root / "delta"
            out_dir = root / "updates"
            before_taxi = self.spark.read.format("delta").load(
                str(delta_root / "standardized" / "taxi")
            ).count()
            before_integrated = self.spark.read.format("delta").load(
                str(delta_root / "integrated" / "integrated_taxi_trips")
            ).count()

            manifest = generate_update(
                self.spark,
                "taxi",
                out_dir,
                delta_root=delta_root,
                seed=7,
                # Small fixture: keep the test fast; CLI/defaults stay at assignment 7%/1.5%.
                new_fraction=0.5,
                duplicate_fraction=0.5,
            )
            self.assertGreaterEqual(manifest["new_count"], 1)
            self.assertGreaterEqual(manifest["duplicate_count"], 1)
            self.assertTrue(Path(manifest["path"]).exists())

            records = apply_updates(
                self.spark, [manifest], delta_root=delta_root, run_id="inc-1", monitoring=True
            )
            taxi_record = next(item for item in records if item["dataset"] == "taxi")
            self.assertEqual(taxi_record["inserted_count"], manifest["new_count"])
            self.assertEqual(taxi_record["duplicate_count"], manifest["duplicate_count"])
            self.assertEqual(
                taxi_record["processed_count"],
                taxi_record["inserted_count"]
                + taxi_record["updated_count"]
                + taxi_record["duplicate_count"]
                + taxi_record["rejected_count"],
            )
            after_taxi = self.spark.read.format("delta").load(
                str(delta_root / "standardized" / "taxi")
            ).count()
            self.assertEqual(after_taxi, before_taxi + taxi_record["inserted_count"])
            after_integrated = self.spark.read.format("delta").load(
                str(delta_root / "integrated" / "integrated_taxi_trips")
            ).count()
            self.assertEqual(
                after_integrated, before_integrated + taxi_record["inserted_count"]
            )

            # Idempotent second apply.
            again = apply_updates(
                self.spark, [manifest], delta_root=delta_root, run_id="inc-2", monitoring=False
            )
            taxi_again = next(item for item in again if item["dataset"] == "taxi")
            self.assertEqual(taxi_again["inserted_count"], 0)
            self.assertEqual(
                self.spark.read.format("delta").load(
                    str(delta_root / "standardized" / "taxi")
                ).count(),
                after_taxi,
            )

            batch = json.loads(
                (delta_root / "metadata" / "completed_batch.json").read_text(encoding="utf-8")
            )
            self.assertIn("lineage", batch)
            self.assertEqual(batch["lineage"][-1], "inc-2")
            self.assertTrue((delta_root / "metadata" / "coverage_window.json").exists())
            runs = self.spark.read.format("delta").load(str(delta_root / PIPELINE_RUNS))
            self.assertTrue(
                runs.where(
                    (F.col("stage") == "incremental_update") & (F.col("run_id") == "inc-1")
                ).count()
                >= 2
            )

    def test_weather_and_air_schema_evolution(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._seed_platform(root)
            delta_root = root / "delta"
            out_dir = root / "updates"
            weather = generate_update(
                self.spark, "weather", out_dir, delta_root=delta_root, seed=1, new_hours=3
            )
            air = generate_update(
                self.spark, "air_quality", out_dir, delta_root=delta_root, seed=1, new_hours=3
            )
            self.assertEqual(weather["schema_changes"][0]["column"], "humidity")
            self.assertEqual(air["schema_changes"][0]["column"], "aqi")

            records = apply_updates(
                self.spark, [weather, air], delta_root=delta_root, run_id="env-1", monitoring=False
            )
            w = next(item for item in records if item["dataset"] == "weather")
            a = next(item for item in records if item["dataset"] == "air_quality")
            self.assertEqual(w["inserted_count"], weather["new_count"])
            self.assertEqual(a["inserted_count"], air["new_count"])
            weather_df = self.spark.read.format("delta").load(
                str(delta_root / "standardized" / "weather")
            )
            self.assertIn("humidity", weather_df.columns)
            self.assertGreater(
                weather_df.where(F.col("humidity").isNotNull()).count(), 0
            )
            air_df = self.spark.read.format("delta").load(
                str(delta_root / "standardized" / "air_quality")
            )
            self.assertIn("aqi", air_df.columns)

    def test_product_refresh_auto_skips_unaffected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._seed_platform(root)
            delta_root = root / "delta"
            # Full refresh first so products exist.
            full = refresh_data_products(
                self.spark, delta_root=delta_root, run_id="prod-full", monitoring=False, mode="full"
            )
            self.assertTrue(all(item["status"] == "success" for item in full))
            # Weather-only update: no taxi inserts → products list may be weather only.
            weather = generate_update(
                self.spark, "weather", root / "updates", delta_root=delta_root, seed=2, new_hours=2
            )
            apply_updates(
                self.spark, [weather], delta_root=delta_root, run_id="env-only", monitoring=False
            )
            auto = refresh_data_products(
                self.spark, delta_root=delta_root, run_id="prod-auto", monitoring=True, mode="auto"
            )
            by_name = {item["product_name"]: item for item in auto}
            self.assertEqual(by_name["weather_impact_summary"]["status"], "success")
            self.assertEqual(by_name["daily_mobility_summary"]["status"], "skipped")
            self.assertEqual(by_name["daily_mobility_summary"]["refresh_mode"], "skip")


if __name__ == "__main__":
    unittest.main()
