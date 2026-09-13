from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

try:
    from dic_pipeline.ingestion import (
        METADATA_SCHEMA, _utc_now, create_spark, ingest_batch, ingest_dataset,
        read_completed_batch, read_source,
    )
    from dic_pipeline.schemas import RAW_SCHEMAS
    from tests.test_preparation import air_row, taxi_row, weather_row

    HAS_PIPELINE = True
except ModuleNotFoundError:
    HAS_PIPELINE = False


@unittest.skipUnless(HAS_PIPELINE, "Pipeline dependencies are not installed")
class IngestionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark(master="local[2]", shuffle_partitions=2)
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_reader_applies_configured_csv_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            (data_dir / "taxi_zone_lookup.csv").write_text(
                "LocationID,Borough,Zone,service_zone\n"
                "1,Newark Airport,Newark Airport,EWR\n",
                encoding="utf-8",
            )
            frame = read_source(self.spark, "taxi_zones", data_dir)
            self.assertEqual(frame.count(), 1)
            self.assertEqual(frame.schema["LocationID"].dataType.simpleString(), "int")

    def test_ingestion_writes_and_records_verified_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_dir = root / "raw"
            delta_root = root / "delta"
            data_dir.mkdir()
            (data_dir / "taxi_zone_lookup.csv").write_text(
                "LocationID,Borough,Zone,service_zone\n"
                "1,Newark Airport,Newark Airport,EWR\n"
                "1,Newark Airport,Newark Airport,EWR\n"
                "999,Unknown,Unknown,Unknown\n",
                encoding="utf-8",
            )

            record = ingest_dataset(
                self.spark,
                "taxi_zones",
                data_dir=data_dir,
                delta_root=delta_root,
                run_id="a-test-run",
            )

            accepted = self.spark.read.format("delta").load(
                str(delta_root / "standardized" / "taxi_zones")
            )
            rejected = self.spark.read.format("delta").load(
                str(delta_root / "rejected" / "taxi_zones")
            )
            metadata = self.spark.read.format("delta").load(
                str(delta_root / "metadata" / "ingestion_runs")
            )

            self.assertEqual(accepted.count(), 1)
            self.assertEqual(rejected.count(), 2)
            self.assertEqual(record["duplicate_count"], 1)
            self.assertEqual(record["status"], "success")
            self.assertEqual(metadata.first()["run_id"], "a-test-run")
            self.assertEqual(metadata.first()["accepted_count"], 1)

    def test_reader_rejects_swapped_csv_header(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            (data_dir / "taxi_zone_lookup.csv").write_text(
                "LocationID,Zone,Borough,service_zone\n"
                "161,Midtown Center,Manhattan,Yellow\n", encoding="utf-8",
            )
            with self.assertRaisesRegex(Exception, "CSV header does not conform"):
                read_source(self.spark, "taxi_zones", data_dir).collect()

    def test_metadata_timestamp_preserves_utc_instant(self):
        now = _utc_now()
        self.assertIsNotNone(now.tzinfo)
        record = dict(run_id="utc", dataset="taxi", started_at=now, finished_at=now,
                      execution_seconds=0.0, status="success")
        frame = self.spark.createDataFrame([record], METADATA_SCHEMA)
        stored = frame.selectExpr("cast(started_at as double) as epoch").first()["epoch"]
        self.assertAlmostEqual(stored, now.timestamp(), places=5)

    def test_complete_batch_versions_and_rerun_invalidation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_dir, delta_root = root / "raw", root / "delta"
            data_dir.mkdir()
            with self.assertRaisesRegex(RuntimeError, "No completed batch"):
                read_completed_batch(self.spark, delta_root)
            self.spark.createDataFrame([taxi_row()], RAW_SCHEMAS["taxi"]).write.parquet(
                str(data_dir / "yellow_tripdata_2024-01.parquet")
            )
            fixtures = {
                "weather.csv": ("weather", weather_row()),
                "hourly_88101_2024.csv": ("air_quality", air_row()),
                "taxi_zone_lookup.csv": ("taxi_zones", {
                    "LocationID": 161, "Borough": "Manhattan",
                    "Zone": "Midtown Center", "service_zone": "Yellow",
                }),
            }
            for filename, (dataset, row) in fixtures.items():
                with (data_dir / filename).open("w", encoding="utf-8", newline="") as stream:
                    writer = csv.DictWriter(stream, fieldnames=RAW_SCHEMAS[dataset].fieldNames())
                    writer.writeheader()
                    writer.writerow(row)
            records = ingest_batch(self.spark, data_dir=data_dir, delta_root=delta_root, run_id="complete")
            self.assertEqual(len(records), 4)
            snapshot = read_completed_batch(self.spark, delta_root)
            for frame in snapshot.values():
                self.assertEqual(frame.select("run_id").distinct().first()["run_id"], "complete")
            manifest = delta_root / "metadata" / "completed_batch.json"
            handoff_text = manifest.read_text(encoding="utf-8")
            self.assertEqual(json.loads(handoff_text)["run_id"], "complete")

            ingest_dataset(self.spark, "taxi_zones", data_dir=data_dir,
                           delta_root=delta_root, run_id="single")
            with self.assertRaisesRegex(RuntimeError, "No completed batch"):
                read_completed_batch(self.spark, delta_root)
            # A previously loaded snapshot remains on the published Delta version.
            self.assertEqual(snapshot["taxi_zones"].first()["run_id"], "complete")

            # Restore the marker to exercise invalidation on a partially failed full run.
            manifest.write_text(handoff_text, encoding="utf-8")
            (data_dir / "weather.csv").unlink()
            with self.assertRaises(Exception):
                ingest_batch(self.spark, data_dir=data_dir, delta_root=delta_root, run_id="partial")
            self.assertFalse(manifest.exists())
            taxi = self.spark.read.format("delta").load(str(delta_root / "standardized" / "taxi"))
            self.assertEqual(taxi.first()["run_id"], "partial")

    def test_failed_ingestion_is_recorded(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaises(Exception):
                ingest_dataset(
                    self.spark,
                    "weather",
                    data_dir=root / "missing",
                    delta_root=root / "delta",
                    run_id="failed-test-run",
                )

            metadata = self.spark.read.format("delta").load(
                str(root / "delta" / "metadata" / "ingestion_runs")
            )
            row = metadata.first()
            self.assertEqual(row["run_id"], "failed-test-run")
            self.assertEqual(row["status"], "failed")
            self.assertIsNotNone(row["error_message"])


if __name__ == "__main__":
    unittest.main()
