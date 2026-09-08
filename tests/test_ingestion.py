from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

try:
    from dic_pipeline.ingestion import create_spark, ingest_dataset, read_source

    HAS_PIPELINE = True
except ModuleNotFoundError:
    HAS_PIPELINE = False


@unittest.skipUnless(HAS_PIPELINE, "Pipeline dependencies are not installed")
class IngestionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark()
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
