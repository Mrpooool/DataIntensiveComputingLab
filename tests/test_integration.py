import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from delta.tables import DeltaTable

from dic_pipeline.data_products import register_analytics_inputs
from dic_pipeline.ingestion import DATASETS, create_spark, write_delta
from dic_pipeline.integration import INTEGRATION_MANIFEST, add_taxi_zones, build_integrated_table
from dic_pipeline.monitoring import PIPELINE_RUNS


TAXI_SCHEMA = (
    "record_id string, run_id string, pickup_location_id int, dropoff_location_id int, "
    "pickup_hour_utc timestamp, pickup_date date"
)
ZONE_SCHEMA = "location_id int, zone string, borough string"
WEATHER_SCHEMA = (
    "weather_hour_utc timestamp, weather_timestamp_source timestamp, "
    "temp double, temp_source string, rhum double, rhum_source string, "
    "prcp double, prcp_source string, snwd double, snwd_source string, "
    "wdir double, wdir_source string, wspd double, wspd_source string, "
    "wpgt double, wpgt_source string, pres double, pres_source string, "
    "cldc int, cldc_source string, coco int, coco_source string, quality_flags array<string>"
)
AIR_SCHEMA = (
    "site_id string, air_quality_hour_utc timestamp, measurement_value double, qualifier string, "
    "parameter_code string, measurement_unit string, method_type string, method_code string"
)
ZONES = [
    (1, "Newark Airport", "EWR"),
    (161, "Midtown Center", "Manhattan"),
    (236, "Upper East Side North", "Manhattan"),
    (265, "Outside of NYC", "Outside of NYC"),
]


def utc(*args):
    return datetime(*args, tzinfo=timezone.utc)


class ZoneIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark(master="local[2]", shuffle_partitions=2)
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_joins_both_locations_and_preserves_original_fields(self):
        taxi = self.spark.createDataFrame(
            [("trip-1", 161, 236, 15.0)],
            "record_id string, pickup_location_id int, dropoff_location_id int, fare_amount double",
        )
        zones = self.spark.createDataFrame(
            [(161, "Midtown Center", "Manhattan"), (236, "Upper East Side North", "Manhattan")],
            "location_id int, zone string, borough string",
        )
        result = add_taxi_zones(taxi, zones)
        self.assertEqual(result.select(*taxi.columns).collect(), taxi.collect())
        row = result.first()
        self.assertEqual(row.pickup_zone, "Midtown Center")
        self.assertEqual(row.dropoff_zone, "Upper East Side North")
        self.assertEqual(row.pickup_borough, "Manhattan")
        self.assertEqual(row.dropoff_borough, "Manhattan")

    def test_missing_and_special_locations_preserve_trips(self):
        rows = [("missing", 999, None), ("special", 264, 265), ("airport", 1, 264)]
        taxi = self.spark.createDataFrame(
            rows, "record_id string, pickup_location_id int, dropoff_location_id int",
        )
        zones = self.spark.createDataFrame(
            [(1, "Newark Airport", "EWR"), (264, "Unknown", "Unknown"),
             (265, "Outside of NYC", "Outside of NYC")],
            "location_id int, zone string, borough string",
        )
        result = add_taxi_zones(taxi, zones)
        self.assertEqual(result.count(), len(rows))
        values = {row.record_id: row for row in result.collect()}
        self.assertIsNone(values["missing"].pickup_zone)
        self.assertIsNone(values["missing"].dropoff_zone)
        self.assertEqual(values["special"].pickup_zone, "Unknown")
        self.assertEqual(values["special"].dropoff_zone, "Outside of NYC")
        self.assertEqual(values["airport"].pickup_borough, "EWR")

    def test_invalid_lookup_keys_fail_before_join(self):
        taxi = self.spark.createDataFrame(
            [("trip-1", 161, 161)],
            "record_id string, pickup_location_id int, dropoff_location_id int",
        )
        for keys in ([161, 161], [None]):
            with self.subTest(keys=keys):
                zones = self.spark.createDataFrame(
                    [(key, "Example", "Manhattan") for key in keys],
                    "location_id int, zone string, borough string",
                )
                with self.assertRaisesRegex(ValueError, "non-null and unique"):
                    add_taxi_zones(taxi, zones)

    def _write_batch(self, root: Path, run_id: str, hour: datetime) -> dict[str, int]:
        """Overwrite the four standardized tables as one ingestion run and publish its manifest."""
        frames = {
            "taxi": self.spark.createDataFrame(
                [
                    ("trip-1", run_id, 161, 236, hour, hour.date()),
                    ("trip-2", run_id, 265, 1, hour, hour.date()),
                ],
                TAXI_SCHEMA,
            ),
            "taxi_zones": self.spark.createDataFrame(ZONES, ZONE_SCHEMA),
            "weather": self.spark.createDataFrame(
                [
                    {
                        "weather_hour_utc": hour,
                        "weather_timestamp_source": hour,
                        "temp": 1.0,
                        "temp_source": "raw",
                        "coco": 1,
                        "coco_source": "raw",
                        "quality_flags": [],
                    }
                ],
                WEATHER_SCHEMA,
            ),
            "air_quality": self.spark.createDataFrame(
                [
                    ("site-1", hour, 4.0, None, "88101", "Micrograms/cubic meter (LC)", "FEM", "636"),
                    ("site-2", hour, 6.0, None, "88101", "Micrograms/cubic meter (LC)", "FEM", "636"),
                ],
                AIR_SCHEMA,
            ),
        }
        versions = {}
        for dataset in DATASETS:
            path = root / "standardized" / dataset
            write_delta(frames[dataset], path, num_files=1)
            versions[dataset] = int(DeltaTable.forPath(self.spark, str(path)).history(1).first()["version"])
        (root / "metadata").mkdir(parents=True, exist_ok=True)
        (root / "metadata" / "completed_batch.json").write_text(
            json.dumps({"run_id": run_id, "versions": versions}), encoding="utf-8"
        )
        return versions

    def test_build_integrated_table_publishes_and_replaces_the_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / INTEGRATION_MANIFEST

            first_versions = self._write_batch(root, "run-1", utc(2024, 1, 1, 5))
            first = build_integrated_table(self.spark, root)
            self.assertEqual(first["stats"]["output_count"], 2)
            self.assertEqual(first["stats"]["nyc_trip_count"], 1)
            self.assertEqual(first["stats"]["weather_match_count"], 1)
            published = json.loads(manifest.read_text(encoding="utf-8"))
            self.assertEqual(published["run_id"], "run-1")
            self.assertEqual(published["standardized_versions"], first_versions)
            self.assertEqual(published["integrated_version"], 0)
            self.assertEqual(first["snapshot"]["integrated_version"], 0)
            rows = {
                row.record_id: row
                for row in self.spark.read.format("delta").load(first["output"]).collect()
            }
            self.assertTrue(rows["trip-1"].environment_in_scope)
            self.assertTrue(rows["trip-1"].weather_matched)
            self.assertEqual(rows["trip-1"].air_quality_pm25, 5.0)
            self.assertFalse(rows["trip-2"].environment_in_scope)
            self.assertFalse(rows["trip-2"].weather_matched)

            # A later ingestion and integration must move analytics to the new snapshot.
            second_versions = self._write_batch(root, "run-2", utc(2024, 1, 2, 5))
            self.assertEqual(set(second_versions.values()), {1})
            second = build_integrated_table(self.spark, root)
            published = json.loads(manifest.read_text(encoding="utf-8"))
            self.assertEqual(published["run_id"], "run-2")
            self.assertEqual(published["standardized_versions"], second_versions)
            self.assertEqual(published["integrated_version"], 1)
            self.assertEqual(second["snapshot"], published)

            snapshot = register_analytics_inputs(self.spark, root)
            self.assertEqual(snapshot["integrated_version"], 1)
            self.assertEqual(
                {row.run_id for row in self.spark.table("integrated_taxi_trips").collect()},
                {"run-2"},
            )

            runs = {
                row.run_id: row
                for row in self.spark.read.format("delta").load(str(root / PIPELINE_RUNS)).collect()
            }
            self.assertEqual(set(runs), {first["run_id"], second["run_id"]})
            latest = runs[second["run_id"]]
            self.assertEqual((latest.stage, latest.target, latest.status),
                             ("integration", "integrated_taxi_trips", "success"))
            self.assertEqual((latest.processed_count, latest.inserted_count, latest.target_rows_after),
                             (2, 2, 2))
            self.assertEqual(latest.output_version, 1)
            self.assertEqual(json.loads(latest.source_versions_json),
                             {f"standardized_{name}": 1 for name in DATASETS})

    def test_failed_integration_is_recorded(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(RuntimeError, "No completed batch"):
                build_integrated_table(self.spark, root, run_id="no-batch")
            row = self.spark.read.format("delta").load(str(root / PIPELINE_RUNS)).first()
            self.assertEqual((row.run_id, row.stage, row.status), ("no-batch", "integration", "failed"))
            self.assertIn("No completed batch", row.error_message)


if __name__ == "__main__":
    unittest.main()
