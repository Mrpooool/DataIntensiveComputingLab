"""Week 4 Task 4: the raw-file route must reproduce the platform route row for row.

The fixture files go through the real platform ingestion and integration, and through
the raw route, so every Taxi rejection rule the raw route mirrors is exercised against
the platform's own result. Timings are recorded, never asserted.
"""

import csv
import tempfile
import unittest
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import functions as F

from dic_pipeline.ingestion import create_spark, ingest_batch
from dic_pipeline.integration import build_integrated_table
from dic_pipeline.ml_pipeline import load_ml_config
from dic_pipeline.ml_raw_route import raw_training_dataset
from dic_pipeline.schemas import RAW_SCHEMAS
from dic_pipeline.w4_evaluation import (
    FEATURE_GROUPS,
    compare_feature_groups,
    compare_routes,
    feature_group_config,
    platform_training_dataset,
    row_differences,
)
from tests.test_preparation import air_row, taxi_row, weather_row


def local(month, day, hour, minute=0, year=2024):
    """A Taxi wall-clock time; stored as UTC so the UTC session reads it back unchanged."""
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


def trip(pickup, minutes=15, **changes):
    return taxi_row(
        tpep_pickup_datetime=pickup,
        tpep_dropoff_datetime=pickup + timedelta(minutes=minutes),
        **changes,
    )


TRIPS = [
    trip(local(3, 9, 10)),                                      # Zone 161, 15:00 UTC
    trip(local(3, 9, 10)),                                      # exact duplicate
    trip(local(3, 9, 10, 30)),                                  # same zone-hour
    trip(local(3, 9, 12), PULocationID=236, DOLocationID=161),  # Zone 236, 17:00 UTC
    trip(local(3, 9, 12, 10), minutes=-5, PULocationID=236),    # dropoff before pickup
    trip(local(3, 9, 13), PULocationID=236, trip_distance=-1.0),
    trip(local(3, 9, 13), fare_amount=float("nan")),
    trip(local(3, 9, 13), DOLocationID=999),                    # not in the zone lookup
    trip(local(12, 31, 10, year=2023)),                         # before the coverage window
    trip(local(3, 9, 11), PULocationID=1),                      # EWR is not an NYC zone
    trip(local(3, 10, 3, 30)),                                  # after the DST jump: 07:30 UTC
    trip(local(3, 20, 9)),                                      # test split, 13:00 UTC
]
KINGS = {"County Code": "047", "County Name": "Kings", "Site Num": "0118"}
MARCH_9_14H = {"Date GMT": "2024-03-09", "Time GMT": "14:00"}
ZONES = [
    (1, "EWR", "Newark Airport", "EWR"),
    (161, "Manhattan", "Midtown Center", "Yellow Zone"),
    (236, "Manhattan", "Upper East Side North", "Yellow Zone"),
    (264, "Unknown", "N/A", "N/A"),
    (265, "N/A", "Outside of NYC", "N/A"),
]


def write_csv(path, dataset, rows):
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=RAW_SCHEMAS[dataset].fieldNames())
        writer.writeheader()
        writer.writerows(rows)


class RouteComparisonTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark(master="local[2]", shuffle_partitions=2)
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.directory = tempfile.TemporaryDirectory()
        root = Path(cls.directory.name)
        cls.data_dir, cls.delta_root = root / "raw", root / "delta"
        cls.data_dir.mkdir()
        cls.spark.createDataFrame(TRIPS, RAW_SCHEMAS["taxi"]).write.parquet(
            str(cls.data_dir / "yellow_tripdata_2024-03.parquet")
        )
        write_csv(cls.data_dir / "weather.csv", "weather", [
            weather_row(),                                      # Jan 15 17:00, train
            weather_row(month=3, day=9, hour=14, temp=7.0),
        ])
        write_csv(cls.data_dir / "hourly_88101_2024.csv", "air_quality", [
            air_row(),                                          # Jan 15 17:00, train
            air_row(**MARCH_9_14H, **{"Sample Measurement": 8.0}),
            air_row(**MARCH_9_14H, **KINGS, **{"Sample Measurement": 10.0}),
            air_row(**MARCH_9_14H, **KINGS, **{"Sample Measurement": 10.0}),
            air_row(**MARCH_9_14H, **{"State Name": "New Jersey", "Sample Measurement": 100.0}),
        ])
        write_csv(cls.data_dir / "taxi_zone_lookup.csv", "taxi_zones", [
            dict(zip(RAW_SCHEMAS["taxi_zones"].fieldNames(), zone)) for zone in ZONES
        ])
        ingest_batch(cls.spark, data_dir=cls.data_dir, delta_root=cls.delta_root,
                     run_id="fixture", monitoring=False)
        build_integrated_table(cls.spark, cls.delta_root, run_id="fixture", monitoring=False)
        cls.config = deepcopy(load_ml_config())
        cls.config["training_dataset"]["expected_source_files"]["taxi"] = [
            "yellow_tripdata_2024-03.parquet"
        ]

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def test_raw_route_reproduces_platform_rows(self):
        platform = platform_training_dataset(self.spark, self.delta_root, self.config)
        raw = raw_training_dataset(self.spark, self.data_dir, self.config).cache()
        self.assertEqual(row_differences(platform, raw), {"only_platform": 0, "only_raw": 0})

        def at(zone, hour):
            return raw.where(
                (F.col("pickup_location_id") == zone) & (F.col("target_hour_utc") == F.lit(hour))
            ).first()

        self.assertEqual(raw.count(), 2 * 2183)
        self.assertEqual(raw.agg(F.sum("trip_count")).first()[0], 5)
        march_9 = at(161, local(3, 9, 15))
        self.assertEqual(march_9.trip_count, 2)
        self.assertEqual(march_9.weather_temp_lag_1h, 7.0)
        self.assertEqual(march_9.air_quality_pm25_lag_1h, 9.0)
        self.assertEqual(at(236, local(3, 9, 17)).trip_count, 1)
        self.assertEqual(at(161, local(3, 10, 7)).trip_count, 1)
        self.assertEqual(at(161, local(3, 20, 13)).split, "test")
        raw.unpersist()

    def test_route_comparison_requires_equal_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            result = compare_routes(
                self.spark, delta_root=self.delta_root, data_dir=self.data_dir,
                config=self.config, workspace_root=Path(directory), run_prefix="test", repeats=1,
            )
            self.assertEqual(list(Path(directory).iterdir()), [])
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["row_differences"], {"only_platform": 0, "only_raw": 0})
        self.assertEqual(len(result["runs"]), 4)
        self.assertEqual(result["output"]["signature"][0], 2 * 2183)
        self.assertEqual(set(result["median_seconds"]), {"platform", "raw"})

    def test_feature_groups_add_one_source_at_a_time(self):
        taxi_only = feature_group_config(self.config, FEATURE_GROUPS[0][1])
        self.assertEqual(taxi_only["categorical_features"], ["pickup_zone", "pickup_borough"])
        self.assertEqual(taxi_only["missing_indicator_features"], [])
        self.assertEqual(
            feature_group_config(self.config, FEATURE_GROUPS[-1][1])["numeric_features"],
            self.config["numeric_features"],
        )
        dataset = platform_training_dataset(self.spark, self.delta_root, self.config)
        result = compare_feature_groups(dataset, self.config)
        self.assertEqual([group["name"] for group in result["groups"]],
                         [name for name, _ in FEATURE_GROUPS])


if __name__ == "__main__":
    unittest.main()
