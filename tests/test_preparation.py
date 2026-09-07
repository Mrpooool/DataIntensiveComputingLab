from __future__ import annotations

import unittest
from datetime import datetime

try:
    from pyspark.sql import SparkSession

    from dic_pipeline.contracts import load_dataset_config
    from dic_pipeline.preparation import SchemaValidationError, prepare
    from dic_pipeline.schemas import (
        AIR_QUALITY_RAW_SCHEMA,
        TAXI_RAW_SCHEMA,
        TAXI_ZONES_RAW_SCHEMA,
        WEATHER_RAW_SCHEMA,
    )

    HAS_PYSPARK = True
except ModuleNotFoundError:
    HAS_PYSPARK = False


def taxi_row(**changes):
    row = {
        "VendorID": 2,
        "tpep_pickup_datetime": datetime(2024, 1, 15, 12, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 15, 12, 20),
        "passenger_count": 1,
        "trip_distance": 2.5,
        "RatecodeID": 1,
        "store_and_fwd_flag": "N",
        "PULocationID": 161,
        "DOLocationID": 236,
        "payment_type": 1,
        "fare_amount": 15.0,
        "extra": 0.0,
        "mta_tax": 0.5,
        "tip_amount": 3.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 1.0,
        "total_amount": 19.5,
        "congestion_surcharge": 0.0,
        "Airport_fee": 0.0,
    }
    row.update(changes)
    return row


def weather_row(**changes):
    row = {
        "year": 2024,
        "month": 1,
        "day": 15,
        "hour": 17,
        "temp": 5.0,
        "temp_source": "isd_lite",
        "rhum": 60.0,
        "rhum_source": "isd_lite",
        "prcp": 0.0,
        "prcp_source": "isd_lite",
        "snwd": None,
        "snwd_source": None,
        "wdir": 180.0,
        "wdir_source": "isd_lite",
        "wspd": 10.0,
        "wspd_source": "isd_lite",
        "wpgt": None,
        "wpgt_source": None,
        "pres": 1012.0,
        "pres_source": "isd_lite",
        "cldc": 4,
        "cldc_source": "isd_lite",
        "coco": 3,
        "coco_source": "metar",
    }
    row.update(changes)
    return row


def air_row(**changes):
    row = {
        "State Code": "36",
        "County Code": "061",
        "Site Num": "0079",
        "Parameter Code": "88101",
        "POC": 4,
        "Latitude": 40.7,
        "Longitude": -74.0,
        "Datum": "NAD83",
        "Parameter Name": "PM2.5 - Local Conditions",
        "Date Local": "2024-01-15",
        "Time Local": "12:00",
        "Date GMT": "2024-01-15",
        "Time GMT": "17:00",
        "Sample Measurement": 8.5,
        "Units of Measure": "Micrograms/cubic meter (LC)",
        "MDL": 0.5,
        "Uncertainty": None,
        "Qualifier": "",
        "Method Type": "FEM",
        "Method Code": "636",
        "Method Name": "Example monitor",
        "State Name": "New York",
        "County Name": "New York",
        "Date of Last Change": "2024-07-19",
    }
    row.update(changes)
    return row


@unittest.skipUnless(HAS_PYSPARK, "PySpark is not installed")
class PreparationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("dic-role-b-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_missing_source_column_fails_schema_validation(self):
        frame = self.spark.createDataFrame([(1,)], ["VendorID"])
        with self.assertRaises(SchemaValidationError):
            prepare(frame, load_dataset_config("taxi"))

    def test_taxi_rejects_duplicate_and_reverse_time(self):
        rows = [
            taxi_row(),
            taxi_row(),
            taxi_row(
                tpep_pickup_datetime=datetime(2024, 1, 16, 12, 20),
                tpep_dropoff_datetime=datetime(2024, 1, 16, 12, 0),
            ),
        ]
        frame = self.spark.createDataFrame(rows, TAXI_RAW_SCHEMA)
        result = prepare(frame, load_dataset_config("taxi"), run_id="taxi-test")
        self.addCleanup(result.release)
        self.assertEqual(result.metrics["input_count"], 3)
        self.assertEqual(result.metrics["accepted_count"], 1)
        self.assertEqual(result.metrics["rejected_count"], 2)
        self.assertEqual(result.metrics["duplicate_count"], 1)
        self.assertEqual(result.metrics["accepted_count"] + result.metrics["rejected_count"], 3)
        reasons = {
            reason
            for row in result.rejected.select("error_reasons").collect()
            for reason in row["error_reasons"]
        }
        self.assertIn("duplicate_record", reasons)
        self.assertIn("dropoff_before_pickup", reasons)

    def test_taxi_fingerprint_does_not_collapse_distinct_payment_details(self):
        rows = [taxi_row(), taxi_row(payment_type=2, tip_amount=0.0, total_amount=16.5)]
        frame = self.spark.createDataFrame(rows, TAXI_RAW_SCHEMA)
        result = prepare(frame, load_dataset_config("taxi"), run_id="taxi-distinct-test")
        self.addCleanup(result.release)
        self.assertEqual(result.metrics["accepted_count"], 2)
        self.assertEqual(result.metrics["duplicate_count"], 0)

    def test_weather_rejects_bad_range_and_duplicate_hour(self):
        rows = [weather_row(), weather_row(), weather_row(hour=18, rhum=101.0)]
        frame = self.spark.createDataFrame(rows, WEATHER_RAW_SCHEMA)
        result = prepare(frame, load_dataset_config("weather"), run_id="weather-test")
        self.addCleanup(result.release)
        self.assertEqual(result.metrics["accepted_count"], 1)
        self.assertEqual(result.metrics["rejected_count"], 2)
        self.assertEqual(result.metrics["duplicate_count"], 1)
        self.assertEqual(result.metrics["error_counts"]["invalid_numeric_value"], 1)

    def test_air_excludes_non_nyc_without_rejecting_it(self):
        rows = [
            air_row(),
            air_row(
                **{
                    "State Code": "01",
                    "County Code": "003",
                    "Site Num": "0010",
                    "State Name": "Alabama",
                    "County Name": "Baldwin",
                }
            ),
        ]
        frame = self.spark.createDataFrame(rows, AIR_QUALITY_RAW_SCHEMA)
        result = prepare(frame, load_dataset_config("air_quality"), run_id="air-test")
        self.addCleanup(result.release)
        self.assertEqual(result.metrics["raw_input_count"], 2)
        self.assertEqual(result.metrics["scope_excluded_count"], 1)
        self.assertEqual(result.metrics["input_count"], 1)
        self.assertEqual(result.metrics["accepted_count"], 1)
        accepted = result.accepted.first()
        self.assertEqual(accepted["site_id"], "36-061-0079")

    def test_zone_special_rows_are_normalized_and_retained(self):
        rows = [
            {"LocationID": 264, "Borough": "Unknown", "Zone": None, "service_zone": None},
            {"LocationID": 265, "Borough": None, "Zone": "Outside of NYC", "service_zone": None},
        ]
        frame = self.spark.createDataFrame(rows, TAXI_ZONES_RAW_SCHEMA)
        result = prepare(frame, load_dataset_config("taxi_zones"), run_id="zone-test")
        self.addCleanup(result.release)
        self.assertEqual(result.metrics["accepted_count"], 2)
        values = {
            row["location_id"]: (row["borough"], row["zone"], row["service_zone"])
            for row in result.accepted.collect()
        }
        self.assertEqual(values[264], ("Unknown", "Unknown", "Unknown"))
        self.assertEqual(
            values[265], ("Outside of NYC", "Outside of NYC", "Outside of NYC")
        )


if __name__ == "__main__":
    unittest.main()
