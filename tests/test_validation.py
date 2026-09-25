"""Week 3 role B: extensible row rules, references, and schema policy."""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from dic_pipeline.contracts import load_dataset_config
from dic_pipeline.ingestion import create_spark
from dic_pipeline.incremental import read_update_source
from dic_pipeline.preparation import SchemaValidationError, prepare
from dic_pipeline.schemas import (
    AIR_QUALITY_RAW_SCHEMA,
    REQUIRED_RAW_COLUMNS,
    TAXI_RAW_SCHEMA,
    WEATHER_RAW_SCHEMA,
)
from dic_pipeline.validation import (
    check_schema,
    register_rule_builder,
    unregister_rule_builder,
)
from tests.test_preparation import air_row, taxi_row, weather_row


class SchemaPolicyTests(unittest.TestCase):
    def test_additive_policy_checks_declared_type(self):
        expected = StructType([StructField("value", DoubleType(), True)])
        accepted_schema = StructType(
            [
                StructField("value", DoubleType(), True),
                StructField("humidity", DoubleType(), True),
            ]
        )
        accepted, unsupported = check_schema(
            accepted_schema,
            expected,
            policy={
                "allow_add": {
                    "humidity": {"type": "double", "nullable": True}
                }
            },
        )
        self.assertEqual(unsupported, [])
        self.assertEqual(
            accepted,
            [{"op": "add", "column": "humidity", "type": "double", "nullable": True}],
        )

        wrong_type = StructType(
            [
                StructField("value", DoubleType(), True),
                StructField("humidity", StringType(), True),
            ]
        )
        _accepted, unsupported = check_schema(
            wrong_type,
            expected,
            policy={"allow_add": {"humidity": {"type": "double"}}},
        )
        self.assertEqual(unsupported[0]["op"], "change_type")
        self.assertEqual(unsupported[0]["column"], "humidity")

    def test_removal_type_change_and_unlisted_addition_are_unsupported(self):
        expected = StructType(
            [
                StructField("required", DoubleType(), True),
                StructField("other", StringType(), True),
            ]
        )
        actual = StructType(
            [
                StructField("required", StringType(), True),
                StructField("surprise", DoubleType(), True),
            ]
        )
        accepted, unsupported = check_schema(actual, expected)
        self.assertEqual(accepted, [])
        changes = {(item["op"], item["column"]) for item in unsupported}
        self.assertEqual(
            changes,
            {
                ("remove", "other"),
                ("add", "surprise"),
                ("change_type", "required"),
            },
        )

    def test_duplicate_name_and_relaxed_nullability_are_unsupported(self):
        expected = StructType(
            [
                StructField("required", DoubleType(), False),
                StructField("label", StringType(), True),
            ]
        )
        actual = StructType(
            [
                StructField("required", DoubleType(), True),
                StructField("label", StringType(), True),
                StructField("label", StringType(), True),
            ]
        )
        _accepted, unsupported = check_schema(actual, expected)
        changes = {(item["op"], item["column"]) for item in unsupported}
        self.assertEqual(
            changes,
            {
                ("duplicate", "label"),
                ("relax_nullability", "required"),
            },
        )


class ExtendedValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark(master="local[2]", shuffle_partitions=2)
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_missing_zone_reference_is_isolated_and_reported(self):
        rows = [
            taxi_row(PULocationID=161, DOLocationID=236),
            taxi_row(
                tpep_pickup_datetime=datetime(2024, 1, 16, 12, 0),
                tpep_dropoff_datetime=datetime(2024, 1, 16, 12, 20),
                PULocationID=42,
                DOLocationID=236,
            ),
        ]
        result = prepare(
            self.spark.createDataFrame(rows, TAXI_RAW_SCHEMA),
            load_dataset_config("taxi"),
            reference_data={"taxi_zone_ids": (161, 236)},
        )
        self.addCleanup(result.release)
        self.assertEqual(result.metrics["accepted_count"], 1)
        self.assertEqual(result.metrics["rejected_count"], 1)
        self.assertIn(
            "missing_reference_record",
            result.rejected.first()["error_reasons"],
        )

    def test_evolved_values_are_required_and_range_checked(self):
        weather_schema = StructType(
            [*WEATHER_RAW_SCHEMA.fields, StructField("humidity", DoubleType(), True)]
        )
        weather_rows = [
            {**weather_row(hour=17), "humidity": 55.0},
            {**weather_row(hour=18), "humidity": 120.0},
            {**weather_row(hour=19), "humidity": None},
        ]
        weather = prepare(
            self.spark.createDataFrame(weather_rows, weather_schema),
            load_dataset_config("weather"),
        )
        self.addCleanup(weather.release)
        self.assertEqual(weather.metrics["accepted_count"], 1)
        self.assertEqual(weather.metrics["error_counts"]["invalid_attribute_value"], 1)
        self.assertEqual(weather.metrics["error_counts"]["incomplete_record"], 1)

        air_schema = StructType(
            [*AIR_QUALITY_RAW_SCHEMA.fields, StructField("aqi", DoubleType(), True)]
        )
        air_rows = [
            {**air_row(), "aqi": 42.0},
            {**air_row(**{"Time GMT": "18:00", "POC": 2}), "aqi": 501.0},
        ]
        air = prepare(
            self.spark.createDataFrame(air_rows, air_schema),
            load_dataset_config("air_quality"),
        )
        self.addCleanup(air.release)
        self.assertEqual(air.metrics["accepted_count"], 1)
        self.assertEqual(air.metrics["error_counts"]["invalid_attribute_value"], 1)

    def test_validation_switch_keeps_duplicate_collapse(self):
        rows = [
            weather_row(hour=17, rhum=101.0),
            weather_row(hour=18),
            weather_row(hour=18),
        ]
        result = prepare(
            self.spark.createDataFrame(rows, WEATHER_RAW_SCHEMA),
            load_dataset_config("weather"),
            validate=False,
        )
        self.addCleanup(result.release)
        self.assertFalse(result.metrics["validation_enabled"])
        self.assertEqual(result.metrics["accepted_count"], 2)
        self.assertEqual(result.metrics["rejected_count"], 1)
        self.assertEqual(result.metrics["error_counts"], {"duplicate_record": 1})

    def test_extra_rule_builder_does_not_modify_core(self):
        def reject_extreme_temperature(df, _config, _references):
            return [("extreme_temperature", F.col("temp") > 50)], []

        register_rule_builder("weather", reject_extreme_temperature)
        try:
            frame = self.spark.createDataFrame(
                [weather_row(temp=60.0)], WEATHER_RAW_SCHEMA
            )
            result = prepare(frame, load_dataset_config("weather"))
            self.addCleanup(result.release)
            self.assertEqual(result.metrics["rejected_count"], 1)
            self.assertIn(
                "extreme_temperature", result.rejected.first()["error_reasons"]
            )
        finally:
            unregister_rule_builder("weather", reject_extreme_temperature)

    def test_update_reader_rejects_unlisted_schema_change(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "weather_bad_schema.csv"
            path.write_text(
                ",".join([*REQUIRED_RAW_COLUMNS["weather"], "surprise"]) + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                SchemaValidationError, "unsupported schema changes"
            ):
                read_update_source(self.spark, "weather", path)


if __name__ == "__main__":
    unittest.main()
