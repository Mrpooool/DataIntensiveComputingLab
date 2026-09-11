import unittest
from datetime import datetime

from dic_pipeline.ingestion import create_spark
from dic_pipeline.integration import (
    AIR_SIGNATURE, WEATHER_METRICS, add_environment, aggregate_air_quality, integration_metrics,
)


HOUR = datetime(2024, 1, 15, 17)
AIR_SCHEMA = (
    "site_id string, monitor_id string, air_quality_hour_utc timestamp, "
    "parameter_code string, measurement_unit string, method_type string, method_code string, "
    "measurement_value double, qualifier string"
)
TRIP_SCHEMA = "record_id string, pickup_hour_utc timestamp, pickup_borough string"


def air_row(site, value, monitor="1", hour=HOUR, qualifier=None):
    return (site, f"{site}-{monitor}", hour, *AIR_SIGNATURE, value, qualifier)


def weather_frame(spark, hours):
    schema = "weather_hour_utc timestamp, weather_timestamp_source timestamp"
    for metric in WEATHER_METRICS:
        schema += f", {metric} double, {metric}_source string"
    schema += ", quality_flags array<string>"
    rows = []
    for hour in hours:
        values = [hour, hour]
        for metric in WEATHER_METRICS:
            values.extend([None if metric in ("prcp", "snwd", "wpgt") else 5.0, "sample"])
        rows.append((*values, ["missing_precipitation"]))
    return spark.createDataFrame(rows, schema)


class EnvironmentIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark(master="local[2]", shuffle_partitions=2)
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_site_weight_is_independent_of_instrument_count(self):
        next_hour = datetime(2024, 1, 15, 18)
        rows = [
            air_row("A", 2.0, "1", qualifier="flag"), air_row("A", 2.0, "2"),
            air_row("A", 2.0, "3"), air_row("B", 20.0),
            air_row("A", 8.0, hour=next_hour),
        ]
        result = aggregate_air_quality(self.spark.createDataFrame(rows, AIR_SCHEMA))
        values = {row.air_quality_hour_utc: row for row in result.collect()}
        self.assertEqual(len(values), 2)
        self.assertEqual(values[HOUR].air_quality_pm25, 11.0)
        self.assertEqual(values[HOUR].air_quality_site_count, 2)
        self.assertEqual(values[HOUR].air_quality_flagged_observation_count, 1)
        self.assertEqual(values[next_hour].air_quality_site_count, 1)
        self.assertEqual(values[next_hour].air_quality_pm25, 8.0)

    def test_incompatible_pollutant_unit_and_method_are_rejected(self):
        for column, value in ((3, "44201"), (4, "Parts per million"), (5, "OTHER"), (6, "999")):
            with self.subTest(column=column):
                changed = list(air_row("B", 8.0))
                changed[column] = value
                air = self.spark.createDataFrame([air_row("A", 2.0), tuple(changed)], AIR_SCHEMA)
                with self.assertRaisesRegex(ValueError, "requires PM2.5"):
                    aggregate_air_quality(air)

    def test_nyc_scope_missing_hours_and_coverage_denominators(self):
        next_hour = datetime(2024, 1, 15, 18)
        trips = self.spark.createDataFrame([
            ("nyc-1", HOUR, "Manhattan"), ("nyc-2", HOUR, "Brooklyn"),
            ("air-only", next_hour, "Queens"),
            ("missing", datetime(2024, 1, 16, 17), "Manhattan"),
            ("airport", HOUR, "EWR"), ("unknown", HOUR, "Unknown"),
            ("outside", HOUR, "Outside of NYC"), ("null-borough", HOUR, None),
        ], TRIP_SCHEMA)
        weather = weather_frame(self.spark, [HOUR])
        air = self.spark.createDataFrame([
            air_row("A", 8.0), air_row("B", 12.0), air_row("A", 6.0, hour=next_hour),
        ], AIR_SCHEMA)
        result = add_environment(trips, weather, air)
        self.assertCountEqual(result.select(*trips.columns).collect(), trips.collect())
        values = {row.record_id: row for row in result.collect()}
        self.assertEqual(values["nyc-1"].air_quality_pm25, 10.0)
        self.assertTrue(values["nyc-1"].weather_matched)
        self.assertIsNone(values["nyc-1"].weather_prcp)
        self.assertEqual(values["nyc-1"].weather_temp_source, "sample")
        self.assertFalse(values["air-only"].weather_matched)
        self.assertTrue(values["air-only"].air_quality_matched)
        for name in ("missing", "airport", "unknown", "outside", "null-borough"):
            self.assertIsNone(values[name].weather_temp)
            self.assertIsNone(values[name].air_quality_pm25)
            self.assertFalse(values[name].weather_matched)
            self.assertFalse(values[name].air_quality_matched)
        stats = integration_metrics(trips, result)
        self.assertEqual(stats["nyc_trip_count"], 4)
        self.assertEqual(stats["weather_match_rate_overall"], 2 / 8)
        self.assertEqual(stats["weather_match_rate_nyc"], 2 / 4)
        self.assertEqual(stats["air_quality_match_rate_nyc"], 3 / 4)
        self.assertEqual(stats["weather_prcp_nonnull_count"], 0)

    def test_duplicate_weather_hour_fails_before_join(self):
        trips = self.spark.createDataFrame([("trip", HOUR, "Manhattan")], TRIP_SCHEMA)
        air = self.spark.createDataFrame([], AIR_SCHEMA)
        weather = weather_frame(self.spark, [HOUR, HOUR])
        with self.assertRaisesRegex(ValueError, "weather_hour_utc must be non-null and unique"):
            add_environment(trips, weather, air)

    def test_empty_environment_and_empty_trips(self):
        weather = weather_frame(self.spark, [])
        air = self.spark.createDataFrame([], AIR_SCHEMA)
        for rows in ([("trip", HOUR, "Manhattan")], []):
            with self.subTest(rows=rows):
                trips = self.spark.createDataFrame(rows, TRIP_SCHEMA)
                result = add_environment(trips, weather, air)
                stats = integration_metrics(trips, result)
                self.assertEqual(stats["output_count"], len(rows))
                self.assertEqual(stats["weather_match_count"], 0)
                self.assertEqual(stats["air_quality_match_count"], 0)
                if not rows:
                    self.assertIsNone(stats["weather_match_rate_overall"])


if __name__ == "__main__":
    unittest.main()
