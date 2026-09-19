import json
from pathlib import Path
import tempfile
import unittest
from datetime import datetime, timezone

from dic_pipeline.ingestion import create_spark
from dic_pipeline.queries import QUERY_DEFINITIONS, render_query, run_query


INTEGRATED_SCHEMA = (
    "record_id string, pickup_hour_utc timestamp, pickup_location_id int, "
    "pickup_zone string, pickup_borough string, environment_in_scope boolean, "
    "weather_matched boolean, weather_coco int, trip_distance double"
)
AIR_SCHEMA = (
    "site_id string, air_quality_hour_utc timestamp, measurement_value double, "
    "parameter_code string, measurement_unit string, method_type string, method_code string"
)
WEATHER_SCHEMA = "weather_hour_utc timestamp, coco int"


class AnalyticalQueryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark(master="local[2]", driver_memory="2g", shuffle_partitions=2)
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        hours = [
            datetime(2024, 1, 1, hour, tzinfo=timezone.utc)
            for hour in (5, 6, 7, 8)
        ]
        february = datetime(2024, 2, 1, 5, tzinfo=timezone.utc)
        rows = [
            ("a1", hours[0], 1, "Alpha", "Manhattan", True, True, 1, 1.0),
            ("a2", hours[0], 1, "Alpha", "Manhattan", True, True, 1, 3.0),
            ("a3", hours[1], 1, "Alpha", "Manhattan", True, True, 2, 5.0),
            ("a4", hours[1], 1, "Alpha", "Manhattan", True, True, 2, None),
            ("b1", hours[1], 2, "Beta", "Queens", True, True, 2, 2.0),
            ("b2", hours[2], 2, "Beta", "Queens", True, True, 8, 4.0),
            ("b3", hours[2], 2, "Beta", "Queens", True, True, 8, 6.0),
            ("b4", hours[3], 2, "Beta", "Queens", True, True, 8, 8.0),
            ("f1", february, 1, "Alpha", "Manhattan", True, False, None, 10.0),
        ]
        self.spark.createDataFrame(rows, INTEGRATED_SCHEMA).createOrReplaceTempView(
            "integrated_taxi_trips"
        )
        weather_rows = [
            (hours[0], 1),
            (hours[1], 2),
            (hours[2], 8),
            (hours[3], 8),
        ]
        self.spark.createDataFrame(weather_rows, WEATHER_SCHEMA).createOrReplaceTempView(
            "standardized_weather"
        )
        air_rows = []
        for hour, value in zip(hours[:3], (4.0, 8.0, 12.0)):
            air_rows.extend(
                [
                    ("site-1", hour, value, "88101", "Micrograms/cubic meter (LC)", "FEM", "636"),
                    ("site-2", hour, value + 2.0, "88101", "Micrograms/cubic meter (LC)", "FEM", "636"),
                ]
            )
        self.spark.createDataFrame(air_rows, AIR_SCHEMA).createOrReplaceTempView(
            "standardized_air_quality"
        )

    def test_registry_and_templates_cover_all_six_queries(self):
        self.assertEqual(list(QUERY_DEFINITIONS), ["q1", "q2", "q3", "q4", "q5", "q6"])
        for query_id in QUERY_DEFINITIONS:
            sql = render_query(query_id, start_date="2024-01-01", end_date="2024-02-01")
            self.assertNotIn("{", sql)
            self.assertIn("integrated_taxi_trips", sql)

    def test_q1_monthly_demand_by_zone(self):
        rows = run_query(self.spark, "q1").collect()
        values = {(row.local_pickup_month, row.pickup_location_id): row.trip_count for row in rows}
        self.assertEqual(values[("2024-01", 1)], 4)
        self.assertEqual(values[("2024-01", 2)], 4)
        self.assertEqual(values[("2024-02", 1)], 1)

    def test_q2_distance_null_denominator_and_weather_mapping(self):
        rows = {
            row.weather_category: row
            for row in run_query(
                self.spark,
                "q2",
                start_date="2024-01-01",
                end_date="2024-02-01",
            ).collect()
        }
        self.assertEqual(rows["clear_or_fair"].trip_count, 5)
        self.assertEqual(rows["clear_or_fair"].valid_distance_count, 4)
        self.assertAlmostEqual(rows["clear_or_fair"].average_trip_distance, 2.75)
        self.assertEqual(rows["rain"].trip_count, 3)
        self.assertAlmostEqual(rows["rain"].average_trip_distance, 6.0)
        all_rows = {row.weather_category: row for row in run_query(self.spark, "q2").collect()}
        self.assertEqual(all_rows["unmatched"].trip_count, 1)

    def test_q3_includes_zero_demand_hours_and_two_level_air_median(self):
        rows = {row.pm25_band: row for row in run_query(
            self.spark,
            "q3",
            start_date="2024-01-01",
            end_date="2024-01-02",
        ).collect()}
        self.assertEqual(sum(row.hour_count for row in rows.values()), 4)
        self.assertEqual(sum(row.total_trip_count for row in rows.values()), 8)
        self.assertAlmostEqual(rows["0_to_5"].average_pm25, 5.0)
        self.assertIsNotNone(rows["0_to_5"].overall_pm25_demand_correlation)
        self.assertEqual(rows["missing_pm25"].hour_count, 1)
        self.assertEqual(rows["missing_pm25"].total_trip_count, 1)
        self.assertIsNone(rows["missing_pm25"].average_pm25)

    def test_q4_ranks_zone_variation_with_zero_zone_hours(self):
        rows = run_query(
            self.spark,
            "q4",
            start_date="2024-01-01",
            end_date="2024-01-02",
        ).collect()
        self.assertEqual([row.pickup_location_id for row in rows], [1, 2])
        self.assertAlmostEqual(rows[0].demand_range, 2.0)
        self.assertAlmostEqual(rows[1].demand_range, 1.0)
        self.assertEqual(rows[0].weather_hour_count, 4)

    def test_q5_peak_hour_and_ties_use_complete_calendar(self):
        rows = run_query(
            self.spark,
            "q5",
            start_date="2024-01-01",
            end_date="2024-01-02",
        ).collect()
        monday = [row for row in rows if row.weekday_name == "Monday"]
        self.assertEqual(len(monday), 1)
        self.assertEqual(monday[0].local_pickup_hour, 1)
        self.assertEqual(monday[0].total_trip_count, 3)

    def test_q6_monthly_change_and_half_open_date_filter(self):
        rows = run_query(self.spark, "q6").collect()
        self.assertEqual([(row.local_pickup_month, row.trip_count) for row in rows], [
            ("2024-01", 8),
            ("2024-02", 1),
        ])
        self.assertIsNone(rows[0].month_over_month_percent)
        self.assertAlmostEqual(rows[1].month_over_month_percent, -87.5)
        january = run_query(
            self.spark,
            "q6",
            start_date="2024-01-01",
            end_date="2024-02-01",
        ).collect()
        self.assertEqual([(row.local_pickup_month, row.trip_count) for row in january], [("2024-01", 8)])

    def test_invalid_configuration_is_rejected_before_sql_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "queries.json"
            config = json.loads(Path("configs/analytical_queries.json").read_text(encoding="utf-8"))
            config["weather_categories"][0]["codes"].append(3)
            path.write_text(json.dumps(config), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "only one category"):
                render_query("q1", config_path=path)


if __name__ == "__main__":
    unittest.main()
