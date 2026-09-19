"""The four products must carry the same scope, labels and grain as the canonical Q1-Q6 SQL.

Products store observed rows only; these tests re-aggregate them and compare with
the query results, which is the check any product-backed query variant must pass
before it is treated as equivalent.
"""

import unittest
from collections import Counter, defaultdict
from datetime import datetime, timezone

from pyspark.sql import functions as F

from dic_pipeline.data_products import (
    build_air_quality_impact_summary,
    build_daily_mobility_summary,
    build_taxi_zone_statistics,
    build_weather_impact_summary,
)
from dic_pipeline.ingestion import create_spark
from dic_pipeline.queries import load_query_config, run_query


INTEGRATED_SCHEMA = (
    "record_id string, pickup_hour_utc timestamp, pickup_location_id int, pickup_zone string, "
    "pickup_borough string, environment_in_scope boolean, weather_matched boolean, "
    "weather_coco int, trip_distance double, trip_duration_seconds long, fare_amount double, "
    "air_quality_matched boolean, air_quality_pm25 double"
)
WEATHER_SCHEMA = "weather_hour_utc timestamp, coco int"
AIR_SCHEMA = (
    "site_id string, air_quality_hour_utc timestamp, measurement_value double, "
    "parameter_code string, measurement_unit string, method_type string, method_code string"
)


def utc(*args):
    return datetime(*args, tzinfo=timezone.utc)


H0 = utc(2024, 1, 1, 5)   # Monday 00:00 New York
H1 = utc(2024, 1, 1, 6)
H2 = utc(2024, 1, 1, 7)
H3 = utc(2024, 1, 8, 5)   # next Monday 00:00
H4 = utc(2024, 1, 1, 9)   # weather observed, no trips
HF = utc(2024, 2, 1, 5)   # Thursday 00:00, no environment data

TRIPS = [
    ("a1", H0, 1, "Alpha", "Manhattan", True, True, 1, 1.0, 600, 10.0, True, 4.5),
    ("a2", H0, 1, "Alpha", "Manhattan", True, True, 1, 3.0, 700, 12.0, True, 4.5),
    # Same hour, outside the NYC environment scope: counted by Q1/Q5/Q6 only.
    ("x1", H0, 265, "Outside of NYC", "Outside of NYC", False, False, None, 20.0, 900, 50.0, False, None),
    ("a3", H1, 1, "Alpha", "Manhattan", True, True, 2, 5.0, 500, 9.0, True, 9.0),
    ("a4", H1, 1, "Alpha", "Manhattan", True, True, 2, None, 400, 8.0, True, 9.0),
    ("b1", H1, 2, "Beta", "Queens", True, True, 2, 2.0, 300, 7.0, True, 9.0),
    ("b2", H2, 2, "Beta", "Queens", True, True, 8, 4.0, 800, 11.0, True, 13.0),
    ("b3", H2, 2, "Beta", "Queens", True, True, 8, 6.0, 850, 12.0, True, 13.0),
    ("b4", H3, 2, "Beta", "Queens", True, True, 8, 8.0, 1000, 15.0, True, 6.0),
    ("u1", HF, 1, "Alpha", "Manhattan", True, False, None, 10.0, 200, 5.0, False, None),
]
WEATHER = [(H0, 1), (H1, 2), (H2, 8), (H3, 8), (H4, 1)]
AIR_SITE_PAIRS = {H0: (4.0, 5.0), H1: (8.0, 10.0), H2: (12.0, 14.0), H3: (5.0, 7.0)}


class ProductQueryAlignmentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark(master="local[2]", driver_memory="2g", shuffle_partitions=2)
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.createDataFrame(TRIPS, INTEGRATED_SCHEMA).createOrReplaceTempView(
            "integrated_taxi_trips"
        )
        cls.spark.createDataFrame(WEATHER, WEATHER_SCHEMA).createOrReplaceTempView(
            "standardized_weather"
        )
        air_rows = [
            (site, hour, value, "88101", "Micrograms/cubic meter (LC)", "FEM", "636")
            for hour, values in AIR_SITE_PAIRS.items()
            for site, value in zip(("site-1", "site-2"), values)
        ]
        cls.spark.createDataFrame(air_rows, AIR_SCHEMA).createOrReplaceTempView(
            "standardized_air_quality"
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_zone_statistics_match_q1(self):
        canonical = {
            (row.local_pickup_month, row.pickup_location_id): row.trip_count
            for row in run_query(self.spark, "q1").collect()
        }
        product = {
            (row.local_pickup_month, row.pickup_location_id): row.trip_count
            for row in build_taxi_zone_statistics(self.spark).collect()
        }
        self.assertEqual(product, canonical)
        self.assertEqual(product[("2024-01", 265)], 1)

    def test_weather_summary_reaggregates_to_q2(self):
        canonical = {row.weather_category: row for row in run_query(self.spark, "q2").collect()}
        totals = defaultdict(lambda: [0, 0, 0.0])
        for row in build_weather_impact_summary(self.spark).collect():
            bucket = totals[row.weather_category]
            bucket[0] += row.trip_count
            bucket[1] += row.valid_distance_count
            bucket[2] += row.distance_sum or 0.0
        self.assertEqual(set(totals), set(canonical))
        for category, row in canonical.items():
            trips, valid, distance = totals[category]
            self.assertEqual(trips, row.trip_count)
            self.assertEqual(valid, row.valid_distance_count)
            if valid:
                self.assertAlmostEqual(distance / valid, row.average_trip_distance)
            else:
                self.assertIsNone(row.average_trip_distance)
        # Frozen labels, NYC scope: the out-of-scope trip x1 is absent, the in-scope u1 stays.
        self.assertNotIn("1", totals)
        self.assertEqual(totals["unmatched"][0], 1)
        self.assertEqual(totals["clear_or_fair"][0], 5)

    def test_air_summary_counts_nyc_demand_per_hour_like_q3(self):
        product = {
            row.hour: row
            for row in build_air_quality_impact_summary(self.spark)
            .withColumn("hour", F.col("pickup_hour_utc").cast("string"))
            .collect()
        }
        # One NYC trip pair plus one out-of-scope trip in the same hour: Q3 demand is 2.
        self.assertEqual(product["2024-01-01 05:00:00"].trip_count, 2)
        self.assertEqual(product["2024-01-01 05:00:00"].air_quality_pm25, 4.5)
        self.assertEqual(product["2024-01-01 05:00:00"].match_status, "matched")
        self.assertEqual(product["2024-02-01 05:00:00"].match_status, "unmatched")
        canonical = run_query(self.spark, "q3").collect()
        self.assertEqual(
            sum(row.total_trip_count for row in canonical),
            sum(row.trip_count for row in product.values()),
        )

    def test_daily_summary_reaggregates_to_q6_and_q5_peaks(self):
        daily = build_daily_mobility_summary(self.spark).collect()
        monthly = defaultdict(int)
        buckets = defaultdict(int)
        for row in daily:
            monthly[row.local_pickup_date.strftime("%Y-%m")] += row.trip_count
            buckets[(row.local_weekday, row.local_pickup_hour)] += row.trip_count
        canonical_months = {
            row.local_pickup_month: row.trip_count for row in run_query(self.spark, "q6").collect()
        }
        self.assertEqual(dict(monthly), canonical_months)
        self.assertEqual(sum(monthly.values()), len(TRIPS))
        peaks = run_query(self.spark, "q5").collect()
        # Monday 00:00 New York: a1, a2, the out-of-scope x1 and b4 a week later; 01:00 has three.
        monday = {row.local_pickup_hour: row for row in peaks if row.weekday_name == "Monday"}
        self.assertEqual(set(monday), {0})
        self.assertEqual(monday[0].total_trip_count, 4)
        for row in peaks:
            self.assertEqual(
                row.total_trip_count,
                buckets.get((row.weekday_name, row.local_pickup_hour), 0),
            )

    def test_weather_summary_rebuilds_q4_demand_range(self):
        config = load_query_config()
        label = {code: item["label"] for item in config["weather_categories"] for code in item["codes"]}
        minimum = config["minimum_weather_hours_per_category"]
        # Every fixture weather hour lies inside the validated coverage calendar.
        hours_per_category = Counter(label[coco] for _, coco in WEATHER)
        product = defaultdict(dict)
        for row in build_weather_impact_summary(self.spark).collect():
            product[row.pickup_location_id][row.weather_category] = row.trip_count
        canonical = {row.pickup_location_id: row for row in run_query(self.spark, "q4").collect()}
        self.assertEqual(set(canonical), set(product))
        retained_hours = sum(hours for hours in hours_per_category.values() if hours >= minimum)
        for zone, categories in product.items():
            averages = [
                categories.get(category, 0) / hours
                for category, hours in hours_per_category.items()
                if hours >= minimum
            ]
            self.assertAlmostEqual(canonical[zone].demand_range, max(averages) - min(averages))
            self.assertEqual(canonical[zone].weather_hour_count, retained_hours)
        self.assertAlmostEqual(canonical[1].demand_range, 4 / 3)
        self.assertAlmostEqual(canonical[2].demand_range, 1.5 - 1 / 3)


if __name__ == "__main__":
    unittest.main()
