"""Regression checks for the Week 2 experiment runner. Timings are recorded, never asserted."""

import tempfile
import unittest
from pathlib import Path

from pyspark.sql import functions as F

from dic_pipeline.data_products import DEFAULT_PRODUCT_BUILDERS
from dic_pipeline.ingestion import create_spark, write_delta
from dic_pipeline.query_benchmark import (
    Experiment,
    Variant,
    build_experiments,
    plan_indicators,
    register_products,
    run_experiment,
    spark_conf,
)
from dic_pipeline.queries import render_query
from tests.test_product_query_alignment import (
    AIR_SCHEMA,
    AIR_SITE_PAIRS,
    INTEGRATED_SCHEMA,
    TRIPS,
    WEATHER,
    WEATHER_SCHEMA,
)


ZONES = [(1, "Alpha", "Manhattan"), (2, "Beta", "Queens"), (265, "Outside of NYC", "Outside of NYC")]


class QueryBenchmarkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark(master="local[2]", driver_memory="2g", shuffle_partitions=2)
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.directory = tempfile.TemporaryDirectory()
        root = Path(cls.directory.name)
        trips = cls.spark.createDataFrame(TRIPS, INTEGRATED_SCHEMA).withColumn(
            "pickup_date", F.to_date(F.from_utc_timestamp("pickup_hour_utc", "America/New_York"))
        )
        # A real partitioned Delta table, so partition filters show up in plans.
        integrated_path = root / "integrated" / "integrated_taxi_trips"
        write_delta(trips, integrated_path, partition_by=["pickup_date"])
        integrated = cls.spark.read.format("delta").load(str(integrated_path))
        integrated.createOrReplaceTempView("integrated_taxi_trips")
        integrated.createOrReplaceTempView("standardized_taxi")
        cls.spark.createDataFrame(ZONES, "location_id int, zone string, borough string").createOrReplaceTempView(
            "standardized_taxi_zones"
        )
        cls.spark.createDataFrame(WEATHER, WEATHER_SCHEMA).createOrReplaceTempView("standardized_weather")
        air_rows = [
            (site, hour, value, "88101", "Micrograms/cubic meter (LC)", "FEM", "636")
            for hour, values in AIR_SITE_PAIRS.items()
            for site, value in zip(("site-1", "site-2"), values)
        ]
        cls.spark.createDataFrame(air_rows, AIR_SCHEMA).createOrReplaceTempView("standardized_air_quality")
        for name, builder in DEFAULT_PRODUCT_BUILDERS.items():
            write_delta(builder(cls.spark), root / "analytics" / name, num_files=1)
        cls.products = register_products(cls.spark, root)
        cls.root = root

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def test_every_experiment_verifies_results_and_records_plans(self):
        adaptive_before = self.spark.conf.get("spark.sql.adaptive.enabled")
        threshold_before = self.spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
        experiments = build_experiments(
            start_date="2024-01-01", end_date="2024-01-02", products=self.products
        )
        self.assertEqual(len(self.products), 4)
        self.assertEqual(len(experiments), 13)
        with tempfile.TemporaryDirectory() as output:
            results = {}
            for experiment in experiments:
                with self.subTest(experiment=experiment.name):
                    result = run_experiment(self.spark, experiment, output, repeats=1)
                    self.assertTrue(result["results_equal"], result.get("mismatch"))
                    self.assertEqual(len(result["samples"]), 2)
                    self.assertGreater(result["speedup"], 0)
                    for variant in ("baseline", "optimized"):
                        self.assertTrue((Path(output) / "plans" / f"{experiment.name}_{variant}.txt").exists())
                        self.assertTrue((Path(output) / "sql" / f"{experiment.name}_{variant}.sql").exists())
                    results[experiment.name] = result

        plans = {
            name: {variant: result["variants"][variant]["plan"] for variant in ("baseline", "optimized")}
            for name, result in results.items()
        }
        self.assertEqual(plans["q1_partition_pruning"]["baseline"]["partition_filters"], [])
        self.assertTrue(plans["q1_partition_pruning"]["optimized"]["partition_filters"])
        self.assertTrue(plans["q1_broadcast_zones"]["baseline"]["sort_merge_join"])
        self.assertFalse(plans["q1_broadcast_zones"]["baseline"]["broadcast_hash_join"])
        self.assertTrue(plans["q1_broadcast_zones"]["optimized"]["broadcast_hash_join"])
        self.assertFalse(plans["q4_cache_projection"]["baseline"]["in_memory_table_scan"])
        self.assertTrue(plans["q4_cache_projection"]["optimized"]["in_memory_table_scan"])
        self.assertFalse(plans["q4_aqe"]["baseline"]["adaptive_final_plan"])
        self.assertTrue(plans["q4_aqe"]["optimized"]["adaptive_final_plan"])

        cache = results["q4_cache_projection"]["cache"]
        self.assertEqual(cache["rows"], len(TRIPS))
        self.assertGreaterEqual(cache["build_seconds"], 0)
        self.assertGreater(cache["memory_bytes"] + cache["disk_bytes"], 0)
        self.assertEqual(
            sorted(name for name in results if name.endswith(tuple(self.products))),
            sorted(f"{q}_product_{p}" for q, p in (
                ("q1", "taxi_zone_statistics"), ("q2", "weather_impact_summary"),
                ("q3", "air_quality_impact_summary"), ("q4", "weather_impact_summary"),
                ("q5", "daily_mobility_summary"), ("q6", "daily_mobility_summary"),
            )),
        )
        # Settings are restored and the experiment's own cache is released (Delta keeps
        # log-state RDDs cached on its own, so compare against the pre-build level).
        self.assertEqual(self.spark.conf.get("spark.sql.adaptive.enabled"), adaptive_before)
        self.assertEqual(self.spark.conf.get("spark.sql.autoBroadcastJoinThreshold"), threshold_before)
        self.assertGreater(cache["cached_partitions"], 0)
        self.assertEqual(
            cache["storage_after_release"]["cached_partitions"],
            cache["storage_before"]["cached_partitions"],
        )

    def test_mismatching_variant_is_reported_and_never_timed(self):
        sql = render_query("q6")
        experiment = Experiment(
            name="q6_wrong_rewrite",
            query_id="q6",
            technique="product",
            rationale="deliberately wrong",
            baseline=Variant("baseline", sql),
            optimized=Variant("optimized", sql + "\nLIMIT 1"),
        )
        with tempfile.TemporaryDirectory() as output:
            result = run_experiment(self.spark, experiment, output, repeats=2)
        self.assertFalse(result["results_equal"])
        self.assertIn("row counts differ", result["mismatch"])
        self.assertEqual(result["samples"], [])
        self.assertNotIn("speedup", result)

    def test_spark_conf_context_restores_previous_values(self):
        before = self.spark.conf.get("spark.sql.adaptive.enabled")
        flipped = "false" if before == "true" else "true"
        with spark_conf(self.spark, {"spark.sql.adaptive.enabled": flipped}):
            self.assertEqual(self.spark.conf.get("spark.sql.adaptive.enabled"), flipped)
        self.assertEqual(self.spark.conf.get("spark.sql.adaptive.enabled"), before)

    def test_plan_indicators_extract_facts_from_text(self):
        plan = (
            "AdaptiveSparkPlan isFinalPlan=true\n"
            "BroadcastHashJoin ... AQEShuffleRead ... Exchange ... Exchange\n"
            "PartitionFilters: [isnotnull(pickup_date#1), (pickup_date#1 >= 2024-02-01)]\n"
            "PartitionFilters: []\n"
        )
        facts = plan_indicators(plan)
        self.assertTrue(facts["adaptive_final_plan"])
        self.assertTrue(facts["broadcast_hash_join"])
        self.assertFalse(facts["sort_merge_join"])
        self.assertEqual(facts["aqe_shuffle_reads"], 1)
        self.assertEqual(facts["exchanges"], 2)
        self.assertEqual(len(facts["partition_filters"]), 1)


if __name__ == "__main__":
    unittest.main()
