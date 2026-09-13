import unittest

from dic_pipeline.ingestion import create_spark
from dic_pipeline.integration import add_taxi_zones


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


if __name__ == "__main__":
    unittest.main()
