"""Run and verify the first integration step against the completed batch."""

import argparse
import json
from pathlib import Path

from pyspark.sql import functions as F

from src.dic_pipeline.ingestion import create_spark, read_completed_batch
from src.dic_pipeline.integration import add_taxi_zones


def main() -> None:
    parser = argparse.ArgumentParser(description="Join Taxi trips to pickup/dropoff zones")
    parser.add_argument("--delta-root", type=Path, default=Path("data/delta"))
    args = parser.parse_args()
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        tables = read_completed_batch(spark, args.delta_root)
        taxi = tables["taxi"]
        enriched = add_taxi_zones(taxi, tables["taxi_zones"])
        stats = enriched.agg(
            F.count("*").alias("output_count"),
            F.count(F.when(F.col("pickup_zone").isNull(), 1)).alias("missing_pickup_zone_count"),
            F.count(F.when(F.col("dropoff_zone").isNull(), 1)).alias("missing_dropoff_zone_count"),
        ).first().asDict()
        stats["input_count"] = taxi.count()
        if stats["output_count"] != stats["input_count"]:
            raise RuntimeError("Zone integration changed the Taxi row count.")
        print(json.dumps(stats, sort_keys=True), flush=True)
        enriched.select(
            "pickup_location_id", "pickup_zone", "pickup_borough",
            "dropoff_location_id", "dropoff_zone", "dropoff_borough",
        ).show(5, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
