"""Week 4 Approach A: the zone-hour training dataset straight from the four raw files.

Everything the platform did in Weeks 1-3 for this task happens here, without Delta,
audit columns, rejected-row quarantine or monitoring: parse the files, convert Taxi
local time to UTC, drop the trips the platform would reject, collapse duplicates,
keep the NYC PM2.5 monitors and take the two-level hourly median. Only what follows
integration is shared with the platform route: ``build_training_dataset`` and Role B's
feature and training pipeline.

The Taxi rules mirror the platform's rejection rules, because each of them changes a
pickup count. Weather and Air keep only the checks on the values they feed; the
platform's other range rules rejected no row of the 2024 files, and the route
comparison checks that both routes still produce identical rows.
"""

from __future__ import annotations

from functools import reduce
from operator import or_
from pathlib import Path
from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession, functions as F

from .ml_dataset import build_training_dataset, dataset_bounds


TAXI_TIMEZONE = "America/New_York"
NYC_COUNTIES = ("Bronx", "Kings", "New York", "Queens", "Richmond")
PM25_UNIT = "Micrograms/cubic meter (LC)"
TAXI_NUMERIC = (
    "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge", "Airport_fee",
)


def _csv(spark: SparkSession, path: Path) -> DataFrame:
    return spark.read.option("header", "true").csv(str(path))


def _zones(spark: SparkSession, path: Path) -> DataFrame:
    return _csv(spark, path).select(
        F.col("LocationID").cast("int").alias("location_id"),
        F.trim("Zone").alias("zone"),
        F.trim("Borough").alias("borough"),
    )


def _trips(
    spark: SparkSession, paths: list[Path], zone_ids: list[int], config: Mapping[str, Any],
) -> DataFrame:
    start, _, _, end = dataset_bounds(config)
    taxi = spark.read.parquet(*[str(path) for path in paths])
    pickup = F.to_utc_timestamp("tpep_pickup_datetime", TAXI_TIMEZONE)
    dropoff = F.to_utc_timestamp("tpep_dropoff_datetime", TAXI_TIMEZONE)
    rejected = reduce(or_, [
        *[F.col(name).isNull() for name in (
            "VendorID", "PULocationID", "DOLocationID", "trip_distance", "fare_amount",
        )],
        pickup.isNull(), dropoff.isNull(),
        (pickup < F.lit(start)) | (pickup >= F.lit(end)),
        dropoff < pickup,
        *[F.isnan(name) | F.col(name).isin(float("inf"), float("-inf")) for name in TAXI_NUMERIC],
        (F.col("trip_distance") < 0) | (F.col("passenger_count") < 0),
        ~F.col("PULocationID").isin(*zone_ids) | ~F.col("DOLocationID").isin(*zone_ids),
    ])
    # A null comparison leaves an optional field unchecked, as on the platform.
    return taxi.where(~F.coalesce(rejected, F.lit(False))).dropDuplicates().select(
        F.col("PULocationID").cast("int").alias("pickup_location_id"),
        F.date_trunc("hour", pickup).alias("pickup_hour_utc"),
    )


def _weather(spark: SparkSession, path: Path) -> DataFrame:
    hour = F.try_to_timestamp(F.format_string(
        "%04d-%02d-%02d %02d:00:00",
        *[F.col(name).cast("int") for name in ("year", "month", "day", "hour")],
    ), F.lit("yyyy-MM-dd HH:mm:ss"))
    return _csv(spark, path).select(
        hour.alias("weather_hour_utc"),
        F.col("temp").cast("double").alias("temp"),
        F.col("prcp").cast("double").alias("prcp"),
        F.col("coco").cast("int").alias("coco"),
    ).where(F.col("weather_hour_utc").isNotNull() & F.col("temp").isNotNull()).dropDuplicates(
        ["weather_hour_utc"]
    )


def _air_hours(spark: SparkSession, path: Path) -> DataFrame:
    """NYC PM2.5 per UTC hour: median per site, then median across sites."""
    rows = _csv(spark, path).where(
        (F.col("State Name") == "New York") & F.col("County Name").isin(*NYC_COUNTIES)
    ).select(
        F.concat_ws("-", "State Code", "County Code", "Site Num").alias("site_id"),
        F.col("Parameter Code").alias("parameter_code"),
        F.col("POC").alias("poc"),
        F.col("Method Code").alias("method_code"),
        F.try_to_timestamp(
            F.concat_ws(" ", "Date GMT", "Time GMT"), F.lit("yyyy-MM-dd HH:mm")
        ).alias("hour_utc"),
        F.col("Sample Measurement").cast("double").alias("value"),
        F.col("Units of Measure").alias("unit"),
    )
    valid = rows.where(
        F.col("hour_utc").isNotNull() & (F.col("value") >= 0) & ~F.isnan("value")
        & (F.col("parameter_code") == "88101") & (F.col("unit") == PM25_UNIT)
    ).dropDuplicates(["site_id", "parameter_code", "poc", "hour_utc", "method_code"])
    sites = valid.groupBy("site_id", "hour_utc").agg(F.median("value").alias("site_pm25"))
    return sites.groupBy(F.col("hour_utc").alias("air_quality_hour_utc")).agg(
        F.median("site_pm25").alias("air_quality_pm25")
    )


def raw_training_dataset(
    spark: SparkSession, data_dir: str | Path, config: Mapping[str, Any],
) -> DataFrame:
    """Prepare the raw files listed in the ML config and build the training dataset."""
    root = Path(data_dir)
    files = config["training_dataset"]["expected_source_files"]
    zones = _zones(spark, root / files["taxi_zones"][0])
    zone_ids = [
        int(row.location_id)
        for row in zones.where(F.col("location_id").isNotNull()).select("location_id").collect()
    ]
    return build_training_dataset(
        spark,
        _trips(spark, [root / name for name in files["taxi"]], zone_ids, config),
        zones,
        _weather(spark, root / files["weather"][0]),
        _air_hours(spark, root / files["air_quality"][0]),
        config,
    )
