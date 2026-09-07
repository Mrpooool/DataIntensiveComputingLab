from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from typing import Any

from pyspark.sql import DataFrame, functions as F


def snake_case(name: str) -> str:
    """Convert a source column name to stable lower snake_case."""
    value = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name.strip())
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    value = re.sub(r"[^A-Za-z0-9]+", "_", value)
    return re.sub(r"_+", "_", value).strip("_").lower()


def standardize_column_names(df: DataFrame) -> DataFrame:
    renamed = [snake_case(name) for name in df.columns]
    if len(set(renamed)) != len(renamed):
        collisions = sorted({name for name in renamed if renamed.count(name) > 1})
        raise ValueError(f"Column-name normalization creates collisions: {collisions}")
    return df.toDF(*renamed)


def _stable_record_id(columns: list[Any], required: list[Any]) -> Any:
    all_present = F.lit(True)
    for column in required:
        all_present = all_present & column.isNotNull()
    payload = F.concat_ws("||", *[F.coalesce(column.cast("string"), F.lit("<null>")) for column in columns])
    return F.when(all_present, F.sha2(payload, 256))


def transform_taxi(df: DataFrame, config: Mapping[str, Any]) -> DataFrame:
    result = standardize_column_names(df)
    for source, target in {
        "pu_location_id": "pickup_location_id",
        "pulocation_id": "pickup_location_id",
        "do_location_id": "dropoff_location_id",
        "dolocation_id": "dropoff_location_id",
        "tpep_pickup_datetime": "pickup_timestamp_source",
        "tpep_dropoff_datetime": "dropoff_timestamp_source",
    }.items():
        if source in result.columns:
            result = result.withColumnRenamed(source, target)

    source_timezone = str(config.get("source_timezone", "America/New_York"))
    result = (
        result.withColumn(
            "pickup_timestamp_utc",
            F.to_utc_timestamp(F.col("pickup_timestamp_source"), source_timezone),
        )
        .withColumn(
            "dropoff_timestamp_utc",
            F.to_utc_timestamp(F.col("dropoff_timestamp_source"), source_timezone),
        )
        .withColumn("pickup_hour_utc", F.date_trunc("hour", F.col("pickup_timestamp_utc")))
        .withColumn(
            "pickup_date",
            F.to_date(F.from_utc_timestamp(F.col("pickup_timestamp_utc"), source_timezone)),
        )
        .withColumn(
            "trip_duration_seconds",
            F.col("dropoff_timestamp_utc").cast("long")
            - F.col("pickup_timestamp_utc").cast("long"),
        )
    )

    id_parts = [
        F.col("vendor_id"),
        F.date_format(F.col("pickup_timestamp_utc"), "yyyy-MM-dd HH:mm:ss.SSSSSS"),
        F.date_format(F.col("dropoff_timestamp_utc"), "yyyy-MM-dd HH:mm:ss.SSSSSS"),
        F.col("passenger_count"),
        F.format_string("%.6f", F.col("trip_distance")),
        F.col("ratecode_id"),
        F.col("store_and_fwd_flag"),
        F.col("pickup_location_id"),
        F.col("dropoff_location_id"),
        F.col("payment_type"),
        F.format_string("%.6f", F.col("fare_amount")),
        F.format_string("%.6f", F.col("extra")),
        F.format_string("%.6f", F.col("mta_tax")),
        F.format_string("%.6f", F.col("tip_amount")),
        F.format_string("%.6f", F.col("tolls_amount")),
        F.format_string("%.6f", F.col("improvement_surcharge")),
        F.format_string("%.6f", F.col("total_amount")),
        F.format_string("%.6f", F.col("congestion_surcharge")),
        F.format_string("%.6f", F.col("airport_fee")),
    ]
    required = [
        F.col("vendor_id"),
        F.col("pickup_timestamp_utc"),
        F.col("dropoff_timestamp_utc"),
        F.col("pickup_location_id"),
        F.col("dropoff_location_id"),
        F.col("trip_distance"),
        F.col("fare_amount"),
    ]
    return result.withColumn("record_id", _stable_record_id(id_parts, required))


def transform_weather(df: DataFrame, config: Mapping[str, Any]) -> DataFrame:
    result = standardize_column_names(df)
    source_text = F.format_string(
        "%04d-%02d-%02d %02d:00:00",
        F.col("year"),
        F.col("month"),
        F.col("day"),
        F.col("hour"),
    )
    source_timezone = str(config.get("source_timezone", "UTC"))
    result = (
        result.withColumn(
            "weather_timestamp_source",
            F.to_timestamp(source_text, "yyyy-MM-dd HH:mm:ss"),
        )
        .withColumn(
            "weather_hour_utc",
            F.to_utc_timestamp(F.col("weather_timestamp_source"), source_timezone),
        )
        .withColumn(
            "record_id",
            F.when(
                F.col("weather_hour_utc").isNotNull(),
                F.sha2(
                    F.date_format(F.col("weather_hour_utc"), "yyyy-MM-dd HH:mm:ss"),
                    256,
                ),
            ),
        )
    )
    return result


def transform_air_quality(df: DataFrame, config: Mapping[str, Any]) -> DataFrame:
    result = standardize_column_names(df)
    result = (
        result.withColumnRenamed("sample_measurement", "measurement_value")
        .withColumnRenamed("units_of_measure", "measurement_unit")
        .withColumn(
            "air_quality_hour_utc",
            F.to_timestamp(
                F.concat_ws(" ", F.col("date_gmt"), F.col("time_gmt")),
                "yyyy-MM-dd HH:mm",
            ),
        )
        .withColumn(
            "local_standard_timestamp_source",
            F.to_timestamp(
                F.concat_ws(" ", F.col("date_local"), F.col("time_local")),
                "yyyy-MM-dd HH:mm",
            ),
        )
        .withColumn("state_code", F.lpad(F.col("state_code").cast("string"), 2, "0"))
        .withColumn("county_code", F.lpad(F.col("county_code").cast("string"), 3, "0"))
        .withColumn("site_num", F.lpad(F.col("site_num").cast("string"), 4, "0"))
        .withColumn("parameter_code", F.col("parameter_code").cast("string"))
        .withColumn("method_code", F.col("method_code").cast("string"))
        .withColumn(
            "qualifier",
            F.when(F.trim(F.col("qualifier")) == "", F.lit(None)).otherwise(
                F.trim(F.col("qualifier"))
            ),
        )
        .withColumn(
            "site_id",
            F.concat_ws("-", F.col("state_code"), F.col("county_code"), F.col("site_num")),
        )
        .withColumn(
            "monitor_id",
            F.concat_ws(
                "-",
                F.col("state_code"),
                F.col("county_code"),
                F.col("site_num"),
                F.col("parameter_code"),
                F.col("poc").cast("string"),
            ),
        )
    )

    id_parts = [
        F.col("state_code"),
        F.col("county_code"),
        F.col("site_num"),
        F.col("parameter_code"),
        F.col("poc"),
        F.date_format(F.col("air_quality_hour_utc"), "yyyy-MM-dd HH:mm:ss"),
        F.col("method_code"),
    ]
    required = [
        F.col("state_code"),
        F.col("county_code"),
        F.col("site_num"),
        F.col("parameter_code"),
        F.col("poc"),
        F.col("air_quality_hour_utc"),
        F.col("method_code"),
    ]
    return result.withColumn("record_id", _stable_record_id(id_parts, required))


def transform_taxi_zones(df: DataFrame, config: Mapping[str, Any]) -> DataFrame:
    del config
    result = standardize_column_names(df)
    if "location_id" not in result.columns and "locationid" in result.columns:
        result = result.withColumnRenamed("locationid", "location_id")

    result = (
        result.withColumn(
            "borough",
            F.when(F.col("location_id") == 265, F.lit("Outside of NYC"))
            .when(F.col("location_id") == 264, F.lit("Unknown"))
            .otherwise(F.trim(F.col("borough"))),
        )
        .withColumn(
            "zone",
            F.when(F.col("location_id") == 265, F.lit("Outside of NYC"))
            .when(F.col("location_id") == 264, F.lit("Unknown"))
            .otherwise(F.trim(F.col("zone"))),
        )
        .withColumn(
            "service_zone",
            F.when(F.col("location_id") == 265, F.lit("Outside of NYC"))
            .when(F.col("location_id") == 264, F.lit("Unknown"))
            .otherwise(F.trim(F.col("service_zone"))),
        )
        .withColumn(
            "record_id",
            F.when(
                F.col("location_id").isNotNull(),
                F.sha2(F.col("location_id").cast("string"), 256),
            ),
        )
    )
    return result


TRANSFORMERS: dict[str, Callable[[DataFrame, Mapping[str, Any]], DataFrame]] = {
    "taxi": transform_taxi,
    "weather": transform_weather,
    "air_quality": transform_air_quality,
    "taxi_zones": transform_taxi_zones,
}


def transform_dataset(
    df: DataFrame, dataset: str, config: Mapping[str, Any]
) -> DataFrame:
    try:
        transformer = TRANSFORMERS[dataset]
    except KeyError as error:
        raise KeyError(f"No transformer registered for dataset {dataset!r}") from error
    return transformer(df, config)
