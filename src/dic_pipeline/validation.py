from __future__ import annotations

from collections.abc import Mapping, Sequence
from functools import reduce
from operator import and_, or_
from typing import Any

from pyspark.sql import Column, DataFrame, Window, functions as F


Rule = tuple[str, Column]


def _any(conditions: Sequence[Column]) -> Column:
    return reduce(or_, conditions, F.lit(False))


def _all(conditions: Sequence[Column]) -> Column:
    return reduce(and_, conditions, F.lit(True))


def _append_codes(df: DataFrame, output_column: str, rules: Sequence[Rule]) -> DataFrame:
    result = df.withColumn(output_column, F.array().cast("array<string>"))
    for code, condition in rules:
        result = result.withColumn(
            output_column,
            F.when(
                F.coalesce(condition.cast("boolean"), F.lit(False)),
                F.array_union(F.col(output_column), F.array(F.lit(code))),
            ).otherwise(F.col(output_column)),
        )
    return result


def _taxi_rules(df: DataFrame, config: Mapping[str, Any]) -> tuple[list[Rule], list[Rule]]:
    key_columns = [
        "vendor_id",
        "pickup_timestamp_utc",
        "dropoff_timestamp_utc",
        "pickup_location_id",
        "dropoff_location_id",
        "trip_distance",
        "fare_amount",
    ]
    numeric_columns = [
        "trip_distance",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
    ]
    invalid_numeric = _any([F.isnan(F.col(name)) for name in numeric_columns]) | (
        F.col("trip_distance") < 0
    ) | (F.col("passenger_count") < 0)
    invalid_location = (~F.col("pickup_location_id").between(1, 265)) | (
        ~F.col("dropoff_location_id").between(1, 265)
    )
    start = F.to_timestamp(F.lit(str(config["valid_pickup_start_utc"])))
    end = F.to_timestamp(F.lit(str(config["valid_pickup_end_utc_exclusive"])))

    errors: list[Rule] = [
        ("missing_primary_key_component", _any([F.col(name).isNull() for name in key_columns])),
        (
            "invalid_timestamp",
            F.col("pickup_timestamp_utc").isNull()
            | F.col("dropoff_timestamp_utc").isNull(),
        ),
        (
            "timestamp_outside_source_period",
            (F.col("pickup_timestamp_utc") < start) | (F.col("pickup_timestamp_utc") >= end),
        ),
        (
            "dropoff_before_pickup",
            F.col("dropoff_timestamp_utc") < F.col("pickup_timestamp_utc"),
        ),
        ("invalid_numeric_value", invalid_numeric),
        ("invalid_location_id", invalid_location),
    ]
    flags: list[Rule] = [
        ("zero_trip_distance", F.col("trip_distance") == 0),
        ("zero_trip_duration", F.col("trip_duration_seconds") == 0),
        ("trip_duration_over_24h", F.col("trip_duration_seconds") > 86_400),
        ("trip_distance_over_100", F.col("trip_distance") > 100),
        ("nonpositive_passenger_count", F.col("passenger_count") <= 0),
        ("negative_fare_amount", F.col("fare_amount") < 0),
        ("negative_total_amount", F.col("total_amount") < 0),
    ]
    return errors, flags


def _weather_rules(df: DataFrame, config: Mapping[str, Any]) -> tuple[list[Rule], list[Rule]]:
    del config
    invalid_numeric = _any(
        [
            ~F.col("rhum").between(0, 100),
            F.col("prcp") < 0,
            F.col("snwd") < 0,
            ~F.col("wdir").between(0, 360),
            F.col("wspd") < 0,
            F.col("wpgt") < 0,
            F.col("pres") <= 0,
            ~F.col("cldc").between(0, 8),
            F.col("coco") < 0,
        ]
    )
    errors: list[Rule] = [
        ("missing_primary_key_component", F.col("weather_hour_utc").isNull()),
        ("invalid_timestamp", F.col("weather_hour_utc").isNull()),
        ("missing_required_value", F.col("temp").isNull() | F.col("rhum").isNull()),
        ("invalid_numeric_value", invalid_numeric),
    ]
    flags: list[Rule] = [
        ("missing_precipitation", F.col("prcp").isNull()),
        ("missing_weather_code", F.col("coco").isNull()),
    ]
    return errors, flags


def _air_quality_rules(
    df: DataFrame, config: Mapping[str, Any]
) -> tuple[list[Rule], list[Rule]]:
    del config
    key_columns = [
        "state_code",
        "county_code",
        "site_num",
        "parameter_code",
        "poc",
        "air_quality_hour_utc",
        "method_code",
    ]
    errors: list[Rule] = [
        ("missing_primary_key_component", _any([F.col(name).isNull() for name in key_columns])),
        ("invalid_timestamp", F.col("air_quality_hour_utc").isNull()),
        ("missing_required_value", F.col("measurement_value").isNull()),
        (
            "invalid_numeric_value",
            F.isnan(F.col("measurement_value")) | (F.col("measurement_value") < 0),
        ),
        (
            "unexpected_parameter_or_unit",
            (F.col("parameter_code") != "88101")
            | (F.col("measurement_unit") != "Micrograms/cubic meter (LC)"),
        ),
    ]
    flags: list[Rule] = [
        ("qualified_measurement", F.col("qualifier").isNotNull()),
    ]
    return errors, flags


def _taxi_zone_rules(
    df: DataFrame, config: Mapping[str, Any]
) -> tuple[list[Rule], list[Rule]]:
    del config
    errors: list[Rule] = [
        ("missing_primary_key_component", F.col("location_id").isNull()),
        ("invalid_location_id", ~F.col("location_id").between(1, 265)),
        (
            "missing_required_value",
            F.col("borough").isNull()
            | F.col("zone").isNull()
            | F.col("service_zone").isNull(),
        ),
    ]
    return errors, []


RULE_BUILDERS = {
    "taxi": _taxi_rules,
    "weather": _weather_rules,
    "air_quality": _air_quality_rules,
    "taxi_zones": _taxi_zone_rules,
}


def validate_dataset(
    df: DataFrame, dataset: str, config: Mapping[str, Any]
) -> DataFrame:
    """Attach row-level error reasons and non-rejecting quality flags."""
    try:
        errors, flags = RULE_BUILDERS[dataset](df, config)
    except KeyError as error:
        raise KeyError(f"No validation rules registered for dataset {dataset!r}") from error
    result = _append_codes(df, "error_reasons", errors)
    return _append_codes(result, "quality_flags", flags)


def mark_duplicate_rows(df: DataFrame, key_columns: Sequence[str]) -> DataFrame:
    """Keep one deterministic representative and reject later rows for the same key."""
    missing = sorted(set(key_columns) - set(df.columns))
    if missing:
        raise ValueError(f"Duplicate-key columns are missing after transformation: {missing}")

    valid_key = _all([F.col(name).isNotNull() for name in key_columns])
    order_columns = [
        F.coalesce(F.col("source_file"), F.lit("")),
        F.coalesce(F.col("raw_record_json"), F.lit("")),
    ]
    window = Window.partitionBy(*[F.col(name) for name in key_columns]).orderBy(*order_columns)
    result = df.withColumn("_duplicate_rank", F.row_number().over(window))
    result = result.withColumn(
        "error_reasons",
        F.when(
            valid_key & (F.col("_duplicate_rank") > 1),
            F.array_union(F.col("error_reasons"), F.array(F.lit("duplicate_record"))),
        ).otherwise(F.col("error_reasons")),
    )
    return result.drop("_duplicate_rank")
