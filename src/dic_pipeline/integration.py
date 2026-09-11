"""Enrichment of standardized Taxi trips."""

from pyspark.sql import DataFrame, functions as F


NYC_BOROUGHS = ("Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island")
WEATHER_METRICS = (
    "temp", "rhum", "prcp", "snwd", "wdir", "wspd", "wpgt", "pres", "cldc", "coco",
)
# Verified against the delivered NYC observations; review before adding other methods.
AIR_SIGNATURE = ("88101", "Micrograms/cubic meter (LC)", "FEM", "636")


def _require_unique_key(frame: DataFrame, key: str) -> None:
    invalid = frame.groupBy(key).count().where(F.col(key).isNull() | (F.col("count") > 1))
    if invalid.limit(1).count():
        raise ValueError(f"{key} must be non-null and unique before joining.")


def add_taxi_zones(taxi: DataFrame, zones: DataFrame) -> DataFrame:
    """Add pickup/dropoff zone names and boroughs, preserving every Taxi row."""
    _require_unique_key(zones, "location_id")

    pickup_zones = zones.select(
        F.col("location_id").alias("pickup_location_id"),
        F.col("zone").alias("pickup_zone"),
        F.col("borough").alias("pickup_borough"),
    )
    dropoff_zones = zones.select(
        F.col("location_id").alias("dropoff_location_id"),
        F.col("zone").alias("dropoff_zone"),
        F.col("borough").alias("dropoff_borough"),
    )
    # The 265-row lookup is small enough to broadcast to each Spark task.
    return (
        taxi.join(F.broadcast(pickup_zones), "pickup_location_id", "left")
        .join(F.broadcast(dropoff_zones), "dropoff_location_id", "left")
        .select(*taxi.columns, "pickup_zone", "pickup_borough", "dropoff_zone", "dropoff_borough")
    )


def aggregate_air_quality(air: DataFrame) -> DataFrame:
    """Compute PM2.5 medians with equal weight per available site, not per instrument."""
    signatures = air.select(
        "parameter_code", "measurement_unit", "method_type", "method_code",
    ).distinct().limit(2).collect()
    if signatures and [tuple(row) for row in signatures] != [AIR_SIGNATURE]:
        raise ValueError("Air aggregation requires PM2.5 LC, Micrograms/cubic meter (LC), FEM method 636.")

    site_hours = air.groupBy("site_id", "air_quality_hour_utc").agg(
        F.median("measurement_value").alias("site_pm25"),
        F.count(F.when(F.col("qualifier").isNotNull(), 1)).alias("flagged_count"),
    )
    return site_hours.groupBy("air_quality_hour_utc").agg(
        F.median("site_pm25").alias("air_quality_pm25"),
        F.count("*").alias("air_quality_site_count"),
        F.sum("flagged_count").alias("air_quality_flagged_observation_count"),
    )


def add_environment(taxi_with_zones: DataFrame, weather: DataFrame, air: DataFrame) -> DataFrame:
    """Left-join same-hour NYC background observations; leave missing values unfilled."""
    _require_unique_key(weather, "weather_hour_utc")
    weather_fields = [name for metric in WEATHER_METRICS for name in (metric, f"{metric}_source")]
    weather_hours = weather.select(
        "weather_hour_utc", "weather_timestamp_source",
        *[F.col(name).alias(f"weather_{name}") for name in weather_fields],
        F.col("quality_flags").alias("weather_quality_flags"),
    )
    air_hours = aggregate_air_quality(air)
    trips = taxi_with_zones.withColumn(
        "environment_in_scope",
        F.coalesce(F.col("pickup_borough").isin(*NYC_BOROUGHS), F.lit(False)),
    )
    return (
        trips.join(
            F.broadcast(weather_hours),
            (trips.pickup_hour_utc == weather_hours.weather_hour_utc) & trips.environment_in_scope,
            "left",
        )
        .join(
            F.broadcast(air_hours),
            (F.col("pickup_hour_utc") == air_hours.air_quality_hour_utc) & F.col("environment_in_scope"),
            "left",
        )
        .withColumn("weather_matched", F.col("weather_hour_utc").isNotNull())
        .withColumn("air_quality_matched", F.col("air_quality_hour_utc").isNotNull())
    )


def integration_metrics(taxi: DataFrame, integrated: DataFrame) -> dict:
    """Verify one output per trip and distinguish hour coverage from metric availability."""
    conditions = {
        "nyc_trip_count": F.col("environment_in_scope"),
        "weather_match_count": F.col("weather_matched"),
        "air_quality_match_count": F.col("air_quality_matched"),
        "air_quality_pm25_nonnull_count": F.col("air_quality_pm25").isNotNull(),
    }
    conditions.update({
        f"weather_{name}_nonnull_count": F.col(f"weather_{name}").isNotNull()
        for name in WEATHER_METRICS
    })
    stats = integrated.agg(
        F.count("*").alias("output_count"),
        F.count_distinct("record_id").alias("unique_record_count"),
        *[F.count(F.when(condition, 1)).alias(name) for name, condition in conditions.items()],
    ).first().asDict()
    stats["input_count"] = taxi.count()
    if stats["output_count"] != stats["input_count"] or stats["unique_record_count"] != stats["output_count"]:
        raise ValueError("Integration must preserve the Taxi count and non-null unique record_id.")
    for source in ("weather", "air_quality"):
        matched = stats[f"{source}_match_count"]
        for scope, denominator in (("overall", stats["input_count"]), ("nyc", stats["nyc_trip_count"])):
            stats[f"{source}_match_rate_{scope}"] = matched / denominator if denominator else None
    return stats


def integrate(
    taxi: DataFrame, weather: DataFrame, air: DataFrame, zones: DataFrame,
) -> tuple[DataFrame, dict]:
    """Enrich accepted trips and return the verified DataFrame plus coverage metrics."""
    with_zones = add_taxi_zones(taxi, zones)
    integrated = add_environment(with_zones, weather, air)
    return integrated, integration_metrics(taxi, integrated)
