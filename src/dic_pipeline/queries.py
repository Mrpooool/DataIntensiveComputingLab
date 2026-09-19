"""Reusable Spark SQL analytical queries for Week 2 role B."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from pyspark.sql import DataFrame, SparkSession

from .contracts import load_dataset_config
from .ingestion import PROJECT_ROOT


DEFAULT_QUERY_CONFIG = PROJECT_ROOT / "configs" / "analytical_queries.json"
DEFAULT_DATASET_CONFIG = PROJECT_ROOT / "configs" / "datasets.json"
SQL_DIRECTORY = Path(__file__).with_name("sql")
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_UTC_HOUR_FORMAT = "%Y-%m-%d %H:%M:%S"


@dataclass(frozen=True)
class QueryDefinition:
    """Stable public metadata for one analytical query."""

    query_id: str
    title: str
    sql_file: str
    required_columns: Mapping[str, tuple[str, ...]]
    result_columns: tuple[str, ...]


QUERY_DEFINITIONS: dict[str, QueryDefinition] = {
    "q1": QueryDefinition(
        "q1",
        "Monthly taxi demand by pickup zone",
        "q1_monthly_demand_by_zone.sql",
        {"integrated": ("pickup_hour_utc", "pickup_location_id", "pickup_zone", "pickup_borough")},
        ("local_pickup_month", "pickup_location_id", "pickup_zone", "pickup_borough", "trip_count"),
    ),
    "q2": QueryDefinition(
        "q2",
        "Average trip distance by weather category",
        "q2_average_distance_by_weather.sql",
        {
            "integrated": (
                "pickup_hour_utc",
                "environment_in_scope",
                "weather_matched",
                "weather_coco",
                "trip_distance",
            )
        },
        ("weather_category", "trip_count", "valid_distance_count", "average_trip_distance"),
    ),
    "q3": QueryDefinition(
        "q3",
        "Air quality and hourly taxi demand",
        "q3_air_quality_and_demand.sql",
        {
            "integrated": ("pickup_hour_utc", "environment_in_scope"),
            "air_quality": (
                "site_id",
                "air_quality_hour_utc",
                "measurement_value",
                "parameter_code",
                "measurement_unit",
                "method_type",
                "method_code",
            )
        },
        (
            "pm25_band",
            "hour_count",
            "total_trip_count",
            "average_hourly_trip_count",
            "average_pm25",
            "overall_pm25_demand_correlation",
        ),
    ),
    "q4": QueryDefinition(
        "q4",
        "Pickup zones with the largest demand variation across weather",
        "q4_weather_demand_variation_by_zone.sql",
        {
            "integrated": (
                "pickup_hour_utc",
                "pickup_location_id",
                "pickup_zone",
                "pickup_borough",
                "environment_in_scope",
            ),
            "weather": ("weather_hour_utc", "coco"),
        },
        (
            "variation_rank",
            "pickup_location_id",
            "pickup_zone",
            "pickup_borough",
            "weather_category_count",
            "weather_hour_count",
            "minimum_average_hourly_demand",
            "maximum_average_hourly_demand",
            "demand_range",
            "demand_stddev",
        ),
    ),
    "q5": QueryDefinition(
        "q5",
        "Peak local travel hours for each day of week",
        "q5_peak_hours_by_weekday.sql",
        {"integrated": ("pickup_hour_utc",)},
        (
            "weekday_number",
            "weekday_name",
            "local_pickup_hour",
            "observed_hour_count",
            "total_trip_count",
            "average_hourly_trip_count",
        ),
    ),
    "q6": QueryDefinition(
        "q6",
        "Monthly taxi demand trend",
        "q6_monthly_demand_trend.sql",
        {"integrated": ("pickup_hour_utc",)},
        (
            "local_pickup_month",
            "trip_count",
            "previous_month_trip_count",
            "month_over_month_change",
            "month_over_month_percent",
        ),
    ),
}


def load_query_config(path: str | Path = DEFAULT_QUERY_CONFIG) -> dict[str, Any]:
    """Load and validate the small, version-controlled analysis contract."""
    config = json.loads(Path(path).read_text(encoding="utf-8"))
    timezone_name = config.get("analysis_timezone")
    if not isinstance(timezone_name, str) or not timezone_name:
        raise ValueError("analysis_timezone must be a non-empty string.")
    source_views = config.get("source_views")
    if not isinstance(source_views, dict) or set(source_views) != {
        "integrated", "weather", "air_quality"
    }:
        raise ValueError("source_views must define integrated, weather and air_quality.")
    for name in source_views.values():
        if not isinstance(name, str) or not _SAFE_IDENTIFIER.fullmatch(name):
            raise ValueError(f"Unsafe Spark SQL view name: {name!r}")

    categories = config.get("weather_categories")
    if not isinstance(categories, list) or not categories:
        raise ValueError("weather_categories must be a non-empty list.")
    seen_codes: set[int] = set()
    seen_labels: set[str] = set()
    for category in categories:
        if not isinstance(category.get("label"), str) or not category.get("codes"):
            raise ValueError("Every weather category needs a label and codes.")
        if category["label"] in seen_labels:
            raise ValueError("Weather category labels must be unique.")
        seen_labels.add(category["label"])
        codes = {int(value) for value in category["codes"]}
        if seen_codes.intersection(codes):
            raise ValueError("Weather condition codes may appear in only one category.")
        seen_codes.update(codes)
    if seen_codes != set(range(1, 28)):
        raise ValueError("Weather categories must cover every Meteostat code from 1 to 27.")

    bins = config.get("pm25_bins")
    if not isinstance(bins, list) or not bins:
        raise ValueError("pm25_bins must be a non-empty list.")
    if float(bins[0].get("minimum", -1)) != 0.0 or bins[-1].get("maximum") is not None:
        raise ValueError("PM2.5 bins must start at zero and end with an open upper bound.")
    previous_maximum: float | None = None
    seen_bin_labels: set[str] = set()
    for index, item in enumerate(bins):
        label = item.get("label")
        if not isinstance(label, str) or not label or label in seen_bin_labels:
            raise ValueError("PM2.5 bin labels must be non-empty and unique.")
        seen_bin_labels.add(label)
        minimum = float(item["minimum"])
        maximum = item.get("maximum")
        if previous_maximum is not None and minimum != previous_maximum:
            raise ValueError("PM2.5 bins must be contiguous and ordered.")
        if maximum is not None and float(maximum) <= minimum:
            raise ValueError("Every finite PM2.5 bin must have maximum > minimum.")
        if maximum is None and index != len(bins) - 1:
            raise ValueError("Only the last PM2.5 bin may have no maximum.")
        previous_maximum = None if maximum is None else float(maximum)
    minimum_hours = config.get("minimum_weather_hours_per_category")
    if not isinstance(minimum_hours, int) or minimum_hours < 1:
        raise ValueError("minimum_weather_hours_per_category must be a positive integer.")
    return config


def load_calendar_coverage(
    config_path: str | Path = DEFAULT_DATASET_CONFIG,
) -> tuple[str, str]:
    """The validated Taxi pickup window [start, end) in UTC.

    Ingestion rejects trips outside this window, so it is the only period in
    which an hour without trips can be read as zero demand. The hourly calendars
    in Q3-Q5 never extend past it, whatever range a caller requests.
    """
    contract = load_dataset_config("taxi", config_path)
    bounds = []
    for key in ("valid_pickup_start_utc", "valid_pickup_end_utc_exclusive"):
        value = datetime.strptime(str(contract[key]), _UTC_HOUR_FORMAT)
        if value.minute or value.second:
            raise ValueError(f"{key} must fall on a whole UTC hour.")
        bounds.append(value.strftime(_UTC_HOUR_FORMAT))
    if bounds[0] >= bounds[1]:
        raise ValueError("valid_pickup_start_utc must be earlier than valid_pickup_end_utc_exclusive.")
    return bounds[0], bounds[1]


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _weather_case(config: Mapping[str, Any], column: str) -> str:
    clauses = []
    for category in config["weather_categories"]:
        codes = ", ".join(str(int(value)) for value in category["codes"])
        clauses.append(
            f"WHEN {column} IN ({codes}) THEN {_sql_string(category['label'])}"
        )
    return "CASE " + " ".join(clauses) + " ELSE 'unknown_code' END"


def integrated_weather_category_sql(config: Mapping[str, Any]) -> str:
    """The single weather label expression shared by Q2 and the weather data product."""
    return (
        "CASE WHEN NOT weather_matched THEN 'unmatched' "
        "WHEN weather_coco IS NULL THEN 'missing_code' "
        f"ELSE {_weather_case(config, 'weather_coco')} END"
    )


def _pm25_case(config: Mapping[str, Any], column: str) -> str:
    clauses = [f"WHEN {column} IS NULL THEN 'missing_pm25'"]
    for item in config["pm25_bins"]:
        minimum = float(item["minimum"])
        maximum = item.get("maximum")
        if maximum is None:
            condition = f"{column} > {minimum}"
        elif minimum == 0:
            condition = f"{column} >= {minimum} AND {column} <= {float(maximum)}"
        else:
            condition = f"{column} > {minimum} AND {column} <= {float(maximum)}"
        clauses.append(f"WHEN {condition} THEN {_sql_string(item['label'])}")
    return "CASE " + " ".join(clauses) + " ELSE 'out_of_range' END"


def _validate_date_range(
    start_date: date | str | None,
    end_date: date | str | None,
) -> tuple[str | None, str | None]:
    start = (
        date.fromisoformat(start_date).isoformat()
        if isinstance(start_date, str)
        else start_date.isoformat() if start_date else None
    )
    end = (
        date.fromisoformat(end_date).isoformat()
        if isinstance(end_date, str)
        else end_date.isoformat() if end_date else None
    )
    if start and end and start >= end:
        raise ValueError("start_date must be earlier than end_date.")
    return start, end


def _trip_filter(timezone_name: str, start_date: str | None, end_date: str | None) -> str:
    local_date = (
        "TO_DATE(FROM_UTC_TIMESTAMP(pickup_hour_utc, "
        f"{_sql_string(timezone_name)}))"
    )
    conditions = []
    if start_date:
        conditions.append(f"{local_date} >= DATE {_sql_string(start_date)}")
    if end_date:
        conditions.append(f"{local_date} < DATE {_sql_string(end_date)}")
    return " AND ".join(conditions) if conditions else "TRUE"


def _calendar_bounds(
    timezone_name: str,
    start_date: str | None,
    end_date: str | None,
    coverage: tuple[str, str],
) -> tuple[str, str]:
    """SQL for the UTC hour calendar: the requested local dates clipped to known coverage."""
    coverage_start, coverage_end = coverage
    first_hour = f"TIMESTAMP '{coverage_start}Z'"
    end_hour_exclusive = f"TIMESTAMP '{coverage_end}Z'"
    zone = _sql_string(timezone_name)
    if start_date:
        first_hour = (
            f"GREATEST(TO_UTC_TIMESTAMP(TIMESTAMP '{start_date} 00:00:00', {zone}), {first_hour})"
        )
    if end_date:
        end_hour_exclusive = (
            f"LEAST(TO_UTC_TIMESTAMP(TIMESTAMP '{end_date} 00:00:00', {zone}), {end_hour_exclusive})"
        )
    return first_hour, end_hour_exclusive


def render_query(
    query_id: str,
    *,
    config_path: str | Path = DEFAULT_QUERY_CONFIG,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    coverage: tuple[str, str] | None = None,
) -> str:
    """Render one checked SQL template with an optional half-open local-date range.

    ``coverage`` defaults to the validated Taxi window in ``configs/datasets.json``;
    tests pass an explicit ``(start_utc, end_utc_exclusive)`` pair.
    """
    if query_id not in QUERY_DEFINITIONS:
        raise KeyError(f"Unknown query {query_id!r}; choose from {sorted(QUERY_DEFINITIONS)}")
    config = load_query_config(config_path)
    start, end = _validate_date_range(start_date, end_date)
    views = config["source_views"]
    timezone_name = config["analysis_timezone"]
    first_hour, end_hour_exclusive = _calendar_bounds(
        timezone_name, start, end, coverage or load_calendar_coverage()
    )
    template = (SQL_DIRECTORY / QUERY_DEFINITIONS[query_id].sql_file).read_text(encoding="utf-8")
    return template.format(
        integrated_view=views["integrated"],
        weather_view=views["weather"],
        air_quality_view=views["air_quality"],
        analysis_timezone=timezone_name,
        trip_filter=_trip_filter(timezone_name, start, end),
        calendar_first_hour=first_hour,
        calendar_end_hour_exclusive=end_hour_exclusive,
        weather_category_integrated=integrated_weather_category_sql(config),
        weather_case_standardized=_weather_case(config, "coco"),
        pm25_case=_pm25_case(config, "air_quality_pm25"),
        minimum_weather_hours=int(config["minimum_weather_hours_per_category"]),
    ).strip()


def validate_query_inputs(
    spark: SparkSession,
    query_id: str,
    config_path: str | Path = DEFAULT_QUERY_CONFIG,
) -> None:
    """Fail early with a useful message when the Week 1 handoff is incompatible."""
    definition = QUERY_DEFINITIONS.get(query_id)
    if definition is None:
        raise KeyError(f"Unknown query {query_id!r}")
    views = load_query_config(config_path)["source_views"]
    for logical_view, required in definition.required_columns.items():
        view_name = views[logical_view]
        try:
            columns = set(spark.table(view_name).columns)
        except Exception as error:
            raise RuntimeError(
                f"Required view {view_name!r} is not registered. "
                "Call register_analytics_inputs first."
            ) from error
        missing = sorted(set(required) - columns)
        if missing:
            raise RuntimeError(f"View {view_name!r} is missing required columns: {missing}")


def run_query(
    spark: SparkSession,
    query_id: str,
    *,
    config_path: str | Path = DEFAULT_QUERY_CONFIG,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    coverage: tuple[str, str] | None = None,
) -> DataFrame:
    """Execute one query against already-registered Week 1 views."""
    validate_query_inputs(spark, query_id, config_path)
    result = spark.sql(
        render_query(
            query_id,
            config_path=config_path,
            start_date=start_date,
            end_date=end_date,
            coverage=coverage,
        )
    )
    expected = list(QUERY_DEFINITIONS[query_id].result_columns)
    if result.columns != expected:
        raise RuntimeError(
            f"Query {query_id!r} returned {result.columns}; expected {expected}."
        )
    return result


def run_queries(
    spark: SparkSession,
    query_ids: Sequence[str] | None = None,
    **kwargs: Any,
) -> dict[str, DataFrame]:
    """Build all requested result DataFrames without collecting them."""
    selected = list(query_ids or QUERY_DEFINITIONS)
    return {query_id: run_query(spark, query_id, **kwargs) for query_id in selected}
