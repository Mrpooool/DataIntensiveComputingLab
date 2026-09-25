from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from functools import reduce
from operator import and_, or_
from typing import Any

from pyspark.sql import Column, DataFrame, Window, functions as F
from pyspark.sql.types import StructField, StructType


Rule = tuple[str, Column]
RuleBuilder = Callable[
    [DataFrame, Mapping[str, Any], Mapping[str, Any]],
    tuple[list[Rule], list[Rule]],
]


def _type_name(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "simpleString"):
        value = value.simpleString()
    normalized = str(value).strip().lower()
    aliases = {
        "integer": "int",
        "bigint": "long",
        "float": "float",
        "float64": "double",
        "boolean": "boolean",
    }
    return aliases.get(normalized, normalized)


def _schema_fields(
    schema: Sequence[str | StructField] | StructType,
) -> list[dict[str, Any]]:
    items: Sequence[str | StructField]
    items = schema.fields if isinstance(schema, StructType) else schema
    fields: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, StructField):
            fields.append(
                {
                    "name": item.name,
                    "type": _type_name(item.dataType),
                    "nullable": bool(item.nullable),
                }
            )
        else:
            fields.append({"name": str(item), "type": None, "nullable": None})
    return fields


def _allowed_additions(policy: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    configured = policy.get("allow_add", {})
    if isinstance(configured, Mapping):
        allowed = {}
        for name, value in configured.items():
            if isinstance(value, Mapping):
                spec = dict(value)
            elif value is None:
                spec = {}
            else:
                spec = {"type": value}
            if "type" in spec:
                spec["type"] = _type_name(spec["type"])
            allowed[str(name)] = spec
        return allowed
    return {str(name): {} for name in configured}


def check_schema(
    actual_columns: Sequence[str | StructField] | StructType,
    expected_schema: Sequence[str | StructField] | StructType,
    policy: Mapping[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return accepted and unsupported changes against a source-schema contract.

    Additive evolution is opt-in through ``policy['allow_add']``. Column removal,
    unlisted additions, type changes, duplicate names, and relaxation of a required
    non-null field are unsupported. A sequence of names remains supported for CSV
    header checks; passing ``StructType`` values additionally checks types/nullability.
    """
    policy = dict(policy or {})
    allow_add = _allowed_additions(policy)
    actual = _schema_fields(actual_columns)
    expected = _schema_fields(expected_schema)
    actual_names = [field["name"] for field in actual]
    expected_names = [field["name"] for field in expected]
    actual_by_name = {field["name"]: field for field in actual}
    expected_by_name = {field["name"]: field for field in expected}
    accepted: list[dict[str, Any]] = []
    unsupported: list[dict[str, Any]] = []

    for name in sorted({name for name in actual_names if actual_names.count(name) > 1}):
        unsupported.append({"op": "duplicate", "column": name})
    for name in sorted(set(expected_names) - set(actual_names)):
        unsupported.append({"op": "remove", "column": name})
    for name in [name for name in actual_names if name not in expected_by_name]:
        observed = actual_by_name[name]
        spec = allow_add.get(name)
        change = {
            "op": "add",
            "column": name,
            "type": observed["type"] or (spec or {}).get("type") or "unknown",
            "nullable": (
                observed["nullable"]
                if observed["nullable"] is not None
                else bool((spec or {}).get("nullable", True))
            ),
        }
        if spec is None:
            unsupported.append(change)
            continue
        expected_type = _type_name(spec.get("type"))
        if expected_type and observed["type"] and observed["type"] != expected_type:
            unsupported.append(
                {
                    **change,
                    "op": "change_type",
                    "expected_type": expected_type,
                }
            )
            continue
        if spec.get("nullable") is False and observed["nullable"] is True:
            unsupported.append({**change, "op": "relax_nullability", "nullable": True})
            continue
        accepted.append(change)

    for name in [name for name in expected_names if name in actual_by_name]:
        observed = actual_by_name[name]
        contract = expected_by_name[name]
        if observed["type"] and contract["type"] and observed["type"] != contract["type"]:
            unsupported.append(
                {
                    "op": "change_type",
                    "column": name,
                    "from": contract["type"],
                    "to": observed["type"],
                }
            )
        if contract["nullable"] is False and observed["nullable"] is True:
            unsupported.append(
                {"op": "relax_nullability", "column": name, "nullable": True}
            )
    return accepted, unsupported


def _any(conditions: Sequence[Column]) -> Column:
    return reduce(or_, conditions, F.lit(False))


def _all(conditions: Sequence[Column]) -> Column:
    return reduce(and_, conditions, F.lit(True))


def _nonfinite(column: Column) -> Column:
    """Reject NaN and either infinity while preserving optional nulls."""
    return F.isnan(column) | column.isin(float("inf"), float("-inf"))


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


def _taxi_rules(
    df: DataFrame,
    config: Mapping[str, Any],
    references: Mapping[str, Any],
) -> tuple[list[Rule], list[Rule]]:
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
    invalid_numeric = _any([_nonfinite(F.col(name)) for name in numeric_columns]) | (
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
    if "taxi_zone_ids" in references:
        zone_ids = tuple(int(value) for value in references["taxi_zone_ids"])
        pickup_missing = F.col("pickup_location_id").isNotNull() & (
            ~F.col("pickup_location_id").isin(*zone_ids)
        )
        dropoff_missing = F.col("dropoff_location_id").isNotNull() & (
            ~F.col("dropoff_location_id").isin(*zone_ids)
        )
        errors.append(("missing_reference_record", pickup_missing | dropoff_missing))
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


def _weather_rules(
    df: DataFrame,
    config: Mapping[str, Any],
    references: Mapping[str, Any],
) -> tuple[list[Rule], list[Rule]]:
    del config
    del references
    numeric_columns = (
        "temp", "rhum", "prcp", "snwd", "wdir", "wspd", "wpgt", "pres", "cldc", "coco",
    )
    invalid_numeric = _any([_nonfinite(F.col(name)) for name in numeric_columns]) | _any(
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
    if "humidity" in df.columns:
        errors.extend(
            [
                ("incomplete_record", F.col("humidity").isNull()),
                (
                    "invalid_attribute_value",
                    _nonfinite(F.col("humidity")) | ~F.col("humidity").between(0, 100),
                ),
            ]
        )
    flags: list[Rule] = [
        ("missing_precipitation", F.col("prcp").isNull()),
        ("missing_weather_code", F.col("coco").isNull()),
    ]
    return errors, flags


def _air_quality_rules(
    df: DataFrame,
    config: Mapping[str, Any],
    references: Mapping[str, Any],
) -> tuple[list[Rule], list[Rule]]:
    del config
    del references
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
            _nonfinite(F.col("measurement_value")) | (F.col("measurement_value") < 0),
        ),
        (
            "unexpected_parameter_or_unit",
            (F.col("parameter_code") != "88101")
            | F.col("measurement_unit").isNull()
            | (F.col("measurement_unit") != "Micrograms/cubic meter (LC)"),
        ),
    ]
    if "aqi" in df.columns:
        errors.extend(
            [
                ("incomplete_record", F.col("aqi").isNull()),
                (
                    "invalid_attribute_value",
                    _nonfinite(F.col("aqi")) | ~F.col("aqi").between(0, 500),
                ),
            ]
        )
    flags: list[Rule] = [
        ("qualified_measurement", F.col("qualifier").isNotNull()),
    ]
    return errors, flags


def _taxi_zone_rules(
    df: DataFrame,
    config: Mapping[str, Any],
    references: Mapping[str, Any],
) -> tuple[list[Rule], list[Rule]]:
    del config
    del references
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


RULE_BUILDERS: dict[str, RuleBuilder] = {
    "taxi": _taxi_rules,
    "weather": _weather_rules,
    "air_quality": _air_quality_rules,
    "taxi_zones": _taxi_zone_rules,
}

_EXTENSION_RULE_BUILDERS: dict[str, list[RuleBuilder]] = defaultdict(list)


def register_rule_builder(dataset: str, builder: RuleBuilder) -> None:
    """Register an additional rule builder without changing validation core code.

    Use ``dataset='*'`` for a generic rule applied to every dataset. Registration is
    idempotent for the same function object, which keeps notebooks and tests predictable.
    """
    builders = _EXTENSION_RULE_BUILDERS[str(dataset)]
    if builder not in builders:
        builders.append(builder)


def unregister_rule_builder(dataset: str, builder: RuleBuilder) -> None:
    """Remove a previously registered extension rule builder."""
    builders = _EXTENSION_RULE_BUILDERS.get(str(dataset), [])
    if builder in builders:
        builders.remove(builder)


def initialize_validation_columns(df: DataFrame) -> DataFrame:
    """Attach empty validation arrays for controlled no-validation measurements."""
    empty = F.array().cast("array<string>")
    return df.withColumn("error_reasons", empty).withColumn("quality_flags", empty)


def validate_dataset(
    df: DataFrame,
    dataset: str,
    config: Mapping[str, Any],
    *,
    reference_data: Mapping[str, Any] | None = None,
) -> DataFrame:
    """Attach row-level error reasons and non-rejecting quality flags."""
    references = dict(reference_data or {})
    try:
        errors, flags = RULE_BUILDERS[dataset](df, config, references)
    except KeyError as error:
        raise KeyError(f"No validation rules registered for dataset {dataset!r}") from error
    for builder in (
        *_EXTENSION_RULE_BUILDERS.get("*", ()),
        *_EXTENSION_RULE_BUILDERS.get(dataset, ()),
    ):
        extra_errors, extra_flags = builder(df, config, references)
        errors.extend(extra_errors)
        flags.extend(extra_flags)
    result = _append_codes(df, "error_reasons", errors)
    return _append_codes(result, "quality_flags", flags)


def mark_duplicate_rows(df: DataFrame, key_columns: Sequence[str]) -> DataFrame:
    """Prefer a valid row, then break ties deterministically for each key."""
    missing = sorted(set(key_columns) - set(df.columns))
    if missing:
        raise ValueError(f"Duplicate-key columns are missing after transformation: {missing}")

    valid_key = _all([F.col(name).isNotNull() for name in key_columns])
    order_columns = [
        (F.size("error_reasons") > 0).cast("int"),
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
