"""Week 2 query optimization experiments: paired variants, alternated order, checked results.

Every experiment compares a baseline SQL with one optimized rewrite or setting.
Both variants are warmed once, then measured ``repeats`` times in alternating
order with ``collect()``; the optimized answer must equal the baseline answer
before any timing is reported. Executed plans, SQL text and Spark settings are
written next to the results so the benchmark report can cite them.

Cache experiments are the one exception to alternation: Spark substitutes a
cached plan into every later query that contains the same logical subplan, so
the baseline is measured before the cache exists and the optimized variant
after it is built.
"""

from __future__ import annotations

import re
from contextlib import contextmanager, redirect_stdout
from dataclasses import dataclass, field
from io import StringIO
from pathlib import Path
from statistics import median
from time import perf_counter
from typing import Any, Iterator, Mapping

from pyspark.sql import DataFrame, SparkSession, functions as F

from .benchmark import assert_same_results
from .data_products import DEFAULT_CONFIG_PATH as PRODUCT_CONFIG_PATH
from .data_products import load_product_config, product_table_stats
from .queries import (
    DEFAULT_QUERY_CONFIG,
    QUERY_DEFINITIONS,
    SQL_DIRECTORY,
    load_query_config,
    render_query,
    render_template,
)


PRODUCT_SQL_DIRECTORY = SQL_DIRECTORY / "products"
# Which product answers which query; the templates live in sql/products/.
PRODUCT_QUERIES = {
    "q1": "taxi_zone_statistics",
    "q2": "weather_impact_summary",
    "q3": "air_quality_impact_summary",
    "q4": "weather_impact_summary",
    "q5": "daily_mobility_summary",
    "q6": "daily_mobility_summary",
}
# Broadcast experiments must not let Spark broadcast on its own or let AQE convert the join.
NO_AUTO_BROADCAST = {
    "spark.sql.autoBroadcastJoinThreshold": "-1",
    "spark.sql.adaptive.enabled": "false",
}
BASE_TABLE_Q1 = """SELECT {hint}
    DATE_FORMAT(FROM_UTC_TIMESTAMP(t.pickup_hour_utc, '{analysis_timezone}'), 'yyyy-MM') AS local_pickup_month,
    t.pickup_location_id,
    COALESCE(MAX(z.zone), 'Unknown') AS pickup_zone,
    COALESCE(MAX(z.borough), 'Unknown') AS pickup_borough,
    COUNT(*) AS trip_count
FROM {taxi_view} AS t
LEFT JOIN {zones_view} AS z
  ON t.pickup_location_id = z.location_id
WHERE {trip_filter}
GROUP BY 1, 2
ORDER BY local_pickup_month, trip_count DESC, pickup_location_id"""


@dataclass(frozen=True)
class Variant:
    """One side of an experiment: SQL text plus the settings it runs under."""

    name: str
    sql: str
    conf: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Experiment:
    name: str
    query_id: str
    technique: str
    rationale: str
    baseline: Variant
    optimized: Variant
    date_range: tuple[str | None, str | None] = (None, None)
    # Projection that both variants read through a temp view; only the optimized view is cached.
    cache_source_sql: str | None = None
    # Canonical SQL both variants must also match (used when neither variant is the canonical query).
    reference_sql: str | None = None

    @property
    def variants(self) -> tuple[Variant, Variant]:
        return (self.baseline, self.optimized)


def cache_view_names(experiment_name: str) -> tuple[str, str]:
    return f"{experiment_name}_uncached", f"{experiment_name}_cached"


@contextmanager
def spark_conf(spark: SparkSession, overrides: Mapping[str, str]) -> Iterator[None]:
    """Apply session settings for one variant and restore them afterwards."""
    previous = {key: spark.conf.get(key, None) for key in overrides}
    for key, value in overrides.items():
        spark.conf.set(key, value)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)


def explain_formatted(frame: DataFrame) -> str:
    """EXPLAIN FORMATTED of a DataFrame; after an action this shows the final adaptive plan."""
    buffer = StringIO()
    with redirect_stdout(buffer):
        frame.explain(mode="formatted")
    return buffer.getvalue()


def plan_indicators(plan: str) -> dict[str, Any]:
    """Facts the report cites from a plan, extracted rather than eyeballed."""
    partition_filters = [
        match.group(1).strip()
        for match in re.finditer(r"PartitionFilters: \[(.*?)\]", plan)
        if match.group(1).strip()
    ]
    return {
        "broadcast_hash_join": "BroadcastHashJoin" in plan,
        "sort_merge_join": "SortMergeJoin" in plan,
        "in_memory_table_scan": "InMemoryTableScan" in plan,
        "adaptive_final_plan": "isFinalPlan=true" in plan,
        "aqe_shuffle_reads": plan.count("AQEShuffleRead"),
        "exchanges": plan.count("Exchange"),
        "partition_filters": partition_filters,
    }


def cache_storage(spark: SparkSession) -> dict[str, int]:
    """Bytes held by cached RDDs right now (the memory cost of a cache experiment)."""
    infos = spark.sparkContext._jsc.sc().getRDDStorageInfo()
    return {
        "cached_partitions": int(sum(info.numCachedPartitions() for info in infos)),
        "memory_bytes": int(sum(info.memSize() for info in infos)),
        "disk_bytes": int(sum(info.diskSize() for info in infos)),
    }


def register_products(
    spark: SparkSession,
    delta_root: str | Path,
    *,
    output_root: str | Path | None = None,
    config_path: str | Path = PRODUCT_CONFIG_PATH,
) -> dict[str, dict[str, Any]]:
    """Register existing product tables as ``product_<name>`` views and record their storage."""
    root = Path(output_root) if output_root is not None else Path(delta_root) / "analytics"
    products: dict[str, dict[str, Any]] = {}
    for name in load_product_config(config_path)["products"]:
        path = root / name
        if not (path / "_delta_log").exists():
            continue
        frame = spark.read.format("delta").load(str(path))
        view = f"product_{name}"
        frame.createOrReplaceTempView(view)
        info: dict[str, Any] = {"view": view, "path": str(path), "row_count": frame.count()}
        info.update(product_table_stats(spark, path))
        products[name] = info
    audit_path = root / "metadata" / "product_refresh_runs"
    if products and (audit_path / "_delta_log").exists():
        latest = (
            spark.read.format("delta")
            .load(str(audit_path))
            .where(F.col("status") == "success")
            .groupBy("product_name")
            .agg(F.max_by("execution_seconds", "finished_at").alias("refresh_seconds"))
            .collect()
        )
        for row in latest:
            if row.product_name in products:
                products[row.product_name]["last_refresh_seconds"] = float(row.refresh_seconds)
    return products


def build_experiments(
    *,
    start_date: str = "2024-02-01",
    end_date: str = "2024-03-01",
    config_path: str | Path = DEFAULT_QUERY_CONFIG,
    products: Mapping[str, Mapping[str, Any]] | None = None,
) -> list[Experiment]:
    """The Week 2 experiment set. Range experiments use ``[start_date, end_date)``."""
    config = load_query_config(config_path)
    views = config["source_views"]
    timezone_name = config["analysis_timezone"]
    ranged = {"config_path": config_path, "start_date": start_date, "end_date": end_date}
    experiments: list[Experiment] = []

    for query_id in ("q1", "q6"):
        experiments.append(
            Experiment(
                name=f"{query_id}_partition_pruning",
                query_id=query_id,
                technique="partition_pruning",
                rationale=(
                    "The local-date filter is a function of pickup_hour_utc, which Delta cannot "
                    "prune on. Adding the equivalent bounds on the pickup_date partition column "
                    "lets the scan skip partitions outside the range."
                ),
                baseline=Variant("baseline", render_query(query_id, **ranged)),
                optimized=Variant(
                    "optimized", render_query(query_id, pickup_date_filter=True, **ranged)
                ),
                date_range=(start_date, end_date),
            )
        )

    for query_id, projection, rationale in (
        (
            "q4",
            "SELECT pickup_hour_utc, pickup_location_id, pickup_zone, pickup_borough, "
            f"environment_in_scope FROM {views['integrated']}",
            "Q4 reads the filtered trips twice (zone list and hourly demand); caching the "
            "five-column projection serves both reads from memory.",
        ),
        (
            "q6",
            f"SELECT pickup_hour_utc FROM {views['integrated']}",
            "Q6 scans one column once, so a cache can only pay off across repeated runs; "
            "this measures the cost of caching a single-column projection.",
        ),
    ):
        name = f"{query_id}_cache_projection"
        uncached, cached = cache_view_names(name)
        experiments.append(
            Experiment(
                name=name,
                query_id=query_id,
                technique="cache",
                rationale=rationale,
                baseline=Variant(
                    "baseline",
                    render_query(query_id, config_path=config_path, views={"integrated": uncached}),
                ),
                optimized=Variant(
                    "optimized",
                    render_query(query_id, config_path=config_path, views={"integrated": cached}),
                ),
                cache_source_sql=projection,
            )
        )

    base = {
        "analysis_timezone": timezone_name,
        "taxi_view": "standardized_taxi",
        "zones_view": "standardized_taxi_zones",
        "trip_filter": "TRUE",
    }
    experiments.append(
        Experiment(
            name="q1_broadcast_zones",
            query_id="q1",
            technique="broadcast_join",
            rationale=(
                "Q1 from the base tables joins nine million trips with a 265-row zone lookup. "
                "With automatic broadcasting and AQE disabled the join is a sort-merge join; "
                "an explicit BROADCAST hint removes the shuffle of the large side."
            ),
            baseline=Variant(
                "baseline", BASE_TABLE_Q1.format(hint="", **base), conf=NO_AUTO_BROADCAST
            ),
            optimized=Variant(
                "optimized",
                BASE_TABLE_Q1.format(hint="/*+ BROADCAST(z) */", **base),
                conf=NO_AUTO_BROADCAST,
            ),
            reference_sql=render_query("q1", config_path=config_path),
        )
    )

    for query_id, rationale in (
        (
            "q4",
            "Q4 chains a cross join, a left join and three aggregations, so AQE can coalesce "
            "post-shuffle partitions and switch join strategies from runtime statistics.",
        ),
        (
            "q6",
            "Q6 is a single wide aggregation followed by a window; AQE mainly coalesces the "
            "128 shuffle partitions of a three-row result.",
        ),
    ):
        sql = render_query(query_id, config_path=config_path)
        experiments.append(
            Experiment(
                name=f"{query_id}_aqe",
                query_id=query_id,
                technique="aqe",
                rationale=rationale,
                baseline=Variant("baseline", sql, conf={"spark.sql.adaptive.enabled": "false"}),
                optimized=Variant("optimized", sql, conf={"spark.sql.adaptive.enabled": "true"}),
            )
        )

    for query_id, product in PRODUCT_QUERIES.items():
        if not products or product not in products:
            continue
        template = (PRODUCT_SQL_DIRECTORY / f"{query_id}_from_{product}.sql").read_text(
            encoding="utf-8"
        )
        experiments.append(
            Experiment(
                name=f"{query_id}_product_{product}",
                query_id=query_id,
                technique="product",
                rationale=(
                    f"{query_id.upper()} answered from the materialized {product} table instead "
                    "of the integrated trips; the rewrite must reproduce the canonical result."
                ),
                baseline=Variant("baseline", render_query(query_id, config_path=config_path)),
                optimized=Variant(
                    "optimized",
                    render_template(
                        template,
                        config_path=config_path,
                        product_view=products[product]["view"],
                    ),
                ),
            )
        )
    return experiments


def _check_columns(frame: DataFrame, query_id: str) -> None:
    expected = list(QUERY_DEFINITIONS[query_id].result_columns)
    if frame.columns != expected:
        raise RuntimeError(f"Variant returned {frame.columns}; expected {expected}.")


def _timed_collect(spark: SparkSession, variant: Variant) -> tuple[DataFrame, list, float, str]:
    with spark_conf(spark, variant.conf):
        started = perf_counter()
        frame = spark.sql(variant.sql)
        rows = frame.collect()
        elapsed = perf_counter() - started
        # Both views come from the executed QueryExecution, so with AQE they show the final plan.
        executed = frame._jdf.queryExecution().executedPlan().toString()
        plan = explain_formatted(frame) + "\n== Executed Plan ==\n" + executed
    return frame, rows, elapsed, plan


class _Runner:
    """Bookkeeping shared by the alternated and the cache protocols of one experiment."""

    def __init__(self, spark: SparkSession, experiment: Experiment, result: dict[str, Any], sql_dir: Path):
        self.spark = spark
        self.experiment = experiment
        self.result = result
        self.sql_dir = sql_dir
        self.answers: dict[str, list] = {}
        self.plans: dict[str, str] = {}

    def warm(self, variant: Variant) -> None:
        (self.sql_dir / f"{self.experiment.name}_{variant.name}.sql").write_text(
            variant.sql + "\n", encoding="utf-8"
        )
        frame, rows, _, _ = _timed_collect(self.spark, variant)
        _check_columns(frame, self.experiment.query_id)
        self.answers[variant.name] = rows

    def verify(self) -> bool:
        try:
            assert_same_results(self.answers["baseline"], self.answers["optimized"])
            if self.experiment.reference_sql:
                reference = self.spark.sql(self.experiment.reference_sql).collect()
                assert_same_results(reference, self.answers["baseline"])
        except AssertionError as error:
            self.result["mismatch"] = str(error)
            self.result["samples"] = []
            return False
        self.result["results_equal"] = True
        self.result["row_count"] = len(self.answers["baseline"])
        return True

    def measure(self, variant: Variant, repeat: int, position: int) -> None:
        _, rows, elapsed, plan = _timed_collect(self.spark, variant)
        assert_same_results(self.answers[variant.name], rows)
        self.result["samples"].append(
            {"variant": variant.name, "repeat": repeat, "position": position, "seconds": elapsed}
        )
        self.plans[variant.name] = plan


def run_experiment(
    spark: SparkSession,
    experiment: Experiment,
    output_dir: str | Path,
    *,
    repeats: int = 3,
) -> dict[str, Any]:
    """Warm both variants, verify equality, then time them; see the module docstring."""
    if repeats < 1:
        raise ValueError("repeats must be at least 1")
    output = Path(output_dir)
    sql_dir = output / "sql"
    plans_dir = output / "plans"
    sql_dir.mkdir(parents=True, exist_ok=True)
    plans_dir.mkdir(parents=True, exist_ok=True)
    result: dict[str, Any] = {
        "name": experiment.name,
        "query_id": experiment.query_id,
        "technique": experiment.technique,
        "rationale": experiment.rationale,
        "date_range": list(experiment.date_range),
        "variants": {variant.name: {"conf": dict(variant.conf)} for variant in experiment.variants},
        "samples": [],
        "results_equal": False,
    }
    runner = _Runner(spark, experiment, result, sql_dir)
    cached_frame: DataFrame | None = None
    try:
        if experiment.cache_source_sql:
            # The baseline must finish before the cache exists (see module docstring).
            uncached_view, cached_view = cache_view_names(experiment.name)
            spark.sql(experiment.cache_source_sql).createOrReplaceTempView(uncached_view)
            runner.warm(experiment.baseline)
            for repeat in range(repeats):
                runner.measure(experiment.baseline, repeat + 1, 1)

            # Delta keeps its own log-state RDDs cached, so report the cache as a difference.
            storage_before = cache_storage(spark)
            cached_frame = spark.sql(experiment.cache_source_sql).cache()
            cached_frame.createOrReplaceTempView(cached_view)
            started = perf_counter()
            cached_rows = cached_frame.count()
            storage_after = cache_storage(spark)
            result["cache"] = {
                "source_sql": experiment.cache_source_sql,
                "build_seconds": perf_counter() - started,
                "rows": cached_rows,
                **{key: storage_after[key] - storage_before[key] for key in storage_after},
                "storage_before": storage_before,
                "storage_after_build": storage_after,
            }
            result["order"] = "baseline measured before the cache was built; not alternated"
            runner.warm(experiment.optimized)
            if not runner.verify():
                return result
            for repeat in range(repeats):
                runner.measure(experiment.optimized, repeat + 1, 1)
        else:
            for variant in experiment.variants:
                runner.warm(variant)
            if not runner.verify():
                return result
            result["order"] = "baseline/optimized alternated every repeat"
            for repeat in range(repeats):
                order = experiment.variants if repeat % 2 == 0 else experiment.variants[::-1]
                for position, variant in enumerate(order):
                    runner.measure(variant, repeat + 1, position + 1)

        for variant in experiment.variants:
            (plans_dir / f"{experiment.name}_{variant.name}.txt").write_text(
                variant.sql + ";\n\n" + runner.plans[variant.name], encoding="utf-8"
            )
            summary = result["variants"][variant.name]
            summary["median_seconds"] = median(
                sample["seconds"]
                for sample in result["samples"]
                if sample["variant"] == variant.name
            )
            summary["plan"] = plan_indicators(runner.plans[variant.name])
        result["speedup"] = (
            result["variants"]["baseline"]["median_seconds"]
            / result["variants"]["optimized"]["median_seconds"]
        )
        return result
    finally:
        if cached_frame is not None:
            cached_frame.unpersist(blocking=True)
            result["cache"]["storage_after_release"] = cache_storage(spark)
        if experiment.cache_source_sql:
            for view in cache_view_names(experiment.name):
                spark.catalog.dropTempView(view)
        spark.catalog.clearCache()


def run_experiments(
    spark: SparkSession,
    experiments: list[Experiment],
    output_dir: str | Path,
    *,
    repeats: int = 3,
    on_result=None,
) -> list[dict[str, Any]]:
    """Run experiments in order; ``on_result`` lets a CLI persist after each one."""
    results = []
    for experiment in experiments:
        result = run_experiment(spark, experiment, output_dir, repeats=repeats)
        results.append(result)
        if on_result is not None:
            on_result(result)
    return results
