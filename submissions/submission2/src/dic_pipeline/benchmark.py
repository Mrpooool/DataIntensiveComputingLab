"""Controlled Taxi storage experiment for Week 1 Task 6."""

from contextlib import redirect_stdout
from io import StringIO
import math
from statistics import median
from time import perf_counter

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from .contracts import load_dataset_config
from .ingestion import read_source, write_delta
from .preparation import prepare


LAYOUTS = ("S0", "S1")
QUERIES = {
    "trips_per_borough": """SELECT z.borough, COUNT(*) AS trip_count
FROM {table} t LEFT JOIN benchmark_zones z ON t.pickup_location_id = z.location_id
GROUP BY z.borough ORDER BY z.borough""",
    "duration_per_day": """SELECT pickup_date, AVG(trip_duration_seconds) AS avg_duration_seconds
FROM {table} GROUP BY pickup_date ORDER BY pickup_date""",
    "fare_per_borough": """SELECT z.borough, AVG(t.fare_amount) AS avg_fare
FROM {table} t LEFT JOIN benchmark_zones z ON t.pickup_location_id = z.location_id
GROUP BY z.borough ORDER BY z.borough""",
    "one_week": """SELECT pickup_date, COUNT(*) AS trip_count, AVG(fare_amount) AS avg_fare
FROM {table} WHERE pickup_date BETWEEN DATE '2024-02-01' AND DATE '2024-02-07'
GROUP BY pickup_date ORDER BY pickup_date""",
}
REL_TOL = 1e-9
ABS_TOL = 1e-6


def assert_same_results(expected, actual):
    """Compare sorted aggregate rows; only floating-point means use tolerance."""
    if len(expected) != len(actual):
        raise AssertionError("Query result row counts differ")
    for left, right in zip(expected, actual):
        if len(left) != len(right):
            raise AssertionError("Query result column counts differ")
        for a, b in zip(left, right):
            if isinstance(a, float) and isinstance(b, float):
                equal = math.isclose(a, b, rel_tol=REL_TOL, abs_tol=ABS_TOL)
            else:
                equal = a == b
            if not equal:
                raise AssertionError(f"Query results differ: {a!r} != {b!r}")


def table_stats(spark, path):
    """Measure current Delta data files, excluding logs, CRCs and old files."""
    table = DeltaTable.forPath(spark, str(path))
    detail = table.detail().first()
    frame = spark.read.format("delta").load(str(path))
    counts = frame.agg(
        F.count("*").alias("rows"), F.count_distinct("record_id").alias("unique_ids"),
    ).first()
    return {
        "row_count": counts.rows, "unique_record_count": counts.unique_ids,
        "data_bytes": detail.sizeInBytes, "data_files": detail.numFiles,
        "partition_columns": detail.partitionColumns,
        "delta_version": table.history(1).first().version,
    }


def ingest_layout(spark, data_dir, output_dir, layout, run_id):
    """Time raw read + B preparation + accepted/rejected Delta commits."""
    config = load_dataset_config("taxi")
    spark.catalog.clearCache()
    started = perf_counter()
    result = prepare(read_source(spark, "taxi", data_dir), config, run_id=run_id)
    try:
        write_delta(
            result.accepted, output_dir / "taxi", num_files=config["output_files"],
            partition_by=["pickup_date"] if layout == "S1" else [],
        )
        write_delta(result.rejected, output_dir / "rejected", num_files=1)
        elapsed = perf_counter() - started
        quality = dict(result.metrics)
    finally:
        result.release()
        spark.catalog.clearCache()
    stats = table_stats(spark, output_dir / "taxi")
    rejected_count = spark.read.format("delta").load(str(output_dir / "rejected")).count()
    if not (stats["row_count"] == stats["unique_record_count"] == quality["accepted_count"]):
        raise AssertionError("Written Taxi rows/unique IDs differ from accepted count")
    if rejected_count != quality["rejected_count"]:
        raise AssertionError("Written rejected count differs from preparation")
    return {"layout": layout, "ingestion_seconds": elapsed, **stats, "quality": quality}


def benchmark_queries(spark, tables, zones, output_dir):
    """Warm each query once, then alternate layouts for three measured rounds."""
    zones.select("location_id", "borough").createOrReplaceTempView("benchmark_zones")
    schemas = []
    for layout, frame in tables.items():
        frame.createOrReplaceTempView(f"benchmark_{layout}")
        schemas.append(sorted((f.name, f.dataType.simpleString()) for f in frame.schema))
    if schemas[0] != schemas[1]:
        raise AssertionError("Storage layouts have different schemas")
    plans_dir = output_dir / "plans"
    plans_dir.mkdir()
    samples, answers = [], {}
    for name, template in QUERIES.items():
        for layout in LAYOUTS:
            sql = template.format(table=f"benchmark_{layout}")
            rows = spark.sql(sql).collect()
            if layout == "S0":
                answers[name] = rows
            else:
                assert_same_results(answers[name], rows)
        for repeat in range(3):
            order = LAYOUTS if repeat % 2 == 0 else LAYOUTS[::-1]
            for position, layout in enumerate(order):
                sql = template.format(table=f"benchmark_{layout}")
                started = perf_counter()
                frame = spark.sql(sql)
                rows = frame.collect()
                elapsed = perf_counter() - started
                assert_same_results(answers[name], rows)
                samples.append({
                    "query": name, "layout": layout, "repeat": repeat + 1,
                    "position": position + 1, "seconds": elapsed,
                })
                if repeat == 2:
                    plan = StringIO()
                    with redirect_stdout(plan):
                        frame.explain(mode="formatted")
                    (plans_dir / f"{name}_{layout}.txt").write_text(
                        sql + ";\n\n" + plan.getvalue(), encoding="utf-8",
                    )
        print(f"Query verified: {name}", flush=True)
    medians = {
        name: {
            layout: median(s["seconds"] for s in samples if s["query"] == name and s["layout"] == layout)
            for layout in LAYOUTS
        }
        for name in QUERIES
    }
    return {
        "samples": samples, "median_seconds": medians,
        "results": {name: [row.asDict() for row in rows] for name, rows in answers.items()},
        "results_equal": True, "relative_tolerance": REL_TOL, "absolute_tolerance": ABS_TOL,
    }
