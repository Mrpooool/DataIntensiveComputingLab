"""Week 3 production-readiness evaluation (Task 5).

Each measurement compares one or two variants of a pipeline stage, for example
the same ingestion with monitoring on and off. Every run, warm-up included,
starts from a fresh copy of the baseline Delta root in its own scratch
directory: incremental updates and refreshes mutate tables, and Delta keeps
every old file after an overwrite or RESTORE, so a reused workspace would carry
one run's output into the next and inflate the storage numbers. A new path per
run also keeps Delta's per-path log cache from seeing a table rewound under
it. Copying happens before the timer starts.

Variants must produce the same outputs; a run whose output signature differs
from the first run's makes the measurement fail instead of reporting a time.

Measurements that depend on role A's incremental pipeline or role B's
validation switch are declared here with the entry point they need and are
reported as ``pending`` until that entry point exists (see
``docs/w3_interfaces.md``).
"""

from __future__ import annotations

import importlib
import inspect
import os
import shutil
import stat
from dataclasses import dataclass, field
from pathlib import Path
from statistics import median
from time import perf_counter
from typing import Any, Callable, Mapping

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F

from .data_products import refresh_data_products
from .ingestion import DEFAULT_DATA_DIR, ingest_batch, ingest_dataset
from .integration import build_integrated_table
from .monitoring import PIPELINE_RUNS


# (spark, workspace delta root, run_id) -> output signature used for the equality check.
VariantRun = Callable[[SparkSession, Path, str], Mapping[str, Any]]


@dataclass(frozen=True)
class Variant:
    name: str
    run: VariantRun


@dataclass(frozen=True)
class Measurement:
    name: str
    metric: str
    description: str
    variants: tuple[Variant, ...] = ()
    # Copy the baseline Delta root into the workspace; ingestion starts from an empty one.
    needs_baseline: bool = True
    # Why the measurement cannot run yet: the missing entry point of another role.
    pending: str | None = None
    notes: tuple[str, ...] = field(default_factory=tuple)


def _remove_tree(path: Path) -> None:
    def make_writable(function, name, _info):
        os.chmod(name, stat.S_IWRITE)
        function(name)

    if path.exists():
        shutil.rmtree(path, onerror=make_writable)


def fresh_workspace(baseline_root: Path | None, workspace: Path) -> Path:
    """Create ``workspace`` as a copy of the baseline (or empty); it must not exist yet."""
    if workspace.exists():
        raise FileExistsError(f"Workspace {workspace} already exists; runs never share one.")
    if baseline_root is None:
        workspace.mkdir(parents=True)
    else:
        shutil.copytree(baseline_root, workspace)
    return workspace


def storage_report(spark: SparkSession, root: str | Path) -> dict[str, dict[str, int]]:
    """Per Delta table: current snapshot size, bytes on disk including old files and log, versions."""
    root = Path(root)
    report = {}
    for log in sorted(root.rglob("_delta_log")):
        table_path = log.parent
        files = [path for path in table_path.rglob("*") if path.is_file()]
        detail = DeltaTable.forPath(spark, str(table_path)).detail().first()
        report[table_path.relative_to(root).as_posix()] = {
            "snapshot_bytes": int(detail.sizeInBytes),
            "snapshot_files": int(detail.numFiles),
            "disk_bytes": sum(path.stat().st_size for path in files),
            "disk_files": len(files),
            "delta_log_bytes": sum(path.stat().st_size for path in log.rglob("*") if path.is_file()),
            "commits": len(list(log.glob("*.json"))),
        }
    return report


def stage_rows(spark: SparkSession, root: Path, run_id: str) -> list[dict[str, Any]]:
    """The monitoring rows a run wrote into its workspace, for a per-stage breakdown."""
    path = root / PIPELINE_RUNS
    if not (path / "_delta_log").exists():
        return []
    rows = (
        spark.read.format("delta").load(str(path))
        .where(F.col("run_id") == run_id)
        .select("stage", "target", "mode", "execution_seconds", "processed_count",
                "inserted_count", "rejected_count", "duplicate_count", "status")
        .orderBy("started_at")
        .collect()
    )
    return [row.asDict() for row in rows]


def _optional(module: str, attribute: str) -> Any | None:
    """A sibling module's function, or None while its owner has not delivered it."""
    try:
        return getattr(importlib.import_module(f".{module}", __package__), attribute, None)
    except ModuleNotFoundError:
        return None


def _accepts(function: Callable[..., Any], parameter: str) -> bool:
    return parameter in inspect.signature(function).parameters


# ---- Variants available now -------------------------------------------------------------


def _ingestion(data_dir: Path, *, monitoring: bool) -> VariantRun:
    def run(spark: SparkSession, root: Path, run_id: str) -> Mapping[str, Any]:
        records = ingest_batch(spark, data_dir=data_dir, delta_root=root, run_id=run_id,
                               monitoring=monitoring)
        return {r["dataset"]: [r["accepted_count"], r["rejected_count"]] for r in records}
    return run


def _integration(*, monitoring: bool) -> VariantRun:
    def run(spark: SparkSession, root: Path, run_id: str) -> Mapping[str, Any]:
        result = build_integrated_table(spark, root, run_id=run_id, monitoring=monitoring)
        return {"output_count": result["stats"]["output_count"]}
    return run


def _refresh(*, monitoring: bool, **options: Any) -> VariantRun:
    def run(spark: SparkSession, root: Path, run_id: str) -> Mapping[str, Any]:
        records = refresh_data_products(spark, delta_root=root, run_id=run_id,
                                        monitoring=monitoring, **options)
        return {r["product_name"]: r["row_count"] for r in records}
    return run


def build_measurements(data_dir: str | Path = DEFAULT_DATA_DIR) -> list[Measurement]:
    """All Task 5 measurements; the ones waiting on role A or B carry ``pending``."""
    data_dir = Path(data_dir)
    apply_updates = _optional("incremental", "apply_updates")
    incremental_pending = (
        None if apply_updates is not None else
        "role A: dic_pipeline.incremental.generate_update() and apply_updates()"
    )
    refresh_pending = (
        None if _accepts(refresh_data_products, "mode") else
        "role A: refresh_data_products(mode='auto'|'full') after apply_updates()"
    )
    validation_pending = (
        None if _accepts(ingest_dataset, "validate") else
        "role B: ingest_dataset(validate=False) / prepare(validate=False)"
    )
    return [
        Measurement(
            "monitoring_overhead_ingestion", "monitoring overhead",
            "Full four-dataset ingestion with and without the pipeline_runs rows.",
            (Variant("monitoring_off", _ingestion(data_dir, monitoring=False)),
             Variant("monitoring_on", _ingestion(data_dir, monitoring=True))),
            needs_baseline=False,
            notes=("Monitoring covers the row write plus the metadata only it needs: input file "
                   "list and output table stats. Per-code rejection counts are validation "
                   "reporting (Task 4) and run in both variants.",),
        ),
        Measurement(
            "monitoring_overhead_integration", "monitoring overhead",
            "Integration of the baseline batch with and without its pipeline_runs row.",
            (Variant("monitoring_off", _integration(monitoring=False)),
             Variant("monitoring_on", _integration(monitoring=True))),
        ),
        Measurement(
            "monitoring_overhead_refresh", "monitoring overhead",
            "Full refresh of the four data products with and without their pipeline_runs rows.",
            (Variant("monitoring_off", _refresh(monitoring=False)),
             Variant("monitoring_on", _refresh(monitoring=True))),
        ),
        Measurement(
            "incremental_update", "incremental update time",
            "Apply the Taxi, Weather and Air Quality update files to the baseline, "
            "including the integrated table, without rebuilding it.",
            pending=incremental_pending,
        ),
        Measurement(
            "analytical_refresh", "analytical refresh time",
            "After the updates: refresh only the affected products (mode='auto') versus "
            "rebuilding all four (mode='full'); outputs must be equal.",
            pending=incremental_pending or refresh_pending,
        ),
        Measurement(
            "storage_overhead", "storage overhead",
            "Snapshot and on-disk bytes, files and commits of every table before and after "
            "the updates, from the first measured run only.",
            pending=incremental_pending,
        ),
        Measurement(
            "validation_overhead", "validation overhead",
            "The same ingestion or update with validation rules on and off, on a scratch copy "
            "that is never published.",
            pending=validation_pending,
        ),
    ]


# ---- Runner -----------------------------------------------------------------------------


def run_measurement(
    spark: SparkSession,
    measurement: Measurement,
    *,
    baseline_root: Path,
    workspace_root: Path,
    run_prefix: str,
    repeats: int = 3,
    keep_workspaces: bool = False,
    on_run: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Warm each variant once, then time ``repeats`` alternated rounds on fresh workspaces."""
    if repeats < 1:
        raise ValueError("repeats must be at least 1")
    result: dict[str, Any] = {
        "name": measurement.name,
        "metric": measurement.metric,
        "description": measurement.description,
        "notes": list(measurement.notes),
        "status": "pending" if measurement.pending else "running",
        "variants": {variant.name: {} for variant in measurement.variants},
        "runs": [],
    }
    if measurement.pending or not measurement.variants:
        # An entry point that arrived before its variants were wired here is still pending.
        result["status"] = "pending"
        result["requires"] = measurement.pending or "role C: wire the variants in build_measurements()"
        return result

    signature: Mapping[str, Any] | None = None
    schedule = [("warmup", 0, variant) for variant in measurement.variants]
    for repeat in range(1, repeats + 1):
        order = measurement.variants if repeat % 2 else measurement.variants[::-1]
        schedule += [("measured", repeat, variant) for variant in order]

    for phase, repeat, variant in schedule:
        run_id = f"{run_prefix}-{measurement.name}-{variant.name}-{phase}{repeat or ''}"
        workspace = fresh_workspace(
            baseline_root if measurement.needs_baseline else None, workspace_root / run_id
        )
        spark.catalog.clearCache()
        started = perf_counter()
        output = dict(variant.run(spark, workspace, run_id))
        seconds = perf_counter() - started
        entry = {
            "variant": variant.name, "phase": phase, "repeat": repeat, "run_id": run_id,
            "seconds": seconds, "output": output, "stages": stage_rows(spark, workspace, run_id),
        }
        if not keep_workspaces:
            _remove_tree(workspace)
        result["runs"].append(entry)
        if on_run is not None:
            on_run(entry)
        if signature is None:
            signature = output
        elif output != signature:
            result["status"] = "mismatch"
            result["mismatch"] = {"expected": signature, "actual": output, "run_id": run_id}
            return result

    for variant in measurement.variants:
        samples = [run["seconds"] for run in result["runs"]
                   if run["variant"] == variant.name and run["phase"] == "measured"]
        result["variants"][variant.name] = {"median_seconds": median(samples), "samples": samples}
    if len(measurement.variants) == 2:
        first, second = (result["variants"][v.name]["median_seconds"] for v in measurement.variants)
        result["difference_seconds"] = second - first
        result["relative_difference"] = (second - first) / first if first else None
    result["status"] = "success"
    return result
