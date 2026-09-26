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

The update-based measurements share one untimed setup (``prepare_updates``):
update files generated from the baseline, and a copy of the baseline with them
applied, which the refresh comparison starts from. Storage overhead is the
storage report of that copy against the baseline's; it is not timed.
"""

from __future__ import annotations

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

from .data_products import RESERVED_METADATA_COLUMNS, refresh_data_products
from .incremental import UPDATE_DATASETS, apply_updates, generate_update
from .ingestion import DEFAULT_DATA_DIR, ingest_batch
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
    # Copy the baseline with the updates applied instead (see prepare_updates).
    after_updates: bool = False
    # Why the measurement cannot run yet.
    pending: str | None = None
    notes: tuple[str, ...] = field(default_factory=tuple)


def remove_tree(path: Path) -> None:
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


def prepare_updates(
    spark: SparkSession, baseline_root: Path, directory: Path, run_id: str,
) -> tuple[list[dict[str, Any]], Path]:
    """Untimed setup: update files generated from the baseline, and a copy with them applied."""
    manifests = [
        generate_update(spark, dataset, directory / "updates", delta_root=baseline_root, seed=0)
        for dataset in UPDATE_DATASETS
    ]
    updated_root = fresh_workspace(baseline_root, directory / "baseline_updated")
    apply_updates(spark, manifests, delta_root=updated_root, run_id=run_id)
    return manifests, updated_root


def product_signature(spark: SparkSession, root: Path) -> dict[str, list[int]]:
    """Row count and an order-independent content hash of every product, metadata excluded.

    Doubles are rounded first: an incremental MERGE sums an hour's trips in a
    different order than a full rebuild, which can move the last bits.
    """
    signature = {}
    for path in sorted((root / "analytics").iterdir()):
        frame = spark.read.format("delta").load(str(path))
        columns = [
            F.round(F.col(field.name), 6) if field.dataType.typeName() == "double" else F.col(field.name)
            for field in frame.schema.fields if field.name not in RESERVED_METADATA_COLUMNS
        ]
        row = frame.agg(F.count(F.lit(1)), F.bit_xor(F.xxhash64(*columns))).first()
        signature[path.name] = [int(row[0]), int(row[1] or 0)]
    return signature


# ---- Variants ---------------------------------------------------------------------------


def _ingestion(data_dir: Path, *, monitoring: bool = True, validate: bool = True) -> VariantRun:
    def run(spark: SparkSession, root: Path, run_id: str) -> Mapping[str, Any]:
        records = ingest_batch(spark, data_dir=data_dir, delta_root=root, run_id=run_id,
                               validate=validate, monitoring=monitoring)
        # In-scope input rows: validation changes the accepted/rejected split, not this.
        return {r["dataset"]: r["input_count"] for r in records}
    return run


def _integration(*, monitoring: bool) -> VariantRun:
    def run(spark: SparkSession, root: Path, run_id: str) -> Mapping[str, Any]:
        result = build_integrated_table(spark, root, run_id=run_id, monitoring=monitoring)
        return {"output_count": result["stats"]["output_count"]}
    return run


def _refresh(*, monitoring: bool = True, mode: str = "full") -> VariantRun:
    def run(spark: SparkSession, root: Path, run_id: str) -> Mapping[str, Any]:
        refresh_data_products(spark, delta_root=root, run_id=run_id,
                              monitoring=monitoring, mode=mode)
        return product_signature(spark, root)
    return run


def _apply(updates: list[dict[str, Any]]) -> VariantRun:
    def run(spark: SparkSession, root: Path, run_id: str) -> Mapping[str, Any]:
        records = apply_updates(spark, updates, delta_root=root, run_id=run_id)
        return {r["dataset"]: [r.get(key) for key in (
            "inserted_count", "updated_count", "duplicate_count", "rejected_count")]
            for r in records}
    return run


def build_measurements(
    data_dir: str | Path = DEFAULT_DATA_DIR,
    updates: list[dict[str, Any]] | None = None,
) -> list[Measurement]:
    """All Task 5 timings. The update-based ones need the manifests from ``prepare_updates``."""
    data_dir = Path(data_dir)
    no_updates = None if updates is not None else (
        "update files: scripts.run_w3_evaluation generates them from the baseline first"
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
            "validation_overhead", "validation overhead",
            "Full four-dataset ingestion with the validation rules off and on.",
            (Variant("validation_off", _ingestion(data_dir, validate=False)),
             Variant("validation_on", _ingestion(data_dir, validate=True))),
            needs_baseline=False,
            notes=("Off keeps the in-file duplicate collapse, so the outputs differ only by the "
                   "rows the rules reject; the per-run split is in each run's stage rows.",),
        ),
        Measurement(
            "incremental_update", "incremental update time",
            "Apply the Taxi, Weather and Air Quality update files to the baseline, "
            "including the integrated table, without rebuilding it.",
            () if updates is None else (Variant("apply", _apply(updates)),),
            pending=no_updates,
        ),
        Measurement(
            "analytical_refresh", "analytical refresh time",
            "After the updates: rebuild all four products (mode='full') versus refresh only "
            "what changed (mode='auto'); product contents must be equal.",
            (Variant("full", _refresh(mode="full")), Variant("auto", _refresh(mode="auto"))),
            after_updates=True,
            pending=no_updates,
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
    updated_root: Path | None = None,
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
        result["requires"] = measurement.pending or "variants in build_measurements()"
        return result

    source = None
    if measurement.needs_baseline:
        source = updated_root if measurement.after_updates else baseline_root

    signature: Mapping[str, Any] | None = None
    schedule = [("warmup", 0, variant) for variant in measurement.variants]
    for repeat in range(1, repeats + 1):
        order = measurement.variants if repeat % 2 else measurement.variants[::-1]
        schedule += [("measured", repeat, variant) for variant in order]

    for phase, repeat, variant in schedule:
        run_id = f"{run_prefix}-{measurement.name}-{variant.name}-{phase}{repeat or ''}"
        workspace = fresh_workspace(source, workspace_root / run_id)
        spark.catalog.clearCache()
        started = perf_counter()
        output = dict(variant.run(spark, workspace, run_id))
        seconds = perf_counter() - started
        entry = {
            "variant": variant.name, "phase": phase, "repeat": repeat, "run_id": run_id,
            "seconds": seconds, "output": output, "stages": stage_rows(spark, workspace, run_id),
        }
        if not keep_workspaces:
            remove_tree(workspace)
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
