"""Regression checks for the Week 3 evaluation runner. Timings are recorded, never asserted."""

import tempfile
import unittest
from pathlib import Path

import tests.test_incremental as incremental_fixtures
import tests.test_integration as integration_fixtures
from dic_pipeline.data_products import refresh_data_products
from dic_pipeline.ingestion import create_spark, write_delta
from dic_pipeline.integration import build_integrated_table
from dic_pipeline.monitoring import PIPELINE_RUNS
from dic_pipeline.w3_evaluation import (
    Measurement,
    Variant,
    build_measurements,
    prepare_updates,
    product_signature,
    run_measurement,
    storage_report,
)


class EvaluationRunnerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark(master="local[2]", shuffle_partitions=2)
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.directory = tempfile.TemporaryDirectory()
        cls.baseline = Path(cls.directory.name) / "baseline"
        integration_fixtures.ZoneIntegrationTests._write_batch(
            cls, cls.baseline, "base", integration_fixtures.utc(2024, 1, 1, 5)
        )
        build_integrated_table(cls.spark, cls.baseline, run_id="base-integration")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def _run(self, measurement, workspace_root, **options):
        return run_measurement(
            self.spark, measurement, baseline_root=self.baseline,
            workspace_root=workspace_root, run_prefix="test", **options,
        )

    def test_integration_monitoring_overhead_runs_on_copies(self):
        measurement = next(m for m in build_measurements() if m.name == "monitoring_overhead_integration")
        baseline_before = storage_report(self.spark, self.baseline)
        with tempfile.TemporaryDirectory() as directory:
            workspace_root = Path(directory)
            result = self._run(measurement, workspace_root, repeats=1)

            self.assertEqual(result["status"], "success")
            self.assertEqual([(r["variant"], r["phase"]) for r in result["runs"]], [
                ("monitoring_off", "warmup"), ("monitoring_on", "warmup"),
                ("monitoring_off", "measured"), ("monitoring_on", "measured"),
            ])
            self.assertEqual({str(r["output"]) for r in result["runs"]}, {"{'output_count': 2}"})
            stages = {r["variant"]: r["stages"] for r in result["runs"] if r["phase"] == "measured"}
            self.assertEqual(stages["monitoring_off"], [])
            self.assertEqual([(s["stage"], s["status"]) for s in stages["monitoring_on"]],
                             [("integration", "success")])
            self.assertIn("difference_seconds", result)
            self.assertEqual(list(workspace_root.iterdir()), [])
        # The baseline is copied, never written.
        self.assertEqual(storage_report(self.spark, self.baseline), baseline_before)

    def test_every_run_gets_a_fresh_copy_and_mismatches_stop_timing(self):
        seen = []

        def append_row(label):
            def run(spark, root, run_id):
                seen.append((run_id, (root / "integrated" / "integrated_taxi_trips").exists()))
                table = root / "scratch"
                write_delta(spark.createDataFrame([(run_id,)], "run_id string"), table, mode="append")
                return {"rows": spark.read.format("delta").load(str(table)).count(), "label": label}
            return run

        stable = Measurement("stable", "test", "fresh copy per run",
                             (Variant("a", append_row("same")), Variant("b", append_row("same"))))
        drifting = Measurement("drifting", "test", "outputs differ",
                               (Variant("a", append_row("a")), Variant("b", append_row("b"))))
        with tempfile.TemporaryDirectory() as directory:
            result = self._run(stable, Path(directory), repeats=2)
            # Each run appended once to its own copy, so no run saw another run's row.
            self.assertEqual({r["output"]["rows"] for r in result["runs"]}, {1})
            self.assertTrue(all(copied for _, copied in seen))
            self.assertEqual(len({run_id for run_id, _ in seen}), 6)
            # Alternated: repeat 1 runs a then b, repeat 2 runs b then a.
            measured = [(r["repeat"], r["variant"]) for r in result["runs"] if r["phase"] == "measured"]
            self.assertEqual(measured, [(1, "a"), (1, "b"), (2, "b"), (2, "a")])
            self.assertEqual(len(result["variants"]["a"]["samples"]), 2)

            mismatch = self._run(drifting, Path(directory) / "drift", repeats=1)
            self.assertEqual(mismatch["status"], "mismatch")
            self.assertNotIn("median_seconds", mismatch["variants"]["a"])

    def test_update_measurements_wait_for_prepared_updates(self):
        pending = {m.name for m in build_measurements() if m.pending}
        self.assertEqual(pending, {"incremental_update", "analytical_refresh"})
        self.assertFalse([m.name for m in build_measurements(updates=[]) if m.pending])
        measurement = next(m for m in build_measurements() if m.name == "incremental_update")
        with tempfile.TemporaryDirectory() as directory:
            result = self._run(measurement, Path(directory))
        self.assertEqual((result["status"], result["runs"]), ("pending", []))
        self.assertIn("update files", result["requires"])

    def test_update_measurements_run_and_refresh_modes_agree(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            incremental_fixtures.IncrementalTests._seed_platform(self, root)
            baseline = root / "delta"
            refresh_data_products(self.spark, delta_root=baseline, mode="full", monitoring=False)
            manifests, updated = prepare_updates(self.spark, baseline, root / "eval", "setup")
            measurements = {m.name: m for m in build_measurements(updates=manifests)}
            for name in ("incremental_update", "analytical_refresh"):
                result = run_measurement(
                    self.spark, measurements[name], baseline_root=baseline, updated_root=updated,
                    workspace_root=root / "workspace" / name, run_prefix="test", repeats=1,
                )
                # analytical_refresh: auto must reproduce the full rebuild's product contents.
                self.assertEqual(result["status"], "success", result.get("mismatch"))
            modes = {r["variant"]: {s["mode"] for s in r["stages"]} for r in result["runs"]}
            self.assertEqual(modes["full"], {"full"})
            self.assertIn("incremental", modes["auto"])

    def test_product_signature_ignores_metadata_and_float_noise(self):
        schema = ("trip_count long, distance_sum double, "
                  "source_delta_version long, schema_version string")
        rows = {
            "a": [(1, 0.1 + 0.2, 0, "1.0.0")],
            "b": [(1, 0.3, 5, "1.1.0")],
            "c": [(2, 0.3, 0, "1.0.0")],
        }
        with tempfile.TemporaryDirectory() as directory:
            signatures = {}
            for name, values in rows.items():
                root = Path(directory) / name
                write_delta(self.spark.createDataFrame(values, schema),
                            root / "analytics" / "product", num_files=1)
                signatures[name] = product_signature(self.spark, root)
        self.assertEqual(signatures["a"], signatures["b"])
        self.assertNotEqual(signatures["a"], signatures["c"])

    def test_storage_report_separates_snapshot_and_disk(self):
        report = storage_report(self.spark, self.baseline)
        self.assertIn("integrated/integrated_taxi_trips", report)
        self.assertIn(PIPELINE_RUNS, report)
        taxi = report["standardized/taxi"]
        self.assertEqual(taxi["commits"], 1)
        self.assertGreater(taxi["disk_bytes"], taxi["snapshot_bytes"])
        self.assertGreater(taxi["delta_log_bytes"], 0)


if __name__ == "__main__":
    unittest.main()
