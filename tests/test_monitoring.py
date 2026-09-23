import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

from dic_pipeline.ingestion import create_spark, ingest_dataset
from dic_pipeline.monitoring import (
    PIPELINE_RUNS,
    MonitoringWriteError,
    import_legacy_runs,
    record_run,
    run_report,
    run_row,
    write_run,
)


START = datetime(2026, 9, 1, 12, tzinfo=timezone.utc)
LEGACY_INGESTION_SCHEMA = (
    "run_id string, dataset string, started_at timestamp, finished_at timestamp, "
    "execution_seconds double, raw_input_count bigint, scope_excluded_count bigint, "
    "input_count bigint, accepted_count bigint, rejected_count bigint, duplicate_count bigint, "
    "schema_version string, rule_version string, status string, error_counts_json string, "
    "quality_flag_counts_json string, error_message string"
)


def row(run_id, target, seconds, *, stage="ingestion", processed=100, rejected=0,
        status="success", offset_hours=0, codes=None, validation=True):
    return run_row(
        run_id=run_id,
        stage=stage,
        target=target,
        started_at=START + timedelta(hours=offset_hours),
        execution_seconds=seconds,
        status=status,
        processed_count=processed,
        inserted_count=None if processed is None else processed - rejected,
        updated_count=0,
        duplicate_count=0,
        rejected_count=rejected,
        validation_enabled=validation,
        validation_failure_counts=codes or {},
    )


class RunRowTests(unittest.TestCase):
    def test_contract_errors_fail_fast(self):
        row_args = dict(run_id="r", stage="ingestion", target="taxi", started_at=START,
                        execution_seconds=1.0, status="success")
        with self.assertRaisesRegex(ValueError, "Unknown monitoring fields"):
            run_row(**row_args, inserted_rows=3)
        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            run_row(**{**row_args, "started_at": START.replace(tzinfo=None)})
        with self.assertRaisesRegex(ValueError, "Unknown stage"):
            run_row(**{**row_args, "stage": "ingest"})

    def test_json_fields_and_truncated_error(self):
        built = run_row(
            run_id="r", stage="incremental_update", target="weather", started_at=START,
            execution_seconds=2, status="failed", mode="incremental",
            schema_changes=[{"op": "add", "column": "humidity", "type": "double", "nullable": True}],
            error=RuntimeError("x" * 5000),
        )
        self.assertEqual(json.loads(built["schema_changes_json"])[0]["column"], "humidity")
        self.assertEqual(len(built["error_message"]), 4000)
        self.assertIsNone(built["inserted_count"])

    def test_disabled_monitoring_never_writes(self):
        with mock.patch("dic_pipeline.monitoring.write_run") as writer:
            record_run(None, row("r", "taxi", 1.0), "unused", enabled=False)
        writer.assert_not_called()

    def test_stage_failure_wins_over_monitoring_failure(self):
        stage_error = ValueError("bad input")
        stderr = io.StringIO()
        with mock.patch("dic_pipeline.monitoring.write_run", side_effect=OSError("disk full")), \
                redirect_stderr(stderr):
            record_run(None, row("r", "taxi", 1.0, status="failed"), "unused", error=stage_error)
        self.assertIn("monitoring write failed: OSError: disk full", stage_error.__notes__[0])
        self.assertEqual(json.loads(stderr.getvalue())["run_id"], "r")

    def test_monitoring_failure_after_success_is_raised(self):
        with mock.patch("dic_pipeline.monitoring.write_run", side_effect=OSError("disk full")), \
                redirect_stderr(io.StringIO()):
            with self.assertRaises(MonitoringWriteError) as raised:
                record_run(None, row("r", "taxi", 1.0), "unused")
        self.assertEqual(raised.exception.record["target"], "taxi")
        self.assertIsInstance(raised.exception.__cause__, OSError)


class MonitoringTableTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark(master="local[2]", shuffle_partitions=2)
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _zones_csv(self, data_dir: Path) -> None:
        data_dir.mkdir(parents=True, exist_ok=True)
        (data_dir / "taxi_zone_lookup.csv").write_text(
            "LocationID,Borough,Zone,service_zone\n1,Newark Airport,Newark Airport,EWR\n",
            encoding="utf-8",
        )

    def test_ops_queries_answer_the_course_questions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for built in (
                row("run-1", "taxi", 100.0, processed=1000, rejected=10,
                    codes={"invalid_fare": 6, "duplicate_record": 4}),
                row("run-1", "weather", 5.0, processed=100, rejected=20, codes={"invalid_temp": 20}),
                row("run-2", "taxi", 80.0, processed=1000, rejected=0, offset_hours=1),
                row("run-2", "weather", 7.0, processed=100, rejected=0, offset_hours=1),
                row("run-3", "weather", 9.0, processed=100, rejected=0, offset_hours=2),
                # Validation off: must not count as a clean run.
                row("run-4", "weather", 4.0, processed=100, rejected=0, offset_hours=3,
                    validation=False),
                row("run-5", "taxi", 1.0, processed=None, rejected=0, offset_hours=4,
                    status="failed"),
                row("run-6", "daily_mobility_summary", 3.0, stage="product_refresh",
                    processed=5, offset_hours=5, validation=None),
            ):
                write_run(self.spark, built, root)

            report = {name: frame.collect() for name, frame in run_report(self.spark, root).items()}

            failing = report["validation_failures_by_target"]
            self.assertEqual(failing[0].target, "weather")
            self.assertEqual((failing[0].executions, failing[0].executions_with_rejects), (3, 1))
            self.assertAlmostEqual(failing[0].rejected_share, 20 / 300)
            self.assertEqual(failing[1].failed_executions, 1)

            codes = {(r.target, r.code): r.records for r in report["failure_codes_by_target"]}
            self.assertEqual(codes, {("taxi", "invalid_fare"): 6, ("taxi", "duplicate_record"): 4,
                                     ("weather", "invalid_temp"): 20})

            slowest = report["processing_time_by_target"][0]
            self.assertEqual((slowest.stage, slowest.target, slowest.executions), ("ingestion", "taxi", 2))
            self.assertEqual((slowest.avg_seconds, slowest.latest_seconds), (90.0, 80.0))

            per_run = report["rejected_per_execution"]
            self.assertEqual([(r.run_id, r.target, r.rejected_count) for r in per_run[:2]],
                             [("run-1", "taxi", 10), ("run-1", "weather", 20)])
            self.assertNotIn("daily_mobility_summary", {r.target for r in per_run})

            trend = [r for r in report["processing_time_trend"] if r.target == "weather"]
            self.assertEqual([r.execution_number for r in trend], [1, 2, 3, 4])
            self.assertEqual([r.change_seconds for r in trend], [None, 2.0, 2.0, -5.0])
            self.assertAlmostEqual(trend[2].moving_avg_3_seconds, 7.0)

    def test_ingestion_failure_survives_a_broken_monitoring_table(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with mock.patch("dic_pipeline.monitoring.write_run", side_effect=OSError("disk full")), \
                    redirect_stderr(io.StringIO()):
                with self.assertRaises(Exception) as raised:
                    ingest_dataset(self.spark, "weather", data_dir=root / "missing",
                                   delta_root=root / "delta", run_id="both-fail")
            self.assertNotIsInstance(raised.exception, MonitoringWriteError)
            self.assertIn("monitoring write failed", raised.exception.__notes__[0])

    def test_successful_ingestion_reports_a_lost_monitoring_row(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._zones_csv(root / "raw")
            with mock.patch("dic_pipeline.monitoring.write_run", side_effect=OSError("disk full")), \
                    redirect_stderr(io.StringIO()):
                with self.assertRaises(MonitoringWriteError) as raised:
                    ingest_dataset(self.spark, "taxi_zones", data_dir=root / "raw",
                                   delta_root=root / "delta", run_id="lost-row")
            self.assertEqual(raised.exception.record["inserted_count"], 1)
            # The data write already happened and stays in place.
            written = self.spark.read.format("delta").load(
                str(root / "delta" / "standardized" / "taxi_zones")
            )
            self.assertEqual(written.count(), 1)

    def test_legacy_audit_rows_are_imported_once(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy = self.spark.createDataFrame(
                [("w1", "taxi", START, START + timedelta(seconds=90), 90.0, 12, 0, 12, 10, 2, 1,
                  "1.0.0", "1.0.0", "success", '{"duplicate_record": 1, "invalid_fare": 1}', "{}",
                  None)],
                LEGACY_INGESTION_SCHEMA,
            )
            legacy.write.format("delta").save(str(root / "metadata" / "ingestion_runs"))

            self.assertEqual(import_legacy_runs(self.spark, root),
                             {"ingestion": 1, "product_refresh": 0})
            self.assertEqual(import_legacy_runs(self.spark, root)["ingestion"], 0)
            imported = self.spark.read.format("delta").load(str(root / PIPELINE_RUNS)).collect()
            self.assertEqual(len(imported), 1)
            self.assertEqual(
                (imported[0].stage, imported[0].processed_count, imported[0].inserted_count,
                 imported[0].rejected_count, imported[0].duplicate_count),
                ("ingestion", 12, 10, 2, 0),
            )


if __name__ == "__main__":
    unittest.main()
