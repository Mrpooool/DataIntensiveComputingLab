"""Role A ingestion and raw preservation fixtures."""

import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from pyspark.sql import Row, SparkSession

from dic_pipeline.wikipulse.archive import archive_columns
from dic_pipeline.wikipulse.producer import event_key, load_state, save_state, sse_events


class ProducerTests(unittest.TestCase):
    def test_sse_multiline_and_comments(self) -> None:
        lines = [b": heartbeat", b"id: 42", b"data: {\"meta\":", b"data: {\"id\":\"abc\"}}", b"",
                 b"event: status", b"data: ignored", b""]
        self.assertEqual(list(sse_events(lines)), [
            ("42", None, b'{"meta":\n{"id":"abc"}}'),
            (None, "status", b"ignored"),
        ])

    def test_sse_empty_data_and_eof_are_dispatched(self) -> None:
        self.assertEqual(list(sse_events([b"id: empty", b"data:", b""])),
                         [("empty", None, b"")])
        self.assertEqual(list(sse_events([b"id: tail", b"data: final"])),
                         [("tail", None, b"final")])

    def test_source_id_key_and_malformed_fallback(self) -> None:
        first = b'{"meta":{"id":"uuid-1"},"timestamp":1720000000}'
        self.assertEqual(event_key(first), (b"uuid-1", "uuid-1", 1720000000, True))
        bad = b"{invalid"
        self.assertEqual(event_key(bad)[0], event_key(bad)[0])
        self.assertIsNone(event_key(bad)[1])
        self.assertFalse(event_key(bad)[3])
        valid_bad_time = b'{"meta":{"id":"uuid-2"},"timestamp":"unknown"}'
        self.assertEqual(event_key(valid_bad_time)[1:], ("uuid-2", None, True))
        self.assertEqual(event_key(b'{"timestamp":0}')[2], 0)

    def test_capture_persists_only_acknowledged_ids(self) -> None:
        response = MagicMock()
        response.__enter__.return_value = response
        response.iter_lines.return_value = [
            b"id: first", b'data: {"meta":{"id":"source-1"},"timestamp":0}', b"",
            b"id: second", b"data: {invalid", b"",
        ]
        producer = MagicMock()
        with tempfile.TemporaryDirectory() as tmp, \
                patch("dic_pipeline.wikipulse.producer.KafkaProducer", return_value=producer), \
                patch("dic_pipeline.wikipulse.producer.requests.get", return_value=response):
            from dic_pipeline.wikipulse.producer import capture

            capture("localhost:9092", "wikipulse.raw.live", Path(tmp) / "state.json",
                    stream_url="https://example.invalid/events", max_events=2)
            self.assertEqual(load_state(Path(tmp) / "state.json"), "second")
        first_headers = dict(producer.send.call_args_list[0].kwargs["headers"])
        self.assertEqual(first_headers["source_event_unix"], b"0")
        self.assertEqual(producer.send.call_count, 2)

    def test_state_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "nested" / "state.json"
            self.assertIsNone(load_state(path))
            save_state(path, "event-123")
            self.assertEqual(load_state(path), "event-123")
            self.assertEqual(json.loads(path.read_text())["last_event_id"], "event-123")


class ArchiveTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        os.environ["PYSPARK_PYTHON"] = sys.executable
        cls.spark = (SparkSession.builder.master("local[1]").appName("wikipulse-a-test")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.driver.host", "127.0.0.1").getOrCreate())

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_preserves_bad_json_and_non_wikipedia(self) -> None:
        at = datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc)
        header = [Row(key="source_id", value=b"id-old"), Row(key="source_id", value=b"id-1"),
                  Row(key="source_event_unix", value=b"1720000000"),
                  Row(key="received_at_utc", value=b"2026-10-07T12:00:01+00:00")]
        invalid_header = [Row(key="source_event_unix", value=b"not-a-number"),
                          Row(key="received_at_utc", value=b"not-a-date")]
        rows = [
            Row(topic="raw", partition=0, offset=1, timestamp=at, timestampType=0,
                key=b"id-1", value=b'{"server_name":"en.wikipedia.org"}', headers=header),
            Row(topic="raw", partition=1, offset=2, timestamp=at, timestampType=0,
                key=b"hash", value=b"{invalid", headers=invalid_header),
        ]
        archived = archive_columns(self.spark.createDataFrame(rows)).orderBy("kafka_offset").collect()
        self.assertEqual(len(archived), 2)
        self.assertEqual(archived[0]["source_id"], "id-1")
        self.assertEqual(archived[0]["source_event_unix"], 1720000000)
        self.assertEqual(archived[0]["ingest_date_utc"].isoformat(), "2026-10-07")
        self.assertEqual(bytes(archived[1]["raw_value"]), b"{invalid")
        self.assertEqual(archived[1]["kafka_partition"], 1)
        self.assertIsNone(archived[1]["source_event_unix"])
        self.assertIsNone(archived[1]["received_at_utc"])


if __name__ == "__main__":
    unittest.main()
