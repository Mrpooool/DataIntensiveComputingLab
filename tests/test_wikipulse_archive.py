"""Raw Kafka-to-Delta archive projection: preserve payload, keys and headers."""

import unittest
from datetime import datetime, timezone

from pyspark.sql import Row

from dic_pipeline.ingestion import create_spark
from dic_pipeline.wikipulse.archive import archive_columns


class ArchiveTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = create_spark(master="local[1]", shuffle_partitions=1)

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
