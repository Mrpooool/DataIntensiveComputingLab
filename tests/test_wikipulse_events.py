"""Shared WikiPulse parsing, scope, deduplication and revert rules."""

import json
import tempfile
import unittest
from pathlib import Path

from pyspark.sql import functions as F

from dic_pipeline.ingestion import create_spark
from dic_pipeline.wikipulse.events import analysis_events, is_revert, parse_events


def event(source_id="id-1", *, server="en.wikipedia.org", wiki="enwiki", type_="edit",
          namespace=0, title="Page", comment="copyedit", timestamp=1791845328,
          user="Alice", bot=False, length=(100, 120), revision=(10, 11)) -> str:
    payload = {
        "meta": {"id": source_id}, "type": type_, "namespace": namespace, "title": title,
        "comment": comment, "timestamp": timestamp, "user": user, "bot": bot, "minor": False,
        "length": {"old": length[0], "new": length[1]},
        "revision": {"old": revision[0], "new": revision[1]},
        "server_name": server, "wiki": wiki,
    }
    if source_id is None:
        del payload["meta"]
    return json.dumps(payload, ensure_ascii=False)


class EventRuleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = create_spark(master="local[2]", shuffle_partitions=2)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def frame(self, *payloads: str):
        return self.spark.createDataFrame([(p,) for p in payloads], "raw_json string")

    def test_parse_fields(self) -> None:
        new_page = json.loads(event("id-2", type_="new", length=(None, 50), revision=(None, 7)))
        del new_page["comment"]
        parsed = parse_events(self.frame(event(), json.dumps(new_page)))
        rows = {r.source_id: r for r in parsed.withColumn(
            "event_unix", F.col("event_time").cast("long")).collect()}
        self.assertEqual(rows["id-1"].event_unix, 1791845328)
        self.assertEqual(rows["id-1"].bytes_changed, 20)
        self.assertEqual(rows["id-1"].revision_new, 11)
        self.assertFalse(rows["id-1"].is_bot)
        self.assertEqual(rows["id-2"].bytes_changed, 50)
        self.assertEqual(rows["id-2"].comment, "")
        self.assertFalse(rows["id-2"].is_revert)

    def test_scope_and_batch_deduplication(self) -> None:
        kept = analysis_events(self.frame(
            event("id-1"), event("id-1"), event("id-new", type_="new"),
            "{invalid",
            event("id-commons", server="commons.wikimedia.org", wiki="commonswiki"),
            event("id-talk", namespace=1),
            event("id-log", type_="log"),
            event(None),
        ))
        self.assertEqual(sorted(r.source_id for r in kept.collect()), ["id-1", "id-new"])

    def test_hidden_user_is_kept(self) -> None:
        kept = analysis_events(self.frame(event("id-hidden", user=None))).collect()
        self.assertEqual(len(kept), 1)
        self.assertIsNone(kept[0].user)

    def test_revert_keywords(self) -> None:
        cases = [
            ("Undid revision [[Special:Diff/1379835436|1379835436]] by X", True),
            ("rv non-existent infobox field", True),
            ("Reverted edits by [[Special:Contribs/X|X]] to last version by Y", True),
            ("Rollback edit(s) by [[Special:Contributions/X|X]]", True),
            ("Änderungen von X auf die letzte Version von Y zurückgesetzt", True),
            ("Änderung 271288404 von X rückgängig gemacht", True),
            ("Annulation de la modification de X", True),
            ("Révocation d’une modification de X", True),
            ("回退[[Special:Contributions/X|X]]的编辑", True),
            ("[[WP:UNDO|撤销]]X的版本93863563", True),
            ("X による ID:111370962 の版を[[H:RV|取り消し]]", True),
            ("X による版を Y による版へ[[H:RV|巻き戻し]]", True),
            ("/* Opposition à la révolution (1980-1982) */", False),
            ("server migration, derivative work", False),
            ("added [[Category:Scientists from Cambridge]]", False),
            ("", False),
        ]
        frame = self.spark.createDataFrame(cases, "comment string, expected boolean")
        rows = frame.select("comment", "expected",
                            is_revert(F.lit("edit"), F.col("comment")).alias("actual")).collect()
        for row in rows:
            self.assertEqual(row.actual, row.expected, row.comment)
        new_page = self.spark.range(1).select(is_revert(F.lit("new"), F.lit("Undid revision 1")).alias("r"))
        self.assertFalse(new_page.first().r)

    def test_streaming_deduplicates_within_watermark(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "source"
            source.mkdir()
            (source / "events.txt").write_text(
                "\n".join([event("id-1"), event("id-1"), event("id-2"), event("id-commons", server="x.org")]),
                encoding="utf-8")
            stream = self.spark.readStream.text(str(source)).withColumnRenamed("value", "raw_json")
            query = (analysis_events(stream).select("source_id").writeStream.format("memory")
                     .queryName("wikipulse_events_dedup").outputMode("append")
                     .option("checkpointLocation", str(Path(tmp) / "checkpoint"))
                     .trigger(availableNow=True).start())
            query.awaitTermination()
            ids = sorted(r.source_id for r in self.spark.table("wikipulse_events_dedup").collect())
        self.assertEqual(ids, ["id-1", "id-2"])


if __name__ == "__main__":
    unittest.main()
