"""Inspect the raw WikiPulse Delta archive after a capture or replay run."""

import argparse
import json

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from dic_pipeline.wikipulse.archive import spark_session


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", default="hdfs://namenode:8020/wikipulse/raw_events")
    parser.add_argument("--version", type=int)
    args = parser.parse_args()

    spark = spark_session("WikiPulse raw archive inspection")
    reader = spark.read.format("delta")
    if args.version is not None:
        reader = reader.option("versionAsOf", args.version)
    frame = reader.load(args.archive)
    key = F.struct("kafka_topic", "kafka_partition", "kafka_offset")
    server_name = F.get_json_object("raw_json", "$.server_name")
    event_type = F.get_json_object("raw_json", "$.type")
    namespace = F.get_json_object("raw_json", "$.namespace").try_cast("int")
    article = (server_name.endswith(".wikipedia.org")
               & event_type.isin("edit", "new") & (namespace == 0))
    field_paths = {
        "wiki": "$.wiki",
        "title": "$.title",
        "user": "$.user",
        "bot": "$.bot",
        "comment": "$.comment",
        "length": "$.length.new",
        "revision": "$.revision.new",
    }
    result = frame.agg(
        F.count("*").alias("rows"),
        F.countDistinct(key).alias("physical_keys"),
        F.sum(F.length("raw_value")).alias("raw_payload_bytes"),
        F.count(F.when(server_name.endswith(".wikipedia.org"), 1))
        .alias("wikipedia_project_rows"),
        F.count(F.when(article, 1))
        .alias("wikipedia_article_edit_rows"),
        F.count(F.when(F.col("source_id").isNotNull() & (F.col("source_id") != ""), 1))
        .alias("rows_with_source_id"),
        F.countDistinct(F.when(F.col("source_id").isNotNull() & (F.col("source_id") != ""),
                               F.col("source_id"))).alias("distinct_source_ids"),
        F.count(F.when(F.col("raw_value") == F.lit(b"{wikipulse-malformed-smoke"), 1))
        .alias("malformed_smoke_rows"),
        F.min("kafka_timestamp_utc").alias("first_kafka_timestamp_utc"),
        F.max("kafka_timestamp_utc").alias("last_kafka_timestamp_utc"),
        F.countDistinct("kafka_topic").alias("topics"),
        F.count(F.when(F.col("source_event_unix").isNotNull(), 1))
        .alias("rows_with_source_event_time"),
        F.count(F.when(article & F.col("source_event_unix").isNotNull(), 1))
        .alias("article_rows_with_source_event_time"),
        F.countDistinct(F.when(article & F.col("source_id").isNotNull()
                               & (F.col("source_id") != ""), F.col("source_id")))
        .alias("article_distinct_source_ids"),
        F.count(F.when(article & F.col("source_id").isNotNull()
                       & (F.col("source_id") != ""), 1))
        .alias("article_rows_with_source_id"),
        F.min(F.when(article, F.to_timestamp(F.from_unixtime("source_event_unix"))))
        .alias("first_article_event_time_utc"),
        F.max(F.when(article, F.to_timestamp(F.from_unixtime("source_event_unix"))))
        .alias("last_article_event_time_utc"),
        *[
            F.count(F.when(article & F.get_json_object("raw_json", path).isNotNull(), 1))
            .alias(f"article_rows_with_{name}")
            for name, path in field_paths.items()
        ],
    ).first().asDict()
    result["archive"] = args.archive
    result["duplicate_source_id_rows"] = result["rows_with_source_id"] - result["distinct_source_ids"]
    result["article_duplicate_source_id_rows"] = (
        result["article_rows_with_source_id"] - result["article_distinct_source_ids"]
    )
    result["version"] = args.version if args.version is not None else (
        DeltaTable.forPath(spark, args.archive).history(1).first()["version"]
    )
    print(json.dumps(result, default=str, sort_keys=True))
    spark.stop()


if __name__ == "__main__":
    main()
