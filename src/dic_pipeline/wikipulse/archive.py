"""Raw Kafka to Delta archive without analytical filtering."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
DELTA_PACKAGE = "io.delta:delta-spark_2.13:4.4.0"


def spark_session(app_name: str) -> SparkSession:
    """Create Spark 4.2/Delta 4.4 with its matching Kafka connector."""
    return (SparkSession.builder.appName(app_name)
            .config("spark.jars.packages", f"{KAFKA_PACKAGE},{DELTA_PACKAGE}")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())


def archive_columns(frame: DataFrame) -> DataFrame:
    """Keep every Kafka record, including invalid JSON and non-Wikipedia projects."""
    # Read only the contract headers so duplicate Kafka header keys remain intact in
    # ``kafka_headers`` instead of making the projection fail on a duplicate map key.
    def header_value(name: str):
        values = F.transform(
            F.filter(F.col("headers"), lambda header: header.key == F.lit(name)),
            lambda header: header.value,
        )
        return F.try_element_at(values, F.lit(-1))

    return frame.select(
                F.col("topic").alias("kafka_topic"),
                F.col("partition").alias("kafka_partition"),
                F.col("offset").alias("kafka_offset"),
                F.col("timestamp").alias("kafka_timestamp_utc"),
                F.col("timestampType").alias("kafka_timestamp_type"),
                F.col("key").alias("kafka_key"),
                F.col("value").alias("raw_value"),
                F.decode("value", "UTF-8").alias("raw_json"),
                F.decode(header_value("sse_id"), "UTF-8").alias("sse_id"),
                F.decode(header_value("sse_event_type"), "UTF-8").alias("sse_event_type"),
                F.decode(header_value("source_id"), "UTF-8").alias("source_id"),
                F.decode(header_value("source_event_unix"), "UTF-8")
                .try_cast("long").alias("source_event_unix"),
                F.decode(header_value("received_at_utc"), "UTF-8")
                .try_cast("timestamp").alias("received_at_utc"),
                F.col("headers").alias("kafka_headers"),
                F.to_date("timestamp").alias("ingest_date_utc"),
            )


def run_archive(spark: SparkSession, bootstrap: str, topic: str,
                output_path: str, checkpoint_path: str, starting_offsets: str = "earliest",
                trigger_interval: str = "10 seconds") -> None:
    """Resume from a persistent checkpoint; first start uses starting_offsets."""
    if not bootstrap or not topic or not output_path or not checkpoint_path:
        raise ValueError("bootstrap, topic, output_path and checkpoint_path are required")
    if output_path.rstrip("/") == checkpoint_path.rstrip("/"):
        raise ValueError("output_path and checkpoint_path must be separate")
    if starting_offsets not in {"earliest", "latest"} or not trigger_interval:
        raise ValueError("starting_offsets must be earliest/latest and trigger_interval is required")
    source = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", bootstrap)
              .option("subscribe", topic)
              .option("startingOffsets", starting_offsets)
              .option("includeHeaders", "true").load())
    query = (archive_columns(source).writeStream.format("delta")
             .outputMode("append").partitionBy("ingest_date_utc")
             .option("checkpointLocation", checkpoint_path)
             .trigger(processingTime=trigger_interval)
             .start(output_path))
    query.awaitTermination()
