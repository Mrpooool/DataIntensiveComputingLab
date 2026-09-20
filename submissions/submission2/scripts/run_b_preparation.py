from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession

from dic_pipeline.contracts import load_dataset_config
from dic_pipeline.preparation import prepare
from dic_pipeline.schemas import AIR_QUALITY_RAW_SCHEMA, TAXI_ZONES_RAW_SCHEMA, WEATHER_RAW_SCHEMA


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("dic-role-b-validation")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )


def read_source(spark: SparkSession, dataset: str, data_dir: Path):
    if dataset == "taxi":
        paths = sorted(data_dir.glob("yellow_tripdata_2024-*.parquet"))
        if not paths:
            raise FileNotFoundError(f"No Taxi Parquet files found in {data_dir}")
        return spark.read.parquet(*[str(path) for path in paths])
    if dataset == "weather":
        return (
            spark.read.option("header", True)
            .schema(WEATHER_RAW_SCHEMA)
            .csv(str(data_dir / "weather.csv"))
        )
    if dataset == "air_quality":
        return (
            spark.read.option("header", True)
            .schema(AIR_QUALITY_RAW_SCHEMA)
            .csv(str(data_dir / "hourly_88101_2024.csv"))
        )
    if dataset == "taxi_zones":
        return (
            spark.read.option("header", True)
            .schema(TAXI_ZONES_RAW_SCHEMA)
            .csv(str(data_dir / "taxi_zone_lookup.csv"))
        )
    raise KeyError(dataset)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run Role B transformations without writing Delta"
    )
    parser.add_argument(
        "dataset",
        choices=["taxi", "weather", "air_quality", "taxi_zones", "all"],
    )
    parser.add_argument("--data-dir", type=Path, required=True)
    args = parser.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    datasets = (
        ["taxi", "weather", "air_quality", "taxi_zones"]
        if args.dataset == "all"
        else [args.dataset]
    )
    try:
        for dataset in datasets:
            raw = read_source(spark, dataset, args.data_dir)
            result = prepare(raw, load_dataset_config(dataset))
            print(json.dumps(result.metrics, indent=2, sort_keys=True))
            result.release()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
