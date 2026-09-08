from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


TAXI_RAW_SCHEMA = StructType(
    [
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", LongType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", LongType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("payment_type", LongType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("Airport_fee", DoubleType(), True),
    ]
)

WEATHER_RAW_SCHEMA = StructType(
    [
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("hour", IntegerType(), True),
        StructField("temp", DoubleType(), True),
        StructField("temp_source", StringType(), True),
        StructField("rhum", DoubleType(), True),
        StructField("rhum_source", StringType(), True),
        StructField("prcp", DoubleType(), True),
        StructField("prcp_source", StringType(), True),
        StructField("snwd", DoubleType(), True),
        StructField("snwd_source", StringType(), True),
        StructField("wdir", DoubleType(), True),
        StructField("wdir_source", StringType(), True),
        StructField("wspd", DoubleType(), True),
        StructField("wspd_source", StringType(), True),
        StructField("wpgt", DoubleType(), True),
        StructField("wpgt_source", StringType(), True),
        StructField("pres", DoubleType(), True),
        StructField("pres_source", StringType(), True),
        StructField("cldc", IntegerType(), True),
        StructField("cldc_source", StringType(), True),
        StructField("coco", IntegerType(), True),
        StructField("coco_source", StringType(), True),
    ]
)

AIR_QUALITY_RAW_SCHEMA = StructType(
    [
        StructField("State Code", StringType(), True),
        StructField("County Code", StringType(), True),
        StructField("Site Num", StringType(), True),
        StructField("Parameter Code", StringType(), True),
        StructField("POC", IntegerType(), True),
        StructField("Latitude", DoubleType(), True),
        StructField("Longitude", DoubleType(), True),
        StructField("Datum", StringType(), True),
        StructField("Parameter Name", StringType(), True),
        StructField("Date Local", StringType(), True),
        StructField("Time Local", StringType(), True),
        StructField("Date GMT", StringType(), True),
        StructField("Time GMT", StringType(), True),
        StructField("Sample Measurement", DoubleType(), True),
        StructField("Units of Measure", StringType(), True),
        StructField("MDL", DoubleType(), True),
        StructField("Uncertainty", DoubleType(), True),
        StructField("Qualifier", StringType(), True),
        StructField("Method Type", StringType(), True),
        StructField("Method Code", StringType(), True),
        StructField("Method Name", StringType(), True),
        StructField("State Name", StringType(), True),
        StructField("County Name", StringType(), True),
        StructField("Date of Last Change", StringType(), True),
    ]
)

TAXI_ZONES_RAW_SCHEMA = StructType(
    [
        StructField("LocationID", IntegerType(), True),
        StructField("Borough", StringType(), True),
        StructField("Zone", StringType(), True),
        StructField("service_zone", StringType(), True),
    ]
)

RAW_SCHEMAS = {
    "taxi": TAXI_RAW_SCHEMA,
    "weather": WEATHER_RAW_SCHEMA,
    "air_quality": AIR_QUALITY_RAW_SCHEMA,
    "taxi_zones": TAXI_ZONES_RAW_SCHEMA,
}

REQUIRED_RAW_COLUMNS = {
    dataset: tuple(field.name for field in schema.fields)
    for dataset, schema in RAW_SCHEMAS.items()
}
