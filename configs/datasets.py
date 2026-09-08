"""Dataset paths and ingestion settings."""

DATASETS = {
    "taxi_trips": {
        "path": "data/raw/yellow_tripdata_2024-*.parquet",
        "format": "parquet",
        "required_columns": [
            "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "PULocationID", "DOLocationID", "trip_distance", "fare_amount",
        ],
    },
    "weather": {
        "path": "data/raw/weather.csv",
        "format": "csv",
        "options": {"header": "true", "inferSchema": "true"},
        "required_columns": ["year", "month", "day", "hour", "temp"],
    },
    "air_quality": {
        "path": "data/raw/hourly_88101_2024.csv",
        "format": "csv",
        "options": {"header": "true", "inferSchema": "true"},
        "required_columns": [
            "State Code", "County Code", "Site Num", "Date GMT", "Time GMT",
            "Sample Measurement",
        ],
    },
    "taxi_zone_lookup": {
        "path": "data/raw/taxi_zone_lookup.csv",
        "format": "csv",
        "options": {"header": "true", "inferSchema": "true"},
        "required_columns": ["LocationID", "Borough", "Zone", "service_zone"],
    },
}

STANDARDIZED_ROOT = "data/delta/standardized"
REJECTED_ROOT = "data/delta/rejected"
METADATA_PATH = "data/delta/metadata/ingestion_runs"
SCHEMA_VERSION = "1.0"
