# Task 2: Design Your Storage Architecture

## Directory structure

```text
data/
  raw/
  delta/
    standardized/
      taxi/
      weather/
      air_quality/
      taxi_zones/
    rejected/
      taxi/
      weather/
      air_quality/
      taxi_zones/
    metadata/
      ingestion_runs/
    integrated/
      integrated_taxi_trips/
  benchmark/
    taxi_unpartitioned/
    taxi_partitioned_by_pickup_date/
```

- `raw` stores the original CSV and Parquet files without modifying them.
- `standardized` stores records that have been standardized and passed the
  data-quality checks.
- `rejected` stores records that failed validation together with their
  `error_reasons`.
- `metadata/ingestion_runs` stores the status and statistics of each ingestion.
- `integrated` stores the integrated Taxi trip table.
- `benchmark` stores the two Taxi storage layouts used in Task 6.

## Delta table organization

Each standardized, rejected, metadata, or integrated leaf directory is a
separate Delta table. For example, `data/delta/standardized/taxi` is the
standardized Taxi table.

A Delta table directory contains:

- `part-*.snappy.parquet`: the actual columnar data files;
- `_delta_log/`: the table schema, commit history, and list of active files.

The table must be read through its Delta directory:

```python
taxi = spark.read.format("delta").load("data/delta/standardized/taxi")
```

The current implementation generates:

- `standardized/taxi`
- `standardized/weather`
- `standardized/air_quality`
- `standardized/taxi_zones`
- `rejected/taxi`
- `rejected/weather`
- `rejected/air_quality`
- `rejected/taxi_zones`
- `metadata/ingestion_runs`

## Naming conventions

- Table names, directory names, and standardized column names use lowercase
  `snake_case`.
- UTC timestamp fields use the `_utc` suffix, such as
  `pickup_timestamp_utc`.
- original timestamp fields use the `_source` suffix, such as
  `pickup_timestamp_source`.
- The stable Taxi record key is named `record_id`.
- The Taxi Zone business key is named `location_id`.
- Audit fields use the fixed names `source_file`, `run_id`,
  `schema_version`, and `rule_version`.

## Partitioning strategy

| Table | Strategy | Reason |
| --- | --- | --- |
| Taxi standardized | Unpartitioned | Retained as the unpartitioned baseline for Task 6 |
| Weather | Unpartitioned | It has only 8,784 rows and is approximately 0.74 MB |
| Air Quality | Unpartitioned | The NYC standardized table has only 51,885 rows and is approximately 3.79 MB |
| Taxi Zones | Unpartitioned | It has only 265 rows and is a small lookup table |
| Rejected tables | Unpartitioned | The current rejected datasets are very small |
| Ingestion metadata | Unpartitioned | Each run adds only one record per dataset |
| Integrated Taxi Trips | Initially partitioned by `pickup_date` | The table is large, and date-range queries can use partition pruning |

Task 6 uses the following two Taxi strategies:

1. `data/benchmark/taxi_unpartitioned`: no partitioning;
2. `data/benchmark/taxi_partitioned_by_pickup_date`: partitioned by
   `pickup_date`.

The two experimental tables must use the same data, schema, columns, and
compression format. Their row counts and query results must also be identical.

## Which datasets should conceptually be treated as lookup tables?

`taxi_zones` should be treated as a lookup table. It uses `location_id` to map
Taxi pickup and dropoff locations to a borough, zone, and service zone. It has
only 265 records, changes slowly, and is repeatedly referenced by the Taxi fact
table.

Taxi, Weather, and Air Quality are time-growing fact or observation datasets,
not lookup tables.

## Which datasets should not be partitioned? Explain why.

- `taxi_zones`: the table is too small, so partitioning would only add
  directory and metadata overhead.
- `weather`: the complete year contains only 8,784 rows, making a full scan
  inexpensive.
- `air_quality`: the current NYC standardized table contains only 51,885 rows,
  and downstream processing needs to aggregate the complete table by hour.
- Rejected tables: the largest current rejected table contains only the 202
  rejected Taxi records.
- `ingestion_runs`: the number of ingestion records is small.
- Taxi standardized: it is retained as the unpartitioned baseline for Task 6.

## Which datasets require different partitioning strategies?

- The Taxi benchmark retains both an unpartitioned table and a table
  partitioned by `pickup_date`.
- Integrated Taxi Trips is initially partitioned by `pickup_date`.
- Weather, Air Quality, Taxi Zones, rejected tables, and metadata remain
  unpartitioned.

Taxi and Integrated Taxi Trips are much larger than the other tables and
contain temporal fields suitable for date filtering. Their time-based layouts
must therefore be evaluated separately, while small datasets do not need the
same strategy.

## Under what conditions does partitioning become harmful?

- The table itself is small.
- The partition column has high cardinality.
- Each partition contains little data, creating many small directories and
  files.
- Queries do not filter on the partition column.
- The data is skewed, making a small number of partitions disproportionately
  large.
- Each write must modify many partitions.

Under these conditions, partitioning increases file listing, metadata
management, and Spark task scheduling overhead without reducing the amount of
data read through partition pruning.

## If the total data volume increased by 20×, what changes would you make?

- Replace full overwrite operations with incremental batch writes.
- Re-evaluate monthly and daily partitioning using representative date-range
  queries.
- Target data files of approximately 128–256 MB and compact small files.
- Monitor table size, file count, data volume per partition, and data skew.
- Re-evaluate time partitioning when the Taxi table grows to approximately
  20 GB.
- Weather would be approximately 15 MB and NYC Air Quality approximately
  76 MB at the same growth rate, so they should not be partitioned solely
  because the total volume increased.
- If multiple years or nationwide Air Quality data are retained, consider
  `year/month` partitioning when date-filtered queries are common.
- Use `--output-root` to switch to HDFS or object storage and configure the
  corresponding Spark connector.

# Task 3: Build a Generic Ingestion Framework

## Implementation

```text
configs/datasets.json
  -> read_source()
  -> prepare()
  -> accepted + rejected + metrics
  -> write_delta()
  -> read back and verify row counts
  -> write_metadata()
```

Relevant files:

- `configs/datasets.json`: data paths, formats, reader options, versions, and
  rule settings;
- `src/dic_pipeline/schemas.py`: raw Spark schemas;
- `src/dic_pipeline/ingestion.py`: Spark setup, reading, Delta writing,
  read-back verification, and metadata;
- `src/dic_pipeline/preparation.py`: common standardization, validation, and
  deduplication flow;
- `src/dic_pipeline/transforms.py`: dataset-specific transformations;
- `src/dic_pipeline/validation.py`: dataset-specific quality rules;
- `scripts/run_ingestion.py`: common command-line entry point.

The assignment requirements are implemented as follows:

| Requirement | Implementation |
| --- | --- |
| Load CSV and Parquet | `read_source()` selects the Spark reader using `source_format` |
| Validate input schema | CSV uses `RAW_SCHEMAS`, and `prepare()` checks the required raw columns |
| Standardize column names | `standardize_column_names()` |
| Normalize timestamps and types | `transform_taxi()`, `transform_weather()`, and `transform_air_quality()` |
| Apply dataset-specific rules | `TRANSFORMERS` and `RULE_BUILDERS` |
| Perform data-quality checks | `prepare()` checks missing keys, invalid timestamps, invalid numbers, and duplicates |
| Store data as Delta | `write_delta()` |
| Generate ingestion metadata | `write_metadata()` |

## Which components are generic and reusable across all datasets?

- `create_spark()` consistently configures Delta, UTC, ANSI mode, and Spark
  resources.
- `read_source()` reads CSV, Parquet, and wildcard file paths according to
  configuration.
- `prepare()` consistently returns accepted records, rejected records, and
  metrics.
- `write_delta()` handles Delta paths, write modes, partition columns, and
  target file counts.
- `ingest_dataset()` performs reading, preparation, writing, read-back
  verification, and metadata generation.
- `write_metadata()` records execution results using a common schema.
- `run_ingestion.py` provides one CLI for every registered dataset.

These components do not contain the field meanings or business-range rules of
any particular dataset.

## Which components remain dataset-specific, and why?

- Source file names, file formats, and reader options.
- Raw schemas and required columns.
- Mappings from source fields to standardized fields.
- Timestamp source fields and time zones.
- Data types, units, valid ranges, and missing-value rules.
- Primary keys, stable fingerprints, and duplicate keys.
- Air Quality region, pollutant, and unit filters.
- A suitable output file count for each dataset.

These rules depend on field semantics. For example, Taxi timestamps are
interpreted in `America/New_York`, Weather uses UTC, and Air Quality uses
`Date GMT + Time GMT`. A negative `trip_distance` is invalid, while a negative
`fare_amount` may represent a refund. Therefore, one common rule cannot
correctly validate all numeric fields.

## How are transformation rules defined and maintained?

- Common settings are maintained in `configs/datasets.json`.
- Raw schemas are registered in `RAW_SCHEMAS`.
- Transformation functions are maintained in
  `src/dic_pipeline/transforms.py` and registered in `TRANSFORMERS`.
- Quality rules are maintained in `src/dic_pipeline/validation.py` and
  registered in `RULE_BUILDERS`.
- Duplicate keys, time zones, and valid timestamp ranges are maintained in the
  dataset configuration.

When a rule changes, `rule_version` is updated. When an incompatible output
schema change occurs, `schema_version` is updated. Dataset-specific rule
changes do not require modifications to the common reader, writer, or CLI.

## How are metadata managed?

`data/delta/metadata/ingestion_runs` is an unpartitioned Delta table. Each
dataset execution records:

- `run_id`
- `dataset`
- `started_at`
- `finished_at`
- `execution_seconds`
- `raw_input_count`
- `scope_excluded_count`
- `input_count`
- `accepted_count`
- `rejected_count`
- `duplicate_count`
- `schema_version`
- `rule_version`
- `status`
- `error_counts_json`
- `quality_flag_counts_json`
- `error_message`

After the accepted and rejected Delta tables are written, they are read back
and their row counts are verified. A `success` record is written only after
this verification succeeds; ordinary execution errors are recorded as
`failed`.

## How does the design reduce code duplication and simplify maintenance?

Reading, Spark configuration, preparation flow, Delta writing, output
verification, metadata generation, and CLI handling are each implemented once.
Each dataset maintains only its own configuration, schema, transformation, and
validation rules. Adding or changing one dataset does not require duplicating
the complete ingestion pipeline or modifying rules for other datasets.

## If the municipality adds 20 new datasets next year, what changes are required?

Each new dataset requires:

1. A new entry in `configs/datasets.json` containing its path, format, reader
   options, versions, and duplicate key.
2. A raw schema registered in `RAW_SCHEMAS`.
3. A dataset-specific transformer registered in `TRANSFORMERS`.
4. Dataset-specific validation rules registered in `RULE_BUILDERS`.
5. Tests for representative valid records and important boundary cases.

If Spark DataFrameReader already supports the format, such as CSV, Parquet,
JSON, or ORC, only `source_format` needs to be configured; no separate reader
file is required. The reading logic needs to be extended only when a format
requires a third-party connector or special decoding.
