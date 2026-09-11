# Week 1: Urban data integration platform

## 1. Purpose and data catalog

This project combines four separately supplied datasets into standardized Delta tables and an analytical table with one row per taxi trip. Week 1 runs as a reproducible full batch on a local machine. A handles reusable ingestion and storage (Tasks 2-3), B defines the data catalog and cleaning contract (Tasks 1 and 4), and C handles integration and storage experiments (Tasks 5-6).

### Taxi trips

Each row describes one yellow-taxi trip. The supplied January-March 2024 files contain 9,554,778 rows but no trip ID. We therefore identify records with a SHA-256 fingerprint of 19 business fields, formatting numeric measurements to six decimal places. Pickup and dropoff location IDs link trips to zones, while the pickup UTC hour links them to environmental observations. Time fields include pickup and dropoff timestamps, the local pickup date and trip duration. Vendor, payment, rate and location codes are categorical fields. Both trip records and ingestion batches accumulate over time.

### Weather

The weather file contains 8,784 hourly background-weather records for 2024. The source year, month, day and hour form a unique key, represented as weather_hour_utc for joins. Condition and cloud codes, along with measurement-source labels, are categorical fields. The dataset grows as more hours are recorded. Because the file supplies no city, station or timezone metadata, we assume that it describes NYC background weather in UTC. A high match rate with taxi trips cannot verify either assumption.

### Air quality

Each air-quality row records an hourly PM2.5 observation from an EPA monitor. Its key combines state, county, site, pollutant, POC, GMT hour and method code. Joins use GMT hour; site, county and location fields preserve the observation's spatial context. Pollutant, method, qualifier and site codes are categorical fields. The dataset can grow through additional monitor-hours or stations. Of the national file's 8,139,551 rows, 51,885 fall within the agreed NYC five-county scope. These records come from six sites in Bronx, Kings and Queens.

### Taxi zone lookup

The lookup contains one row per taxi zone, keyed by location_id (1-265). Both pickup and dropoff joins use this ID. Borough, zone and service-zone labels are categorical fields; the supplied snapshot has no time field. Changes are infrequent and consist of additions or renaming. IDs 264 and 265 represent Unknown and Outside of NYC. We retain all 265 rows.

The full field inventory and verified observations are recorded in docs/data_catalog.md and docs/data_contract.md.

---

## 2. Storage and reusable ingestion

### Storage layout

Original files remain in data/raw/. Each dataset directory under data/delta/standardized/ and data/delta/rejected/ holds a separate Delta table. Execution records go to metadata/ingestion_runs, and enriched trips go to integrated/integrated_taxi_trips. Each experiment writes to fresh data/benchmark/run_id/round_n/S0 or S1 directories. Column and table names use snake_case, with the _utc suffix identifying UTC timestamps. Each Delta table consists of Parquet files and a transaction log; readers use the table directory.

Taxi Zones serves as the lookup table. Weather, NYC Air, Zones, rejected rows and metadata are small enough to remain unpartitioned. Standardized Taxi also stays unpartitioned as the baseline for Task 6, which compares it with daily partitions using the same inputs and writer settings. The integrated table initially uses New York pickup_date partitions, a choice that still needs evaluation.

Partitioning can add overhead when tables or partitions are small, data is skewed, or queries scan every date. If data volume grows 20 times, we would need to compare daily and monthly layouts, reconsider target file sizes, compact small files and introduce incremental writes. Small reference tables could remain unpartitioned as Taxi grows. The current implementation uses local storage; moving to distributed storage would also require path handling that supports URIs and the relevant connectors.

### Common pipeline

Configuration -> read_source() -> prepare() -> accepted/rejected/metrics -> write_delta() -> readback verification -> ingestion metadata.

The reader supports CSV and Parquet. It validates CSV headers against explicit schemas and checks that required source columns are present. prepare() then applies the registered transformations and validation rules, classifies duplicates and returns accepted rows, rejected rows and metrics in a common format. Intermediate results use a disk-only cache because the wide Taxi audit rows can exhaust the local JVM heap. The pipeline releases this cache after writing.

configs/datasets.json holds source paths, versions, timezones and scope. Each dataset has its own schema, transformer and validation rules to reflect its field meanings, while orchestration, writing and metadata handling are shared. Adding twenty datasets would require registering their configurations, schemas, transformers and validators, adding them to CLI batch processing, and writing tests. Datasets with different join requirements would also need their own integration rules.

Run metadata includes counts, exclusions, rejections, duplicate and quality codes, versions, UTC execution times and status. Once all four tables succeed, the pipeline publishes completed_batch.json with their Delta versions. Integration reads these recorded versions. Rerunning a single table invalidates the completion file, and this local workflow assumes one ingestion writer.

---

## 3. Common model and quality rules

### Naming, types and time

Column names are normalized to snake_case; the pipeline rejects names that collide after normalization. schemas.py declares CSV types, while Taxi keeps the types supplied in Parquet. Standardized times use Spark timestamp values in a UTC session. The original times and source fields remain available for audit. Every accepted and rejected row includes source_file, run_id, schema_version and rule_version. Rejected rows also include raw_record_json and error_reasons.

For Taxi, the PU/DO fields become pickup_location_id and dropoff_location_id. Pickup and dropoff timestamps are interpreted as New York local times and converted to UTC. pickup_date keeps the New York calendar date, while pickup_hour_utc stores the UTC timestamp truncated to the hour. Trip duration is calculated between the converted instants, accounting for daylight-saving changes. Other business values remain unchanged. The fingerprint excludes batch and file information, so identical standardized business records count as duplicates.

For Weather, the pipeline builds a timestamp from the year, month, day and hour fields and applies the configured timezone. It preserves measurements and their source labels without filling gaps or converting units. The supplied metadata is insufficient to verify the units independently. snwd and wpgt are entirely missing, and optional null values remain null.

For Air, the pipeline first filters the agreed state and counties while preserving the national raw file. GMT date and time determine the observation timestamp. Local standard date and time are parsed separately, without applying New York daylight-saving time. Sample Measurement and Units of Measure are renamed. State, county and site codes are padded to 2, 3 and 4 characters respectively; parameter and method codes are stored as strings, and blank qualifiers become null. The pipeline derives site and monitor IDs and the compound fingerprint, preserving the remaining source fields.

For Zones, the pipeline trims text and assigns the explicit special labels to borough, zone and service_zone for IDs 264 and 265. Hashing the zone ID produces record_id. docs/data_contract.md lists all transformations.

### Rejected rows and quality flags

The pipeline rejects rows with missing keys, invalid required times, negative distances or passenger counts, invalid zone IDs, or weather values outside configured limits. It also rejects NaN/Infinity measurements, incorrect Air pollutants or units, and duplicate keys. Taxi pickups must fall within the supplied January-March period, and dropoff cannot precede pickup. When records share a key, selection prefers a valid row, then uses the source file and raw JSON to break ties deterministically.

Potential refunds, trips with zero or extreme distance or duration, optional weather gaps and EPA qualifiers receive quality flags and remain in the data. Air records outside NYC are counted as scope exclusions separately from quality failures. Within scope, input = accepted + rejected; for Air, national input = excluded + scoped input.

---

## 4. Integration, validation and trade-offs

### One row per accepted trip

Two left joins add pickup and dropoff zone and borough names to Taxi. Both use the broadcast 265-row zone lookup. Trips remain in the result when a location is missing or has a special label. Before each join, the pipeline checks that the right-hand keys are unique so duplicate lookup keys cannot multiply trip rows.

Weather already has one row per hour. Air uses the verified PM2.5 combination: pollutant 88101, Micrograms/cubic meter (LC), FEM method 636. The pipeline first takes the median for each site and hour, then the median across available sites in that hour. This gives each site equal weight even when monitor counts differ. It retains site counts and flagged-observation counts, without fixing the number of sites at six. Supporting incompatible pollutant, unit or method combinations would require a revised contract.

Weather and Air are left-joined on pickup_hour_utc for trips picked up in NYC's five boroughs. Trips with missing hours, unknown locations or pickups outside the city keep null environmental values. The result retains Taxi fields and adds prefixed environmental fields, scope flags and match flags. Before writing the table in pickup_date partitions, integration checks the row count and unique record count. It then reads the output back to check the written row count.

### Verified data and testing

Full-data preparation accepted 9,554,576 Taxi rows and rejected 202. It also accepted 8,784 Weather rows, 51,885 Air rows and all 265 Zones rows. The Air scope filter excluded 8,087,666 rows from the national file. Integration preserved all 9,554,576 unique trips. Weather and Air hours matched all 9,517,007 trips within NYC, giving 100% coverage within scope and about 99.61% across all trips. Measurement completeness is reported separately, including the missing snow and gust measurements.

Small Spark/Delta regression fixtures test row classification, malformed times, duplicate selection, special locations, cross-midnight and DST conversion, missing observations, incompatible Air combinations, trip preservation during joins and Delta readback. These targeted tests run alongside full-data verification.

### Performance and limitations

Task 6 measures ingestion from raw files to Delta, current data size and file count, and three required aggregations: trips per pickup borough, average duration per local date and average fare_amount per pickup borough. A one-week query tests partition pruning. Ingestion rounds reverse the layout order. After warm-up, queries run repeatedly in alternating layout order, with their results checked for equality. Measurements and limitations appear in docs/benchmark_report.md; the architecture diagram is in docs/architecture.md.

The environmental summaries describe conditions across NYC and cannot resolve individual streets. The assumed weather location and timezone remain unverified despite the high match rates. Same-hour aggregates may include observations made after pickup, so prediction tasks need a time-leakage review. Testing one data volume on one local machine also leaves cluster performance and scaling to 20 times the data unverified. Work in later weeks should evaluate file compaction, date layouts, caching, AQE and incremental processing separately.
