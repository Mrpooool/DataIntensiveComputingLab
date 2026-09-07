# Week 1 Data Catalog (Role B Working Artifact)

This catalog records facts verified against the supplied files. It is an engineering input to
the final report, not the report itself.

## Source inventory

| Dataset | Source | Verified size |
| --- | --- | ---: |
| Taxi Trips | `yellow_tripdata_2024-01/02/03.parquet` | 9,554,778 rows |
| Weather | `weather.csv` | 8,784 rows |
| Air Quality | `hourly_88101_2024.csv` | 8,139,551 US rows; 51,885 NYC-scope rows |
| Taxi Zone Lookup | `taxi_zone_lookup.csv` | 265 rows |

## Taxi Trips

| Catalog question | Verified answer |
| --- | --- |
| Primary entity | One recorded yellow-taxi trip. |
| Primary key | No natural trip identifier exists. `record_id` is a versioned SHA-256 fingerprint of all 19 source business fields (with standardized timestamp and numeric representations). Fully identical business rows are therefore treated as duplicates. |
| Join attributes | `pickup_location_id` and `dropoff_location_id` join to Taxi Zones; `pickup_hour_utc` joins to hourly environmental context. |
| Temporal attributes | Pickup and dropoff timestamps; derived `pickup_date`, `pickup_hour_utc`, and `trip_duration_seconds`. Source timestamps are timezone-naive and are interpreted as `America/New_York`. |
| Categorical attributes | Vendor, rate code, store-and-forward flag, payment type, pickup/dropoff location IDs. |
| Growth attributes | Trip rows and ingestion batches grow continuously; lookup codes may evolve more slowly. |

Verified observations: the three files contain 9,554,778 rows and one exact repeated row.
There are 180 rows with dropoff earlier than pickup, 19 pickups before 2024, and two pickups
at or after 2024-04-01 local time. Negative fare values occur and are retained with quality
flags because they can represent reversals/refunds. Zero-distance and extreme-distance trips
are also flagged rather than silently deleted.

## Weather

| Catalog question | Verified answer |
| --- | --- |
| Primary entity | One city-background weather record for one source hour. The file contains no station/location column. |
| Primary key | `(year, month, day, hour)`, standardized as `weather_hour_utc`; verified unique. |
| Join attributes | `weather_hour_utc` joins to Taxi `pickup_hour_utc`. |
| Temporal attributes | `year`, `month`, `day`, `hour`; derived source and UTC timestamps. |
| Categorical attributes | Weather condition code `coco`, cloud cover code `cldc`, and all `*_source` provenance fields. |
| Growth attributes | New hourly observations are expected over time. |

The file covers every nominal hour from 2024-01-01 00:00 through 2024-12-31 23:00 and has no
duplicate hour. `snwd` and `wpgt` are entirely missing; `prcp` is missing in 553 rows and
`coco` in six. Missing optional measurements do not reject the row. With no timezone metadata,
the current contract treats the source hour as UTC; this is an explicit assumption and is
configurable.

## Air Quality

| Catalog question | Verified answer |
| --- | --- |
| Primary entity | One hourly PM2.5 measurement from one EPA monitor. |
| Primary key | `(state_code, county_code, site_num, parameter_code, poc, air_quality_hour_utc, method_code)`; verified unique within NYC scope. |
| Join attributes | `air_quality_hour_utc` provides the temporal key. `site_id`, county, latitude and longitude preserve spatial provenance. C will aggregate monitor-hour values before joining to trips. |
| Temporal attributes | GMT date/time, local-standard date/time, and date of last change. GMT is the authoritative integration time. |
| Categorical attributes | State/county/site codes, parameter, POC, unit, qualifier, method type/code/name, datum. |
| Growth attributes | New monitor-hour measurements and new monitoring sites/methods grow over time. |

The source is a national EPA file with 8,139,551 rows, not a NYC-only dataset. NYC scope is
defined as New York State plus Bronx, Kings, New York, Queens and Richmond counties. The
supplied file contains 51,885 measurements from six sites in Bronx, Kings and Queens; it has no
measurements for New York or Richmond counties. All scoped measurements are parameter 88101
(`PM2.5 - Local Conditions`), unit `Micrograms/cubic meter (LC)`, and method type FEM. Values
range from 0.4 to 175.9. GMT minus the supplied local time is always five hours, confirming the
local field is local standard time rather than daylight-saving-aware New York wall time.

National rows outside the five-county scope are counted as `scope_excluded_count`; they are not
mislabelled as bad-quality rejected records. Non-empty EPA qualifiers are preserved and flagged.

## Taxi Zone Lookup

| Catalog question | Verified answer |
| --- | --- |
| Primary entity | One TLC taxi zone. |
| Primary key | `location_id`; values 1-265 are non-null and unique. |
| Join attributes | Taxi pickup and dropoff location IDs. |
| Temporal attributes | None in the supplied snapshot. |
| Categorical attributes | Borough, zone name and service zone. |
| Growth attributes | Slowly changing reference data; future versions may add or rename zones. |

Rows 264 and 265 intentionally represent `Unknown` and `Outside of NYC`. Their missing text
fields are normalized to those explicit labels instead of rejecting the lookup records.

## Verified preparation results

These results were produced by the Spark implementation against all supplied source rows.

| Dataset | Raw rows | Scope excluded | Accepted | Rejected | Key observations |
| --- | ---: | ---: | ---: | ---: | --- |
| Taxi | 9,554,778 | 0 | 9,554,576 | 202 | 180 reverse-time rows, 21 outside source period, 1 exact duplicate |
| Weather | 8,784 | 0 | 8,784 | 0 | 553 missing precipitation and 6 missing condition codes are optional flags |
| Air Quality | 8,139,551 | 8,087,666 | 51,885 | 0 | 136 scoped rows carry non-empty EPA qualifiers |
| Taxi Zones | 265 | 0 | 265 | 0 | Special IDs 264 and 265 normalized and retained |
