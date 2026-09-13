# Task 6 benchmark report

This report covers the full-data experiment run on 2026-09-09, identified by `20260909T141616Z-ce81d328`.

## Method

The experiment uses the same January-March 2024 Taxi files for both layouts. Of 9,554,778 input rows, 9,554,576 pass cleaning and 202 are rejected. S0 is unpartitioned; S1 is partitioned by `pickup_date` in New York local time. Both use B's cleaning logic, A's writer, `coalesce(8)` and Snappy compression. Only the partition setting changes. Checks confirm that accepted row counts, unique key counts, field types and results for all four queries agree across layouts.

Each layout is ingested twice: S0 followed by S1 in the first round, then S1 followed by S0 in the second. Timing starts with reading the raw data and includes cleaning and Delta commits for accepted and rejected rows. It excludes Spark startup, readback verification and metadata writes.

Queries use the tables written in the second round. Each query runs once as a warm-up on each layout, followed by three measured runs per layout in alternating order. The report gives median elapsed times, measured from SQL construction through completion of `collect()`. Result checks require exact equality for counts and use a relative tolerance of 1e-9 and an absolute tolerance of 1e-6 for averages.

Both layouts use the same configuration, with no Spark cache for the Taxi query input. Operating-system caching is uncontrolled. File counts and sizes come from the current Delta snapshot and exclude CRC files, logs, rejected-row tables and files from older versions.

## Environment

The experiment runs on Windows build 26200 with an i9-13900HX processor and approximately 32 GB of RAM. Software versions are Python 3.11.9, Java 21.0.7, Spark 4.2.0 and Delta 4.4.0. Spark uses `local[4]`, an actual JVM heap of 4 GiB, 128 shuffle partitions and a 10 MiB broadcast threshold. AQE and ANSI mode are enabled, and the session timezone is UTC.

## Results

Data sizes use decimal MB. For each layout, both ingestion rounds produce the same file count and data byte count. All timings below are medians; query rows show execution time, not the aggregate value returned.

| Metric | S0: unpartitioned | S1: partitioned by date |
| --- | ---: | ---: |
| Ingestion time (s) | 130.087 | 154.807 |
| Current data size (MB) | 1042.245 | 1044.463 |
| Current data file count | 8 | 728 |
| Trips per pickup borough (s) | 0.998 | 1.788 |
| Average trip duration per day (s) | 0.911 | 1.389 |
| Average fare per pickup borough (s) | 0.936 | 1.686 |
| Additional query: February 1-7 statistics (s) | 0.408 | 0.376 |

## Interpretation and limitations

In this run, S1 takes about 19% longer to ingest and 53% to 80% longer for the three full-data queries. All three queries need every date. Each of the eight write tasks produces a file for each day, giving S1 a total of 91 × 8 = 728 files, averaging about 1.43 MB each. The results are consistent with higher file-handling overhead, but the experiment does not measure each source of overhead separately. It therefore cannot establish how much of the timing difference comes from file count.

For the one-week query, S1's execution plan includes `PartitionFilters`, and its read schema no longer includes the physical date column. Partition values in the Delta log associate this interval with 7/91 dates, 56/728 files and 707,704 trips, matching the query count. S0's plan also includes date `PushedFilters`, so the unpartitioned layout benefits from filtering too. The median times differ by only about 32 ms. Three measurements are insufficient to establish a consistent advantage for S1.

S0 is a reasonable baseline for further experiments on the current full-data workload. Whether date partitioning helps depends on the date ranges queried and the resulting file sizes. The integrated table is wider than the Taxi table tested here and needs a separate layout evaluation. This experiment covers one machine and one data volume; performance at 20 times the volume or on a cluster remains untested.

## Reproduction and evidence

Run `python -m scripts.run_benchmark` in the repository's virtual environment to repeat the experiment. The [raw timings](benchmark_timings.csv) are stored with the code, and query definitions are in `src/dic_pipeline/benchmark.py`.

The local file `data/benchmark/<run_id>/results.json` records the configuration, input SHA-256 hashes, statistics for each round, all query results and raw timings. The `plans/` directory contains eight SQL and execution-plan files. These run artifacts are excluded from Git; teammates can generate them locally by following the README.
