# Week 2 benchmark report: analytical query optimization

This report covers the full-data experiment run on 2026-09-19, identified by `20260919T143454Z-9d921a51`. It measures thirteen paired experiments across four optimization techniques and six product-backed query rewrites.

## Method

Each experiment pairs one baseline SQL with one optimized variant and changes a single factor: an added predicate, a cached projection, a join hint, an AQE setting, or a different source table. Both variants read the same pinned Delta snapshot, registered through `completed_integration.json` (batch `563b32da`, integrated version 0, 9,554,576 trips).

Both variants are warmed once, then measured three times with `collect()` in alternating order, so position in the sequence does not favour one side. Reported figures are medians of the three measured runs. Before any timing is reported, the optimized result must equal the baseline result: exact equality for counts and keys, relative tolerance 1e-9 and absolute tolerance 1e-6 for floating-point means. Where neither variant is the canonical query — the broadcast experiment, which rebuilds Q1 from the base tables — the result is additionally compared against the canonical Q1. All thirteen experiments passed this check; no run reported a mismatch.

Cache experiments are the one departure from alternation. Spark substitutes a cached plan into any later query containing the same logical subplan, so an alternated order would silently serve the "uncached" variant from the cache. The baseline is therefore measured before the cache is built, and the optimized variant after. Cache build time is measured separately with `count()`, and cache memory is reported as the difference in RDD storage before and after the build, because Delta keeps its own log-state RDDs cached independently.

Partition-pruning experiments use the half-open local-date range 2024-02-01 to 2024-03-01, the same range on both sides. The optimized variant adds bounds on the `pickup_date` partition column alongside the existing filter on `pickup_hour_utc`. Because `pickup_date` is the New York date of the pickup, the added predicate is redundant in meaning and changes nothing in the result; it only gives Delta something to prune on.

Broadcast experiments run with `spark.sql.autoBroadcastJoinThreshold=-1` and AQE disabled on both sides, so the baseline is a genuine sort-merge join rather than an automatic broadcast, and AQE cannot convert it at runtime.

## Environment

Windows build 26200, i9-13900HX, approximately 32 GB RAM. Python 3.11.9, Java 21.0.7, Spark 4.2.0, Delta 4.4.0. Spark runs `local[4]` with a 4 GiB driver heap, 128 shuffle partitions, UTC session timezone and ANSI mode enabled. AQE and a 10 MiB broadcast threshold are the session defaults; individual experiments override them as described above.

## Results

Medians of three measured runs, in seconds.

| Experiment | Technique | Baseline | Optimized | Speedup |
| --- | --- | ---: | ---: | ---: |
| `q1_partition_pruning` | partition pruning | 1.84 | 1.47 | 1.25× |
| `q6_partition_pruning` | partition pruning | 0.81 | 0.63 | 1.28× |
| `q4_cache_projection` | caching | 4.87 | 4.86 | 1.00× |
| `q6_cache_projection` | caching | 1.44 | 1.03 | 1.39× |
| `q1_broadcast_zones` | broadcast join | 8.05 | 5.56 | 1.45× |
| `q4_aqe` | AQE | 28.15 | 4.91 | 5.73× |
| `q6_aqe` | AQE | 1.45 | 1.28 | 1.13× |
| `q1_product_taxi_zone_statistics` | data product | 4.03 | 0.43 | 9.27× |
| `q2_product_weather_impact_summary` | data product | 0.66 | 0.33 | 1.99× |
| `q3_product_air_quality_impact_summary` | data product | 1.54 | 1.18 | 1.31× |
| `q4_product_weather_impact_summary` | data product | 4.38 | 0.84 | 5.21× |
| `q5_product_daily_mobility_summary` | data product | 0.94 | 0.32 | 2.99× |
| `q6_product_daily_mobility_summary` | data product | 1.06 | 0.37 | 2.89× |

Plan changes confirm that each optimization took effect. The pruning variants add three `PartitionFilters` entries where the baselines have none. The broadcast baseline plan contains `SortMergeJoin` and the optimized plan `BroadcastHashJoin`. Both cache variants show `InMemoryTableScan` only on the optimized side. The AQE variants show `isFinalPlan=true` and `AQEShuffleRead` nodes only when AQE is enabled. Exchange counts are not compared across AQE on and off, because the adaptive plan string embeds sub-plans and the counts are not like for like.

Caching cost: the Q4 five-column projection takes 4.59 s to build and holds 77.1 MB across 11 partitions; the Q6 single-column projection takes 0.76 s and holds 19.5 MB. Both were released after the experiment.

Data product storage: the four products together occupy 172,329 bytes in four files, against 1,058,134,566 bytes in 91 files for the integrated table — an 0.016% storage overhead. A full refresh of all four costs 142.5 s (48.7, 33.5, 35.1 and 25.3 s respectively).

## Interpretation

**Largest improvement.** Materializing data products gives the largest single speedup, 9.27× for Q1. Among the four required techniques, AQE on Q4 is the largest at 5.73×. Q4 crosses 259 zones with 2,183 weather hours and then aggregates three times; with AQE disabled, every shuffle writes 128 partitions regardless of how little data survives the aggregation, and the fixed per-partition overhead dominates. AQE coalesces those partitions from runtime statistics, which is visible as 15 `AQEShuffleRead` nodes in the final plan.

**Little or no effect.** Caching the Q4 projection produced no measurable change, 1.00×, and cost 4.59 s to build and 77.1 MB to hold. Q4 does read the trips twice, which is what motivated the experiment, but scanning is not where its time goes: the cross join and the three aggregations are. Caching removes a cost that was already small relative to the shuffle work. Partition pruning also gave modest gains, 1.25× and 1.28×, for a related reason — Parquet column pruning already restricts these queries to one or two narrow columns, so skipping 62 of 91 date partitions removes a read that was inexpensive to begin with. Both results argue that on this workload the bottleneck is shuffle and aggregation, not I/O.

**Queries that remain expensive.** Q4 is the most expensive query even when optimized, at 4.91 s with AQE and 0.84 s only when answered from a product. Q1 rebuilt from the base tables still takes 5.56 s with the broadcast hint, because the join precedes the aggregation and nine million rows must be tagged with a zone before they collapse to 773 groups. Q3 gains least from its product, 1.31×, because the product replaces only the trip-side aggregation; the two-level PM2.5 median over the standardized air-quality table is untouched and dominates what remains.

**Data characteristics that explain these results.** The decisive property is the collapse ratio. Nine and a half million trips reduce to 2,183 distinct hours, 265 zones, three months and six weather categories. Every analytical answer is four to five orders of magnitude smaller than its input, which is precisely the shape that rewards materialization and rewards coalescing shuffle partitions, and which leaves little for scan-level optimizations to recover. The zone lookup has 265 rows, so broadcasting it is unambiguously correct once automatic broadcasting is disabled. The 91 date partitions average about 11 MB, large enough that pruning a month of them is worth something but small enough that the saving is modest.

**Amortizing the products.** The product speedups compare query time only and exclude the 142.5 s refresh. Running all six queries once saves 9.14 s against the canonical versions, so a full refresh pays for itself after roughly 16 complete rounds. Individual products differ sharply: `taxi_zone_statistics` breaks even for Q1 after about 9 runs, whereas `daily_mobility_summary` needs about 37 rounds of Q5 and Q6 together. Materialization is justified here by the reporting pattern — analysts re-run the same aggregates repeatedly — and by the negligible storage cost, not by a single-query win.

**Scaling to ten cities.** At ten times the data the balance would shift. Partition pruning becomes more valuable as partitions grow past the point where per-file overhead dominates, and a city column would be the natural first partition key, with date second. The broadcast of a per-city zone lookup stays correct, since ten cities of 265 zones is still trivially small. AQE's advantage should persist or grow, because the gap between 128 fixed partitions and the true post-aggregation size widens. Caching remains unattractive for single-pass queries but becomes worth revisiting for Q4-style repeated scans if memory per executor grows with the data. The products would need a city dimension and, at that volume, incremental rather than full refresh — a full rebuild scaling linearly from 142.5 s would approach half an hour. The 0.016% storage overhead leaves ample room to materialize more aggregates per city.

## Limitations

Three measured runs per variant cannot establish small differences. The 1.00× cache result and the 1.13× Q6 AQE result are within run-to-run variance and should be read as "no clear effect", not as precise ratios. Operating-system caching is uncontrolled; the warm-up absorbs cold reads, but later runs may still benefit unevenly. Two baselines show a downward trend across their three runs — `q1_broadcast_zones` at 10.13, 8.05, 7.27 s and `q4_aqe` at 20.01, 28.15, 28.45 s in the other direction — which indicates that a single warm-up does not fully stabilize these queries. All figures come from one machine, one data volume and a local Spark master; cluster behaviour, where shuffle crosses the network, is untested.

## Reproduction and evidence

Run `python -m scripts.run_query_benchmark` in the project virtual environment; `--list` prints the experiment names and `--experiment` selects a subset. The run writes `data/benchmark/w2/<run_id>/results.json` with the pinned snapshot, environment, method, per-experiment medians, every raw sample, extracted plan facts, cache cost and product storage. The `sql/` directory holds the exact text submitted for each variant and `plans/` holds `EXPLAIN FORMATTED` plus the executed plan for each. The [raw timings](w2_benchmark_timings.csv) are stored with the code. Run artifacts are excluded from Git and can be regenerated locally.

Experiment definitions live in `src/dic_pipeline/query_benchmark.py`; product-backed rewrites are in `src/dic_pipeline/sql/products/`. Every rewrite renders its date filter, hour calendar and classification expressions through the same `render_template` helper as the canonical queries, so the two sides cannot drift apart in wording.
