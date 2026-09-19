# Week 2 design report: optimization strategy and trade-offs (role C)

This is role C's section of the Week 2 design report. Role A contributes the platform and data-product sections, role B the analytical requirements and query design. Measurements cited here come from run `20260919T143454Z-9d921a51`; the [benchmark report](w2_benchmark_report.md) documents the method and the full results.

## Strategy

The six analytical queries share one shape: they scan a wide table of 9,554,576 enriched trips and collapse it to between three and 773 result rows. That collapse ratio, four to five orders of magnitude, determined which optimizations were worth investigating and in what order.

We treated the canonical Spark SQL in `src/dic_pipeline/sql/` as the definition of a correct answer and every optimization as a claim that must be proven equivalent to it. The experiment harness therefore refuses to report a speedup until the optimized result matches the baseline exactly for counts and keys, and within a fixed tolerance for floating-point means. Where an optimized variant reads from a different table altogether — the product-backed rewrites — the harness compares against the canonical query, not merely against the other variant. This is why the product rewrites render their date filters, hour calendars and weather classification through the same `render_template` helper as the canonical queries: the two sides cannot drift apart through independent edits, because they share the generated text.

Each experiment changes exactly one factor. Both sides read the same pinned Delta snapshot, run under the same session settings except for the single setting under test, are warmed once, and are measured three times in alternating order.

## The four required techniques

**Partition pruning.** The integrated table is partitioned by `pickup_date`, but every query filters on `pickup_hour_utc` converted to a New York date. Delta cannot prune on a computed expression, so the baseline reads all 91 partitions. The optimized variant adds the equivalent bounds on `pickup_date` itself. The predicate is redundant in meaning — `pickup_date` is by construction the New York date of the pickup — so the result is unchanged, and the plan gains three `PartitionFilters` entries. One month of data yielded 1.25× on Q1 and 1.28× on Q6.

**Caching.** Q4 reads the filtered trips twice, once to enumerate zones and once for hourly demand, which made it the natural candidate. Q6 reads a single column once and served as the contrasting case. Caching the Q4 projection produced no measurable change while costing 4.59 s to build and 77.1 MB to hold; caching the Q6 projection gave 1.39×. We report both, including the failure, because the negative result is the more informative one: it locates the bottleneck.

**Broadcast join.** The canonical queries read the integrated table, where zone labels are already joined in, so there is no join left to optimize. We therefore rebuilt Q1 from the base tables — the standardized Taxi table joined to the 265-row zone lookup — which is the join the assignment describes. With automatic broadcasting and AQE both disabled the baseline is a genuine `SortMergeJoin`; the `BROADCAST` hint turns it into a `BroadcastHashJoin` and removes the shuffle of the large side, for 1.45×. Disabling both settings matters: without that, Spark broadcasts a 265-row table on its own and the experiment would have compared two identical plans.

**Adaptive Query Execution.** AQE gave the largest gain of the four techniques, 5.73× on Q4. Q4 crosses 259 zones with 2,183 weather hours and aggregates three times. With AQE off, each shuffle writes 128 partitions no matter how little data survives, and fixed per-partition overhead dominates. AQE coalesces them from runtime statistics, visible as 15 `AQEShuffleRead` nodes in the final plan. On Q6, a single aggregation over one column, the same setting gave 1.13×, within measurement noise.

## Data products as an optimization

The four materialized products are role A's deliverable, but they are also the largest performance lever, so we measured them on the same footing as the four techniques: six rewrites answering Q1 through Q6 from the products instead of the integrated table, each verified against the canonical result. Speedups ranged from 1.31× to 9.27×.

The rewrites are only valid within the constraints the products carry. `weather_impact_summary` is not keyed by time, so the Q4 rewrite is valid for the full coverage range only; a narrower range must go back to the canonical query. The Q4 rewrite also has to reconstruct the per-category denominator from the weather-hour calendar rather than from the product, because the product records only hours in which a zone had trips. Documenting those limits matters more than the speedup: a rewrite that is fast and quietly wrong for some ranges is worse than no rewrite.

## Trade-offs

**Materialization versus freshness and cost.** The products occupy 172,329 bytes against 1,058,134,566 bytes for the integrated table, an 0.016% storage overhead, which is negligible. The real cost is the 142.5 s full refresh and the staleness between refreshes. Running all six queries once saves 9.14 s, so a refresh pays for itself after roughly 16 complete rounds. The case for materializing rests on the reporting pattern — analysts re-run the same aggregates — not on any single query.

**Caching versus memory.** Caching only pays when scanning is the bottleneck. On this workload it usually is not, and a cache that holds 77.1 MB for no gain is a straightforward loss. We would enable caching per query on evidence, never as a default.

**Redundant predicates versus clarity.** The pruning variant states the date range twice, once semantically and once for the partition column. This is duplication that a future edit could desynchronize. We accept it because both predicates are generated together from one range parameter in `_trip_filter`, so they cannot be edited apart, and the harness would catch a divergence as a result mismatch.

**Tuning versus portability.** The broadcast experiment requires disabling automatic broadcasting to be meaningful, but the production default of 10 MiB is the right setting — it makes the optimization automatic. We changed the setting to measure the effect, not to recommend the change.

## Engineering decisions

Experiments are data, not code paths: an `Experiment` is a dataclass holding two `Variant` objects, and adding one means adding a definition rather than a branch. Plan evidence is extracted into structured fields — partition filters, join strategy, in-memory scans, final adaptive plans, shuffle reads — so the report cites facts parsed from the plan rather than impressions of it, while the full plan text is kept for audit.

Two findings changed the harness itself. Spark substitutes a cached plan into any later query containing the same logical subplan, which means an alternated order silently serves the "uncached" baseline from the cache; cache experiments therefore measure the baseline before the cache exists, and the deviation is recorded in the results file. Delta also keeps its own log-state RDDs cached, so reporting absolute RDD storage would have attributed Delta's memory to our cache; cache cost is reported as a before-and-after difference instead.

## What the measurements do not establish

Three runs per variant cannot separate small differences, and two baselines drifted across their three runs, so a single warm-up does not fully stabilize every query. Operating-system caching is uncontrolled. Everything here is one machine, one data volume and a local Spark master, where shuffle never crosses a network. The scaling recommendations in the benchmark report follow from the data's shape and from these measurements, but they are extrapolation, not measurement.
