# Week 2 Role B: Analytical Query Design

## Scope and source contract

Role B owns Q1–Q6, their statistical definitions, and small-fixture correctness tests. The queries run through `scripts/run_analytical_queries.py`. That entry point calls Role A's `register_analytics_inputs`, so every query reads the integrated table and standardized tables at the exact Delta versions published in `completed_integration.json`.

The analysis timezone is `America/New_York`. Optional query ranges are half-open local-date intervals: the start date is included and the end date is excluded. Demand always means pickup trip count. Missing environmental measurements are never replaced with zero; zero is used only for a known hour with no observed taxi trips.

## Frozen classifications

`configs/analytical_queries.json` is the analysis contract.

- Meteostat `coco` codes follow the provider's [published 1–27 definitions](https://dev.meteostat.net/formats.html#weather-condition-codes). The six reporting groups preserve the important distinctions: clear/fair, cloudy/overcast, fog, rain, sleet/snow, and storm/hail. Missing codes are reported separately rather than labelled as fair weather.
- PM2.5 is grouped into `0_to_5`, `over_5_to_10`, `over_10_to_15`, `over_15_to_25`, and `over_25` micrograms per cubic metre. These are descriptive analysis bands, not official AQI categories.
- Q4 requires at least two hours in each retained weather category and at least two retained categories for a zone. The threshold is configurable but fixed before a benchmark run.

## Query definitions

| ID | Grain | Main measures and correctness rules |
| --- | --- | --- |
| Q1 | local month × pickup zone | Trip count. Unknown zone labels remain a separate bucket; zone ID is retained. |
| Q2 | weather category | Total trips, non-null distance count, and average of non-null `trip_distance`. NYC in-scope unmatched weather and matched hours with no code are retained as `unmatched` and `missing_code` audit groups. |
| Q3 | PM2.5 band | Complete hourly calendar between the first and last in-scope trip. Air data uses the same two-level median as Week 1: median per site/hour, then median across sites. Reports hourly sample count, total and average hourly demand, mean PM2.5, and the overall Pearson correlation. Hours without PM2.5 appear as `missing_pm25` but are excluded from correlation; hours with PM2.5 but no trips count as zero demand. Association is not causation. |
| Q4 | pickup zone | Crosses every in-scope zone with each known weather hour, so zero-demand zone-hours are included. For each zone it compares category-level mean hourly demand and ranks the max-minus-min range. At least two valid weather categories are required. |
| Q5 | weekday × local clock hour | Builds a complete UTC-hour calendar, converts each actual hour to New York time, and averages demand by weekday/hour. `DENSE_RANK` preserves exact ties. Keeping the UTC hour before conversion prevents the repeated DST clock hour from being silently collapsed. |
| Q6 | local month | Trip count, previous-month count, absolute month-over-month change, and percentage change. The first month has null change; the supplied data covers only a short period, so this is descriptive rather than a seasonal model. |

## Output and ownership boundary

The SQL templates are under `src/dic_pipeline/sql/`; `src/dic_pipeline/queries.py` validates source views and renders only checked identifiers and values. Q1–Q6 do not materialize Delta products and do not implement caching, partition-pruning experiments, broadcast experiments, or AQE comparisons. Those responsibilities remain with Roles A and C.

Role A's four materialized products can support repeated reporting, but the six canonical answers remain the SQL definitions in this module. Any later product rewrite must be checked against these query results before it is treated as equivalent.
