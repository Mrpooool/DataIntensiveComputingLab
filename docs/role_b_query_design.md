# Week 2 Role B: Analytical Query Design

## Scope and source contract

Role B owns Q1–Q6, their statistical definitions, and small-fixture correctness tests. The queries run through `scripts/run_analytical_queries.py`. That entry point calls Role A's `register_analytics_inputs`, so every query reads the integrated table and standardized tables at the exact Delta versions published in `completed_integration.json`.

The analysis timezone is `America/New_York`. Optional query ranges are half-open local-date intervals: the start date is included and the end date is excluded. Demand always means pickup trip count. Missing environmental measurements are never replaced with zero; zero is used only for a known hour with no observed taxi trips.

## Frozen classifications

`configs/analytical_queries.json` is the analysis contract.

- Meteostat `coco` codes follow the provider's [published 1–27 definitions](https://dev.meteostat.net/formats.html#weather-condition-codes). The six reporting groups preserve the important distinctions: clear/fair, cloudy/overcast, fog, rain, sleet/snow, and storm/hail. Missing codes are reported separately rather than labelled as fair weather.
- PM2.5 is grouped into `0_to_5`, `over_5_to_10`, `over_10_to_15`, `over_15_to_25`, and `over_25` micrograms per cubic metre. These are descriptive analysis bands, not official AQI categories.
- Q4 requires at least two hours in each retained weather category and at least two retained categories for a zone. The threshold is configurable but fixed before a benchmark run.
- The hourly calendars in Q3-Q5 cover the requested local-date range clipped to the validated Taxi pickup window in `configs/datasets.json` (`valid_pickup_start_utc` to `valid_pickup_end_utc_exclusive`, currently 2024-01-01 05:00Z to 2024-04-01 04:00Z). Ingestion rejects trips outside that window, so an hour inside it with no trips is genuine zero demand, while hours outside it are never invented. Calendar hours stay in UTC; a request that lies entirely outside the window returns no rows. The bounds are not derived from the first and last observed trip, which would silently drop zero-demand hours at the edges of a period.

## Query definitions

| ID | Grain | Main measures and correctness rules |
| --- | --- | --- |
| Q1 | local month × pickup zone | Trip count. Unknown zone labels remain a separate bucket; zone ID is retained. |
| Q2 | weather category | Total trips, non-null distance count, and average of non-null `trip_distance`. NYC in-scope unmatched weather and matched hours with no code are retained as `unmatched` and `missing_code` audit groups. |
| Q3 | PM2.5 band | Complete UTC hourly calendar for the requested range within validated coverage (see above). Air data uses the same two-level median as Week 1: median per site/hour, then median across sites. Reports hourly sample count, total and average hourly demand, mean PM2.5, and the overall Pearson correlation. Hours without PM2.5 appear as `missing_pm25` but are excluded from correlation; hours with PM2.5 but no trips count as zero demand. Association is not causation. |
| Q4 | pickup zone | Crosses every in-scope zone with each known weather hour inside the same calendar, so zero-demand zone-hours are included even after the last trip. For each zone it compares category-level mean hourly demand and ranks the max-minus-min range. At least two valid weather categories are required. |
| Q5 | weekday × local clock hour | Builds the same complete UTC-hour calendar, converts each actual hour to New York time, and averages demand by weekday/hour, so leading and trailing zero-demand hours count. `DENSE_RANK` preserves exact ties. Keeping the UTC hour before conversion prevents the repeated DST clock hour from being silently collapsed. |
| Q6 | local month | Trip count, previous-month count, absolute month-over-month change, and percentage change. The first month has null change; the supplied data covers only a short period, so this is descriptive rather than a seasonal model. |

## Output and ownership boundary

The SQL templates are under `src/dic_pipeline/sql/`; `src/dic_pipeline/queries.py` validates source views and renders only checked identifiers and values. Q1–Q6 do not materialize Delta products and do not implement caching, partition-pruning experiments, broadcast experiments, or AQE comparisons. Those responsibilities remain with Roles A and C.

Role A's four materialized products can support repeated reporting, but the six canonical answers remain the SQL definitions in this module. The products share this module's scope and labels: `weather_impact_summary` and `air_quality_impact_summary` keep only `environment_in_scope` trips, and the weather label is the same `CASE` expression Q2 renders (`integrated_weather_category_sql`). `tests/test_product_query_alignment.py` re-aggregates the products and compares them with Q1-Q6; any product-backed query variant must pass that check before it is treated as equivalent.
