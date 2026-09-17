WITH filtered_trips AS (
    SELECT pickup_hour_utc
    FROM {integrated_view}
    WHERE {trip_filter}
      AND environment_in_scope
),
bounds AS (
    SELECT MIN(pickup_hour_utc) AS first_hour, MAX(pickup_hour_utc) AS last_hour
    FROM filtered_trips
),
calendar_hours AS (
    SELECT EXPLODE(SEQUENCE(first_hour, last_hour, INTERVAL 1 HOUR)) AS pickup_hour_utc
    FROM bounds
    WHERE first_hour IS NOT NULL
),
hourly_demand AS (
    SELECT pickup_hour_utc, COUNT(*) AS trip_count
    FROM filtered_trips
    GROUP BY pickup_hour_utc
),
site_hour_pm25 AS (
    SELECT
        site_id,
        air_quality_hour_utc,
        MEDIAN(measurement_value) AS site_pm25
    FROM {air_quality_view}
    WHERE parameter_code = '88101'
      AND measurement_unit = 'Micrograms/cubic meter (LC)'
      AND method_type = 'FEM'
      AND method_code = '636'
    GROUP BY site_id, air_quality_hour_utc
),
hourly_pm25 AS (
    SELECT
        air_quality_hour_utc,
        MEDIAN(site_pm25) AS air_quality_pm25
    FROM site_hour_pm25
    GROUP BY air_quality_hour_utc
),
hourly_analysis AS (
    SELECT
        hours.pickup_hour_utc,
        COALESCE(demand.trip_count, 0) AS trip_count,
        pm25.air_quality_pm25
    FROM calendar_hours AS hours
    LEFT JOIN hourly_demand AS demand USING (pickup_hour_utc)
    LEFT JOIN hourly_pm25 AS pm25
      ON hours.pickup_hour_utc = pm25.air_quality_hour_utc
),
classified AS (
    SELECT *, {pm25_case} AS pm25_band
    FROM hourly_analysis
),
overall AS (
    SELECT CORR(air_quality_pm25, trip_count) AS overall_pm25_demand_correlation
    FROM classified
    WHERE air_quality_pm25 IS NOT NULL
)
SELECT
    pm25_band,
    COUNT(*) AS hour_count,
    SUM(trip_count) AS total_trip_count,
    AVG(trip_count) AS average_hourly_trip_count,
    AVG(air_quality_pm25) AS average_pm25,
    MAX(overall.overall_pm25_demand_correlation) AS overall_pm25_demand_correlation
FROM classified
CROSS JOIN overall
GROUP BY pm25_band
ORDER BY MIN(air_quality_pm25), pm25_band
