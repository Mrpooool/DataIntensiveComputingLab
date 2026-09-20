-- Q5 rebuilt from the daily_mobility_summary product: hourly demand comes from the product
-- (all trips, UTC hour key); the calendar padding and weekday conversion are canonical.
WITH calendar_hours AS (
    SELECT EXPLODE(
        CASE WHEN first_hour < end_hour_exclusive
             THEN SEQUENCE(first_hour, end_hour_exclusive - INTERVAL 1 HOUR, INTERVAL 1 HOUR)
             ELSE ARRAY()
        END
    ) AS pickup_hour_utc
    FROM (
        SELECT
            {calendar_first_hour} AS first_hour,
            {calendar_end_hour_exclusive} AS end_hour_exclusive
    )
),
hourly_demand AS (
    SELECT pickup_hour_utc, trip_count
    FROM {product_view}
    WHERE {trip_filter}
),
local_hours AS (
    SELECT
        hours.pickup_hour_utc,
        FROM_UTC_TIMESTAMP(hours.pickup_hour_utc, '{analysis_timezone}') AS local_pickup_ts,
        COALESCE(demand.trip_count, 0) AS trip_count
    FROM calendar_hours AS hours
    LEFT JOIN hourly_demand AS demand USING (pickup_hour_utc)
),
weekday_hour_statistics AS (
    SELECT
        ((DAYOFWEEK(local_pickup_ts) + 5) % 7) + 1 AS weekday_number,
        CASE ((DAYOFWEEK(local_pickup_ts) + 5) % 7) + 1
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
            WHEN 7 THEN 'Sunday'
        END AS weekday_name,
        HOUR(local_pickup_ts) AS local_pickup_hour,
        COUNT(*) AS observed_hour_count,
        SUM(trip_count) AS total_trip_count,
        AVG(trip_count) AS average_hourly_trip_count
    FROM local_hours
    GROUP BY
        ((DAYOFWEEK(local_pickup_ts) + 5) % 7) + 1,
        HOUR(local_pickup_ts)
),
ranked AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY weekday_number
            ORDER BY average_hourly_trip_count DESC
        ) AS peak_rank
    FROM weekday_hour_statistics
)
SELECT
    weekday_number,
    weekday_name,
    local_pickup_hour,
    observed_hour_count,
    total_trip_count,
    average_hourly_trip_count
FROM ranked
WHERE peak_rank = 1
ORDER BY weekday_number, local_pickup_hour
