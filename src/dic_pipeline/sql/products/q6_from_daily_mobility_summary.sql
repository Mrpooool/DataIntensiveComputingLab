-- Q6 rebuilt from the daily_mobility_summary product (all trips, UTC hour key).
WITH monthly_demand AS (
    SELECT
        DATE_FORMAT(
            FROM_UTC_TIMESTAMP(pickup_hour_utc, '{analysis_timezone}'),
            'yyyy-MM'
        ) AS local_pickup_month,
        SUM(trip_count) AS trip_count
    FROM {product_view}
    WHERE {trip_filter}
    GROUP BY DATE_FORMAT(
        FROM_UTC_TIMESTAMP(pickup_hour_utc, '{analysis_timezone}'),
        'yyyy-MM'
    )
),
with_previous AS (
    SELECT
        *,
        LAG(trip_count) OVER (ORDER BY local_pickup_month) AS previous_month_trip_count
    FROM monthly_demand
)
SELECT
    local_pickup_month,
    trip_count,
    previous_month_trip_count,
    trip_count - previous_month_trip_count AS month_over_month_change,
    CASE
        WHEN previous_month_trip_count IS NULL OR previous_month_trip_count = 0 THEN NULL
        ELSE 100.0 * (trip_count - previous_month_trip_count) / previous_month_trip_count
    END AS month_over_month_percent
FROM with_previous
ORDER BY local_pickup_month
