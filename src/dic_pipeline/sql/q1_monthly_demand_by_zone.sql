WITH filtered_trips AS (
    SELECT
        DATE_FORMAT(
            FROM_UTC_TIMESTAMP(pickup_hour_utc, '{analysis_timezone}'),
            'yyyy-MM'
        ) AS local_pickup_month,
        pickup_location_id,
        pickup_zone,
        pickup_borough
    FROM {integrated_view}
    WHERE {trip_filter}
)
SELECT
    local_pickup_month,
    pickup_location_id,
    COALESCE(MAX(pickup_zone), 'Unknown') AS pickup_zone,
    COALESCE(MAX(pickup_borough), 'Unknown') AS pickup_borough,
    COUNT(*) AS trip_count
FROM filtered_trips
GROUP BY local_pickup_month, pickup_location_id
ORDER BY local_pickup_month, trip_count DESC, pickup_location_id
