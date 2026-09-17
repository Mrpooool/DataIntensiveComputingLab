WITH classified_trips AS (
    SELECT
        CASE
            WHEN NOT weather_matched THEN 'unmatched'
            WHEN weather_coco IS NULL THEN 'missing_code'
            ELSE {weather_case_integrated}
        END AS weather_category,
        trip_distance
    FROM {integrated_view}
    WHERE {trip_filter}
      AND environment_in_scope
)
SELECT
    weather_category,
    COUNT(*) AS trip_count,
    COUNT(trip_distance) AS valid_distance_count,
    AVG(trip_distance) AS average_trip_distance
FROM classified_trips
GROUP BY weather_category
ORDER BY weather_category
