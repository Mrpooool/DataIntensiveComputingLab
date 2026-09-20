-- Q2 rebuilt from the weather_impact_summary product: sums are re-aggregated across zones,
-- and the mean is total distance over total valid samples (never a mean of means).
SELECT
    weather_category,
    SUM(trip_count) AS trip_count,
    SUM(valid_distance_count) AS valid_distance_count,
    TRY_DIVIDE(SUM(distance_sum), SUM(valid_distance_count)) AS average_trip_distance
FROM {product_view}
GROUP BY weather_category
ORDER BY weather_category
