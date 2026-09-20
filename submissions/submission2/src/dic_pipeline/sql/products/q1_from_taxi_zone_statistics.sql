-- Q1 rebuilt from the taxi_zone_statistics product (same grain: local month x pickup zone).
SELECT
    local_pickup_month,
    pickup_location_id,
    COALESCE(pickup_zone, 'Unknown') AS pickup_zone,
    COALESCE(pickup_borough, 'Unknown') AS pickup_borough,
    trip_count
FROM {product_view}
ORDER BY local_pickup_month, trip_count DESC, pickup_location_id
