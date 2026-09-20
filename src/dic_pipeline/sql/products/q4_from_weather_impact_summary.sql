-- Q4 rebuilt from the weather_impact_summary product. Per-zone trips per weather category
-- come from the product; the per-category denominator is the calendar's weather hours,
-- exactly as in the canonical zone x weather-hour cross join. Valid for the full coverage
-- range only, because the product is not keyed by time.
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
weather_hours AS (
    SELECT
        calendar_hours.pickup_hour_utc,
        {weather_case_standardized} AS weather_category
    FROM {weather_view}
    JOIN calendar_hours
      ON weather_hour_utc = calendar_hours.pickup_hour_utc
    WHERE coco IS NOT NULL
),
category_hours AS (
    SELECT weather_category, COUNT(*) AS sample_hour_count
    FROM weather_hours
    GROUP BY weather_category
    HAVING COUNT(*) >= {minimum_weather_hours}
),
zone_demand AS (
    SELECT
        pickup_location_id,
        MAX(pickup_zone) AS pickup_zone,
        MAX(pickup_borough) AS pickup_borough,
        weather_category,
        SUM(trip_count) AS trip_count
    FROM {product_view}
    WHERE pickup_location_id IS NOT NULL
    GROUP BY pickup_location_id, weather_category
),
zones AS (
    SELECT
        pickup_location_id,
        MAX(pickup_zone) AS pickup_zone,
        MAX(pickup_borough) AS pickup_borough
    FROM zone_demand
    GROUP BY pickup_location_id
),
category_statistics AS (
    SELECT
        zones.pickup_location_id,
        zones.pickup_zone,
        zones.pickup_borough,
        category_hours.weather_category,
        category_hours.sample_hour_count,
        COALESCE(zone_demand.trip_count, 0) / category_hours.sample_hour_count AS average_hourly_demand
    FROM zones
    CROSS JOIN category_hours
    LEFT JOIN zone_demand
      ON zones.pickup_location_id = zone_demand.pickup_location_id
     AND category_hours.weather_category = zone_demand.weather_category
),
zone_variation AS (
    SELECT
        pickup_location_id,
        MAX(pickup_zone) AS pickup_zone,
        MAX(pickup_borough) AS pickup_borough,
        COUNT(*) AS weather_category_count,
        SUM(sample_hour_count) AS weather_hour_count,
        MIN(average_hourly_demand) AS minimum_average_hourly_demand,
        MAX(average_hourly_demand) AS maximum_average_hourly_demand,
        MAX(average_hourly_demand) - MIN(average_hourly_demand) AS demand_range,
        STDDEV_POP(average_hourly_demand) AS demand_stddev
    FROM category_statistics
    GROUP BY pickup_location_id
    HAVING COUNT(*) >= 2
)
SELECT
    ROW_NUMBER() OVER (
        ORDER BY demand_range DESC, demand_stddev DESC, pickup_location_id
    ) AS variation_rank,
    pickup_location_id,
    COALESCE(pickup_zone, 'Unknown') AS pickup_zone,
    COALESCE(pickup_borough, 'Unknown') AS pickup_borough,
    weather_category_count,
    weather_hour_count,
    minimum_average_hourly_demand,
    maximum_average_hourly_demand,
    demand_range,
    demand_stddev
FROM zone_variation
ORDER BY variation_rank
