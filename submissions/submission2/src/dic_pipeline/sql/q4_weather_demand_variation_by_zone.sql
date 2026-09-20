WITH filtered_trips AS (
    SELECT
        pickup_hour_utc,
        pickup_location_id,
        pickup_zone,
        pickup_borough
    FROM {integrated_view}
    WHERE {trip_filter}
      AND environment_in_scope
      AND pickup_location_id IS NOT NULL
),
calendar_hours AS (
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
zones AS (
    SELECT
        pickup_location_id,
        MAX(pickup_zone) AS pickup_zone,
        MAX(pickup_borough) AS pickup_borough
    FROM filtered_trips
    GROUP BY pickup_location_id
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
observed_demand AS (
    SELECT pickup_hour_utc, pickup_location_id, COUNT(*) AS trip_count
    FROM filtered_trips
    GROUP BY pickup_hour_utc, pickup_location_id
),
zone_weather_hours AS (
    SELECT
        zones.pickup_location_id,
        zones.pickup_zone,
        zones.pickup_borough,
        weather_hours.pickup_hour_utc,
        weather_hours.weather_category,
        COALESCE(observed_demand.trip_count, 0) AS trip_count
    FROM zones
    CROSS JOIN weather_hours
    LEFT JOIN observed_demand
      ON zones.pickup_location_id = observed_demand.pickup_location_id
     AND weather_hours.pickup_hour_utc = observed_demand.pickup_hour_utc
),
category_statistics AS (
    SELECT
        pickup_location_id,
        MAX(pickup_zone) AS pickup_zone,
        MAX(pickup_borough) AS pickup_borough,
        weather_category,
        COUNT(*) AS sample_hour_count,
        AVG(trip_count) AS average_hourly_demand
    FROM zone_weather_hours
    GROUP BY pickup_location_id, weather_category
    HAVING COUNT(*) >= {minimum_weather_hours}
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
