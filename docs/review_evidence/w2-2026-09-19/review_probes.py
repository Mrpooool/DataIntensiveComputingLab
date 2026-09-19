import json
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import functions as F
from dic_pipeline.ingestion import create_spark
from dic_pipeline.queries import run_query
from dic_pipeline.data_products import build_weather_impact_summary, build_air_quality_impact_summary, _as_utc_naive
from tests.test_queries import AnalyticalQueryTests, INTEGRATED_SCHEMA

spark = create_spark(master='local[2]', driver_memory='2g', shuffle_partitions=2)
spark.sparkContext.setLogLevel('ERROR')
results = {}
try:
    fixture = AnalyticalQueryTests()
    fixture.spark = spark
    fixture.setUp()
    rows = run_query(spark, 'q3', start_date='2024-01-01', end_date='2024-01-02').collect()
    results['q3_requested_full_day'] = {'expected_hours': 24, 'actual_hours': sum(r.hour_count for r in rows)}
    rows = [
        ('a', datetime(2024,1,1,6,tzinfo=timezone.utc), 1, 'Alpha', 'Manhattan', True, True, 1, 1.0),
        ('b', datetime(2024,1,8,5,tzinfo=timezone.utc), 1, 'Alpha', 'Manhattan', True, True, 1, 1.0),
        ('c', datetime(2024,1,8,5,tzinfo=timezone.utc), 1, 'Alpha', 'Manhattan', True, True, 1, 1.0),
        ('d', datetime(2024,1,8,6,tzinfo=timezone.utc), 1, 'Alpha', 'Manhattan', True, True, 1, 1.0),
    ]
    spark.createDataFrame(rows, INTEGRATED_SCHEMA).createOrReplaceTempView('integrated_taxi_trips')
    peaks = run_query(spark, 'q5', start_date='2024-01-01', end_date='2024-01-09').collect()
    results['q5_two_mondays'] = {'expected_tied_hours': [0,1], 'actual': [r.asDict() for r in peaks if r.weekday_name == 'Monday']}
    fixture.setUp()
    canonical = {r.pickup_location_id: r.demand_range for r in run_query(spark,'q4',start_date='2024-01-01',end_date='2024-01-02').collect()}
    product = build_weather_impact_summary(spark).collect()
    results['weather_product_vs_q4'] = {'canonical_demand_range':canonical, 'product_rows':[r.asDict() for r in product]}
    air_schema = 'pickup_hour_utc timestamp, air_quality_pm25 double, air_quality_matched boolean, environment_in_scope boolean'
    hour = datetime(2024,1,1,5,tzinfo=timezone.utc)
    spark.createDataFrame([(hour,8.0,True,True),(hour,None,False,False)],air_schema).createOrReplaceTempView('integrated_taxi_trips')
    air_product = build_air_quality_impact_summary(spark).first()
    results['air_product_scope'] = {'expected_nyc_demand':1,'actual_product_demand':air_product.trip_count}
    instant=datetime(2024,1,1,12,tzinfo=timezone.utc)
    written=spark.createDataFrame([(_as_utc_naive(instant),)],'created_at_utc timestamp').selectExpr('CAST(created_at_utc AS STRING) AS stored_utc').first()
    results['metadata_timestamp']={'expected_utc':'2024-01-01 12:00:00','actual_utc':written.stored_utc}
    Path('review-probes.json').write_text(json.dumps(results,indent=2,default=str),encoding='utf-8')
    print(json.dumps(results,indent=2,default=str))
finally:
    spark.stop()
