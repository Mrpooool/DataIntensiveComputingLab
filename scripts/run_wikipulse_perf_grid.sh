#!/usr/bin/env sh
# C's WikiPulse performance grid. Run inside the spark-client container:
#   docker compose -f compose.wikipulse.yml exec -T spark-client sh scripts/run_wikipulse_perf_grid.sh <baseline> <tag>
# Every paced run replays the same EVENTS archive rows from 2026-10-23 12:00 UTC (after
# the baseline history, about 25 minutes of event time) into a fresh topic, so every
# configuration sees identical input. Results go to /tmp/wikipulse_perf/<run>.json.
set -eu
BASELINE=$1
TAG=$2
VERSION=${VERSION:-348}
FROM=${FROM:-2026-10-23T12:00:00+00:00}
EVENTS=${EVENTS:-50000}
OUT=/tmp/wikipulse_perf

run() {  # run_id rate partitions trigger events [extra args]
  id=$1; rate=$2; partitions=$3; trigger=$4; events=$5; shift 5
  [ -f "$OUT/$id.json" ] && return 0
  /usr/local/bin/spark-submit --master spark://spark-master:7077 --driver-memory 768m \
    --conf spark.sql.shuffle.partitions=8 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,io.delta:delta-spark_2.13:4.4.0 \
    scripts/run_wikipulse_perf.py --version "$VERSION" --from-event-time "$FROM" \
    --run-id "$id" --rate "$rate" --events "$events" --partitions "$partitions" \
    --trigger-interval "$trigger" --baseline-version "$BASELINE" --warmup-seconds 20 \
    --output "$OUT/$id.json" "$@" > "$OUT/$id.log" 2>&1 || echo "FAILED $id"
  echo "done $id"
}

mkdir -p "$OUT"
for rep in a b; do
  for rate in 100 200 400; do
    for trigger in 5 30; do
      run "${TAG}_r${rate}_t${trigger}_p3_${rep}" "$rate" 3 "$trigger seconds" "$EVENTS"
    done
  done
  for partitions in 1 3; do
    run "${TAG}_r200_t10_p${partitions}_${rep}" 200 "$partitions" "10 seconds" "$EVENTS"
  done
done
# Drain throughput: the same slice is fully in Kafka before the queries start.
for partitions in 1 3; do
  run "${TAG}_drain_p${partitions}" 1000000 "$partitions" "1 second" "$EVENTS" \
    --preload --max-offsets-per-trigger 10000 --drain-seconds 0
done
