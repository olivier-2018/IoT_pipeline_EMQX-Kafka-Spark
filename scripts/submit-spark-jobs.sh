#!/bin/bash
# Submit Spark Jobs to Cluster
# Ships spark-jobs/shared_utils.zip to executors via --py-files and submits jobs

set -e

SPARK_MASTER="spark://spark-master:7077"
CONTAINER_JOBS_DIR="/opt/spark-jobs"
SHARED_UTILS_ZIP="$CONTAINER_JOBS_DIR/shared_utils.zip"

echo "=== Submitting Spark Jobs ==="
echo "Spark Master: $SPARK_MASTER"
echo "Jobs Directory (container): $CONTAINER_JOBS_DIR"
echo ""

# Check if Spark master container is running
echo "Checking spark-master status..."
if ! docker ps --filter "name=spark-master" --format "{{.Status}}" | grep -q "Up"; then
    echo "✗ Error: spark-master container is not running"
    echo "  Run: docker compose up -d"
    exit 1
fi
echo "✓ spark-master container is running"

sleep 2

# Submit all Spark jobs
# jobs=(
#     "ingest_weather.py"
#     "ingest_orders.py"
#     "ingest_logistics.py"
#     "ingest_inventory.py"
#     "ingest_user_events.py"
# )
jobs=(
    "ingest_weather.py"
)

for job in "${jobs[@]}"; do
    echo ""
    echo "Submitting: $job"

    # --driver-memory is set explicitly (Spark's 1g default) since the driver JVM
    # runs inside spark-master's container (client deploy mode) alongside the
    # Master daemon; 512m stays within spark-master's mem_limit with headroom.
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master "$SPARK_MASTER" \
        --deploy-mode client \
        --driver-memory 512m \
        --py-files "$SHARED_UTILS_ZIP" \
        "$CONTAINER_JOBS_DIR/$job" &

    echo "  ✓ Submitted (running in background)"
    sleep 2
done

echo ""
echo "=== All jobs submitted ==="
