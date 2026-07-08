#!/bin/bash
# Submit Spark Jobs to Cluster
# Submits all 5 Spark ingestion jobs to the running Spark cluster

set -e

SPARK_MASTER="spark://spark-master:7077"
SPARK_HOME="/opt/spark"
# Use container mount point, not host path
JOBS_DIR_CONTAINER="/spark-jobs"

echo "=== Submitting Spark Jobs ==="
echo "Spark Master: $SPARK_MASTER"
echo "Jobs Directory (in container): $JOBS_DIR_CONTAINER"
echo ""

# Check if Spark master container is running
echo "Checking Spark master status..."
if ! docker ps --filter "name=spark-master" --format "{{.Status}}" | grep -q "Up"; then
    echo "✗ Error: spark-master container is not running"
    echo "  Run: docker compose up -d"
    exit 1
fi
echo "✓ Spark master container is running"

sleep 2

# Submit all Spark jobs
jobs=(
    "ingest_weather.py"
    "ingest_orders.py"
    "ingest_logistics.py"
    "ingest_inventory.py"
    "ingest_user_events.py"
)

for job in "${jobs[@]}"; do
    echo ""
    echo "Submitting: $job"

    docker exec spark-master /opt/spark/bin/spark-submit \
        --master "$SPARK_MASTER" \
        --driver-memory 512m \
        --executor-memory 1g \
        --executor-cores 1 \
        --total-executor-cores 2 \
        --py-files "$JOBS_DIR_CONTAINER/shared_utils.py" \
        "$JOBS_DIR_CONTAINER/$job" &

    echo "  ✓ Submitted (running in background)"
    sleep 2
done

echo ""
echo "=== All Spark Jobs Submitted ==="
echo ""
echo "Monitor Spark cluster at: http://localhost:8080"
echo "Worker 1: http://localhost:8081"
echo "Worker 2: http://localhost:8082"
echo ""
echo "View Spark logs:"
echo "  docker logs spark-master"
echo "  docker logs spark-worker-1"
echo "  docker logs spark-worker-2"
