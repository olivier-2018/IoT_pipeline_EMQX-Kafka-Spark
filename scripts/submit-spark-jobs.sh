#!/bin/bash
# Submit Spark Jobs to Cluster
# Submits all 5 Spark ingestion jobs to the running Spark cluster

set -e

SPARK_MASTER="spark://spark-master:7077"
SPARK_HOME="/opt/spark"
JOBS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/spark-jobs"

echo "=== Submitting Spark Jobs ==="
echo "Spark Master: $SPARK_MASTER"
echo "Jobs Directory: $JOBS_DIR"
echo ""

# Check if Spark master is reachable
echo "Checking Spark master connectivity..."
if ! nc -z spark-master 7077 2>/dev/null; then
    echo "⚠ Warning: Cannot connect to Spark master at $SPARK_MASTER"
    echo "Make sure Docker containers are running: docker compose ps"
fi

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
    
    docker exec spark-master spark-submit \
        --master "$SPARK_MASTER" \
        --driver-memory 512m \
        --executor-memory 1g \
        --executor-cores 1 \
        --total-executor-cores 2 \
        --py-files "$JOBS_DIR/shared_utils.py" \
        "$JOBS_DIR/$job" &
    
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
