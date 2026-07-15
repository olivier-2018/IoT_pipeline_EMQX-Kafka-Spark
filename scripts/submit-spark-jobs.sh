#!/bin/bash
# Submit Spark Jobs to Cluster
# Ships spark-structured-streaming-jobs/shared_utils.zip to executors via --py-files and submits jobs

set -e

SPARK_MASTER="spark://spark-master:7077"
CONTAINER_JOBS_DIR="/opt/spark-structured-streaming-jobs"
SHARED_UTILS_ZIP="$CONTAINER_JOBS_DIR/shared_utils.zip"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOST_JOBS_DIR="$SCRIPT_DIR/../spark-structured-streaming-jobs"
HOST_SHARED_UTILS_ZIP="$HOST_JOBS_DIR/shared_utils.zip"
HOST_SHARED_UTILS_SRC="$HOST_JOBS_DIR/shared_utils/shared_utils.py"

# Driver output (docker exec spark-submit ...) is a separate process from the
# container's PID 1, so `docker logs spark-master` never captures it - it
# would otherwise just print straight to this script's terminal. Redirect it
# to a per-job log file instead.
LOG_DIR="$SCRIPT_DIR/../logs/submit-spark-jobs"
mkdir -p "$LOG_DIR"
RUN_TIMESTAMP=$(date +%Y%m%d_%H%M%S)

echo "=== Submitting Spark Jobs ==="
echo "Spark Master: $SPARK_MASTER"
echo "Jobs Directory (container): $CONTAINER_JOBS_DIR"
echo "Driver logs: $LOG_DIR"
echo ""

# shared_utils.zip (shipped to executors via --py-files) is committed to the repo.
# Regenerate it on the host if it's missing, or stale relative to its source.
if [ ! -f "$HOST_SHARED_UTILS_ZIP" ] || [ "$HOST_SHARED_UTILS_SRC" -nt "$HOST_SHARED_UTILS_ZIP" ]; then
    echo "shared_utils.zip missing or out of date, (re)generating it..."
    if ! command -v zip &> /dev/null; then
        echo "✗ Error: 'zip' is not installed on the host, cannot generate $HOST_SHARED_UTILS_ZIP"
        exit 1
    fi
    ( cd "$HOST_JOBS_DIR" && zip -r shared_utils.zip shared_utils -x "*/__pycache__/*" )
    echo "✓ Generated $HOST_SHARED_UTILS_ZIP"
fi

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
jobs=(
    "ingest_weather.py"
    "ingest_orders.py"
    "ingest_logistics.py"
    "ingest_inventory.py"
    "ingest_user_events.py"
)

for job in "${jobs[@]}"; do
    job_name="${job%.py}"
    log_file="$LOG_DIR/${RUN_TIMESTAMP}_${job_name}.log"

    echo ""
    echo "Submitting: $job"
    echo "  Log: $log_file"

    # --driver-memory is set explicitly (Spark's 1g default) since the driver JVM
    # runs inside spark-master's container (client deploy mode) alongside the
    # Master daemon; 512m stays within spark-master's mem_limit with headroom.
    # --total-executor-cores 1 and --executor-memory 768m cap each job's
    # footprint so multiple concurrent streaming jobs don't starve each other:
    # Spark's 1g executor-memory default would fill an entire worker's
    # advertised memory with a single executor, blocking every other app
    # regardless of free cores (2 workers x SPARK_WORKER_MEMORY=2500m fits
    # ~3 executors@768m each, well above Spark's ~450MB executor-memory floor).
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master "$SPARK_MASTER" \
        --deploy-mode client \
        --driver-memory 512m \
        --total-executor-cores 1 \
        --executor-memory 768m \
        --py-files "$SHARED_UTILS_ZIP" \
        "$CONTAINER_JOBS_DIR/$job" > "$log_file" 2>&1 &

    echo "  ✓ Submitted (PID $!, running in background)"
    sleep 2
done

echo ""
echo "=== All jobs submitted ==="
