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

# Per-job --total-executor-cores. Each Kafka topic has 2 partitions, so 2 cores
# lets a job process both partitions of a micro-batch in true parallel instead
# of serially - given to the two busiest topics only (user_events 200-500
# msg/min, orders 50-100 msg/min). The other 3 stay at 1 core: with
# SPARK_WORKER_CORES=3 x 2 workers = 6 slots total, uniformly giving every job
# 2 cores would need 10 slots and starve whichever job submits last (Spark
# Standalone doesn't preempt or top up a running app's allocation), so this is
# a deliberate cap, not a uniform bump. 2+2+1+1+1 = 7 of 8 slots if
# SPARK_WORKER_CORES is ever raised to 4/worker; against the current 3/worker
# (6 slots) it's 7 of 6 - i.e. not all 5 can run at their target core count
# simultaneously without raising SPARK_WORKER_CORES (whichever job submits
# last when capacity runs out gets fewer cores than requested, not zero -
# Standalone grants what's free at submit time, not an all-or-nothing block).
declare -A JOB_CORES=(
    ["ingest_weather.py"]=1
    ["ingest_orders.py"]=2
    ["ingest_logistics.py"]=1
    ["ingest_inventory.py"]=1
    ["ingest_user_events.py"]=2
)

for job in "${jobs[@]}"; do
    job_name="${job%.py}"
    log_file="$LOG_DIR/${RUN_TIMESTAMP}_${job_name}.log"
    cores="${JOB_CORES[$job]}"

    echo ""
    echo "Submitting: $job (--total-executor-cores $cores)"
    echo "  Log: $log_file"

    # --driver-memory is set explicitly (Spark's 1g default) since the driver JVM
    # runs inside spark-master's container (client deploy mode) alongside the
    # Master daemon; 512m stays within spark-master's mem_limit with headroom.
    # --executor-memory 768m caps each executor's footprint so multiple
    # concurrent streaming jobs don't starve each other: Spark's 1g default
    # would let a single executor consume most of a worker's advertised
    # memory, blocking every other app regardless of free cores. Note a
    # 2-core request typically launches as 2 separate 1-core executors
    # (Standalone's spreadOut spreads across workers by default), so it costs
    # 2 x 768m of worker memory, not 768m.
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master "$SPARK_MASTER" \
        --deploy-mode client \
        --driver-memory 512m \
        --total-executor-cores "$cores" \
        --executor-memory 768m \
        --py-files "$SHARED_UTILS_ZIP" \
        "$CONTAINER_JOBS_DIR/$job" > "$log_file" 2>&1 &

    echo "  ✓ Submitted (PID $!, running in background)"
    sleep 2
done

echo ""
echo "=== All jobs submitted ==="
