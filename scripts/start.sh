#!/bin/bash
# Start Script: Full system startup with data generators and Spark jobs

set -e

echo "=== Starting IoT Data Pipeline ==="
echo ""

# Check prerequisites
if ! command -v docker &> /dev/null; then
    echo "Error: docker is not installed"
    exit 1
fi

if [ ! -f "docker-compose.yml" ]; then
    echo "Error: docker-compose.yml not found in current directory"
    exit 1
fi

# Create required data directories if they don't exist
echo "Step 0: Preparing data directories for persistent volumes..."
DATA_DIRS=("data-nodered" "data-emqx" "data-postgres" "data-kafka" "data-kafka-ui" "data-zookeeper" "data-zookeeper-log" "data-spark-master" "data-spark-logs" "data-spark-worker-1" "data-spark-worker-2")
for dir in "${DATA_DIRS[@]}"; do
    if [ ! -d "$dir" ]; then
        mkdir -p "$dir"
        echo "  ✓ Created: $dir"
    fi
done

# The spark-master/spark-worker images run as the baked-in "spark" user (uid 185),
# which doesn't match the host user owning these bind mounts - without this, the
# containers can read but never write (logs, streaming checkpoints) and fail silently
# or with a "Permission denied" mkdir error.
SPARK_DATA_DIRS=("data-spark-master" "data-spark-logs" "data-spark-worker-1" "data-spark-worker-2")
for dir in "${SPARK_DATA_DIRS[@]}"; do
    chmod 777 "$dir"
done
echo ""

# Start Docker services
echo "---"
echo "Step 1: Starting Docker containers..."
echo ""
docker compose up -d

# Wait for Docker services
echo "---"
echo "Step 2: Waiting 20s for services to stabilize..."
echo ""
sleep 20

# Verify Kafka/Zookeeper agree on the cluster ID: data-kafka/ and
# data-zookeeper/ are independent bind-mounted directories that can fall out
# of sync (e.g. one gets cleared/recreated without the other), which makes
# Kafka crash-loop with InconsistentClusterIdException.
if [ -f "data-kafka/meta.properties" ]; then
    KAFKA_CLUSTER_ID=$(grep '^cluster.id=' data-kafka/meta.properties | cut -d= -f2)
    ZK_CLUSTER_ID=$(docker exec zookeeper zookeeper-shell localhost:2181 get /cluster/id 2>/dev/null \
        | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')

    if [ -n "$KAFKA_CLUSTER_ID" ] && [ -n "$ZK_CLUSTER_ID" ] && [ "$KAFKA_CLUSTER_ID" != "$ZK_CLUSTER_ID" ]; then
        echo "✗ Error: Kafka/Zookeeper cluster ID mismatch."
        echo "  data-kafka/meta.properties: $KAFKA_CLUSTER_ID"
        echo "  Zookeeper /cluster/id:      $ZK_CLUSTER_ID"
        echo "  Kafka will crash-loop (InconsistentClusterIdException). Fix with:"
        echo "    docker compose down"
        echo "    rm -rf data-kafka/* data-zookeeper/* data-zookeeper-log/*"
        echo "    bash scripts/start.sh"
        exit 1
    fi
fi

# Initializing Kafka topics
echo "---"
echo "Step 3: Initializing Kafka topics..."
echo ""
EXISTING_TOPICS=$(docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list 2>/dev/null)
if [ -n "$EXISTING_TOPICS" ]; then
    echo "✓ Kafka topics already created:"
    echo "$EXISTING_TOPICS"
else
    bash scripts/init-kafka-topics.sh
fi

# Initializing EMQX Kafka connector
echo "---"
echo "Step 4: Setting up EMQX Kafka connector and rules..."
echo ""
echo "EMQX Dashboard is running at: http://localhost:18083"
echo "  (Default Username/Password: admin/public"
echo ""
echo "Would you like to copy the EMQX Kafka connector configuration file to the container?"
read -p "Copy ./config/emqx/data/configs/cluster.hocon to container? (y/n) " -n 1 -r
echo "Note: This requires to first connect to the EMQX dashboard else the config file will be replace if you do it later."
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    if [ -f "./config/emqx/data/configs/cluster.hocon" ]; then
        echo "Copying cluster.hocon to container..."
        docker cp ./config/emqx/data/configs/cluster.hocon emqx:/opt/emqx/data/configs/cluster.hocon
        echo "✓ Configuration file copied. EMQX will apply the configuration."
        sleep 3
    else
        echo "✗ Configuration file not found: ./config/emqx/data/configs/cluster.hocon"
    fi
else
    echo "Skipping configuration file copy. Please set up the connector manually in the dashboard."
fi
echo ""

# Running overall health check
echo "---"
echo "Step 5: Running health checks..."
echo ""
bash scripts/health-check.sh
echo ""

echo ""
echo "=== Startup Complete ==="
echo ""

echo "Log onto dashboards:"
echo " Node-Red: http://localhost:1880"
echo " EMQX: http://localhost:18083 (admin/public)"
echo " Kafka UI: http://localhost:8888 (topics, messages, partitions)"
echo " Spark Master: http://localhost:8080"

echo ""
echo "=== Next steps: ==="
echo ""
echo "Terminal 1 - MQTT Generators:"
echo "  source .venv/bin/activate"
echo "  cd mqtt-generators"
echo "  python main.py"
echo ""
echo "Terminal 2 - Submit Spark Jobs:"
echo "  bash scripts/submit-spark-jobs.sh"
echo ""
echo "Terminal 3 - Monitor System:"
echo "  docker stats"
echo ""
echo "View data in PostgreSQL:"
echo "  docker exec postgres psql -U postgres -d iot_database -c \"SELECT * FROM iot.weather_data LIMIT 5;\""
