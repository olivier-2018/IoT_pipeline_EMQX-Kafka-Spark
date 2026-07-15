#!/bin/bash
# Stop Script: Gracefully stop all containers without deleting data

set -e

echo "=== Stopping IoT Pipeline ==="

if [ ! -f "docker-compose.yml" ]; then
    echo "Error: docker-compose.yml not found"
    exit 1
fi

# Spark Structured Streaming jobs run as their own docker-exec'd driver
# processes inside spark-master (--deploy-mode client) - docker compose down
# would just kill them along with the container anyway, but stopping them
# explicitly first avoids interrupting a JDBC write mid-batch.
echo "Stopping Spark Structured Streaming jobs (if running)..."
if docker ps --filter "name=spark-master" --filter "status=running" --format "{{.Names}}" | grep -q spark-master; then
    for job in ingest_weather.py ingest_orders.py ingest_logistics.py ingest_inventory.py ingest_user_events.py; do
        if docker exec spark-master pkill -f "$job" 2>/dev/null; then
            echo "  ✓ Stopped $job"
        fi
    done
else
    echo "  spark-master not running, skipping"
fi
echo ""

# MQTT generators run as a plain host process (started separately, outside
# docker compose), so docker compose down never touches them - stop it here too.
echo "Stopping MQTT generators (if running)..."
if pkill -f "mqtt-generators/main.py" 2>/dev/null; then
    echo "  ✓ Stopped MQTT generators"
else
    echo "  Not running, skipping"
fi
echo ""

echo "Stopping containers (preserving data)..."
docker compose down

echo "✓ IoT pipeline stopped"
echo ""
echo "To restart without losing data, run:"
echo "  docker compose up -d"
