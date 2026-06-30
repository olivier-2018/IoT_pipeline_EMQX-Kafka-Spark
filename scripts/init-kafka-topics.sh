#!/bin/bash
# Kafka Topic Initialization Script
# Creates topics with proper partitioning strategy (2 partitions per topic = 10 total)
# Run this script after docker compose up -d

set -e

BOOTSTRAP_SERVERS="localhost:9092"
NUM_PARTITIONS=2
REPLICATION_FACTOR=1
RETENTION_MS=86400000  # 1 day
RETENTION_BYTES=536870912  # 512 MB

echo "Waiting for Kafka broker to be ready..."
for i in {1..30}; do
    if nc -z localhost 9092 2>/dev/null; then
        echo "✓ Kafka broker is ready"
        break
    fi
    echo "  Waiting... ($i/30)"
    sleep 2
done

# Create topics with 2 partitions each
echo ""
echo "Creating Kafka topics..."

topics=(
    "iot-weather-data"
    "iot-orders-events"
    "iot-logistics-dispatch"
    "iot-inventory-changes"
    "iot-users-activity"
)

for topic in "${topics[@]}"; do
    echo "  Creating topic: $topic (partitions: $NUM_PARTITIONS, replication: $REPLICATION_FACTOR)"
    
    docker exec kafka kafka-topics --bootstrap-server kafka:9092 \
        --create \
        --if-not-exists \
        --topic "$topic" \
        --partitions $NUM_PARTITIONS \
        --replication-factor $REPLICATION_FACTOR \
        --config retention.ms=$RETENTION_MS \
        --config retention.bytes=$RETENTION_BYTES \
        --config compression.type=gzip
done

echo ""
echo "✓ Topic creation complete!"
echo ""
echo "Listing created topics:"
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list

echo ""
echo "✓ Kafka initialization complete!"
