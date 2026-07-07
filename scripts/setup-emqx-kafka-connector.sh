#!/bin/bash
# Setup EMQX Kafka Producer Connectors via REST API (EMQX Community Edition)
# Creates: Kafka producer connectors (one per topic) → rules
# Idempotent: safe to run multiple times, only initializes once

set -e

# Source environment variables from .env file
set -a
if [ -f "$(dirname "$0")/../.env" ]; then
  source "$(dirname "$0")/../.env"
fi
set +a

EMQX_HOST="${EMQX_HOST:-emqx}"
EMQX_PORT="${EMQX_PORT:-18083}"
EMQX_API_KEY="${EMQX_API_KEY:-}"
EMQX_API_SECRET="${EMQX_API_SECRET:-}"
KAFKA_HOST="${KAFKA_HOST:-kafka}"
KAFKA_PORT="${KAFKA_PORT:-9092}"

TOPICS_MAPPING_FILE="$(dirname "$0")/../config/emqx/topics-mapping.conf"

MAX_RETRIES=30
RETRY_INTERVAL=2

echo "=== EMQX Kafka Producer Setup (Community Edition) ==="
echo "EMQX: $EMQX_HOST:$EMQX_PORT"
echo "Kafka: $KAFKA_HOST:$KAFKA_PORT"
echo "Topics mapping: $TOPICS_MAPPING_FILE"
echo ""

# Verify topics mapping file exists
if [ ! -f "$TOPICS_MAPPING_FILE" ]; then
  echo "✗ Error: Topics mapping file not found: $TOPICS_MAPPING_FILE"
  exit 1
fi

# Verify API credentials are set
if [ -z "$EMQX_API_KEY" ] || [ -z "$EMQX_API_SECRET" ]; then
  echo "✗ Error: EMQX_API_KEY and EMQX_API_SECRET must be set in .env"
  exit 1
fi

# Wait for EMQX to be ready
echo "Waiting for EMQX API to be ready..."
for i in $(seq 1 $MAX_RETRIES); do
  if curl -s -u "$EMQX_API_KEY:$EMQX_API_SECRET" \
    "http://$EMQX_HOST:$EMQX_PORT/api/v5/nodes" > /dev/null 2>&1; then
    echo "✓ EMQX API is ready"
    break
  fi

  if [ $i -lt $MAX_RETRIES ]; then
    echo "  Waiting... ($i/$MAX_RETRIES)"
    sleep $RETRY_INTERVAL
  else
    echo "✗ EMQX API failed to respond"
    exit 1
  fi
done

# Check if setup is already complete
echo ""
echo "Checking if setup already exists..."
EXISTING_RULES=$(curl -s -u "$EMQX_API_KEY:$EMQX_API_SECRET" \
  "http://$EMQX_HOST:$EMQX_PORT/api/v5/rules" 2>/dev/null)

RULE_COUNT=$(echo "$EXISTING_RULES" | grep -o '"id":"[^"]*_to_kafka"' | wc -l)
if [ "$RULE_COUNT" -ge 5 ]; then
  echo "✓ Setup already complete (5 rules found)"
  echo "✓ Skipping initialization (idempotent)"
  exit 0
fi

# Step 1: Create Kafka producer connectors (one per topic)
echo ""
echo "Step 1: Creating Kafka producer connectors..."
echo ""

while IFS='|' read -r mqtt_pattern kafka_topic data_type; do
  # Skip comments and empty lines
  [[ "$mqtt_pattern" =~ ^# ]] && continue
  [[ -z "$mqtt_pattern" ]] && continue

  # Trim whitespace
  mqtt_pattern=$(echo "$mqtt_pattern" | xargs)
  kafka_topic=$(echo "$kafka_topic" | xargs)
  data_type=$(echo "$data_type" | xargs)

  CONNECTOR_NAME="${data_type}_producer"

  echo "  Creating producer connector: $CONNECTOR_NAME"
  echo "    Kafka topic: $kafka_topic"

  CONNECTOR_RESPONSE=$(curl -s -X POST \
    -u "$EMQX_API_KEY:$EMQX_API_SECRET" \
    -H "Content-Type: application/json" \
    "http://$EMQX_HOST:$EMQX_PORT/api/v5/connectors/kafka_producer" \
    -d "{
      \"name\": \"$CONNECTOR_NAME\",
      \"enable\": true,
      \"bootstrap_servers\": \"$KAFKA_HOST:$KAFKA_PORT\",
      \"topic\": \"$kafka_topic\",
      \"value_type\": \"plain\",
      \"client_id\": \"emqx-iot-$data_type\"
    }")

  if echo "$CONNECTOR_RESPONSE" | grep -q '"code":"ALREADY_EXISTS"'; then
    echo "    ✓ Connector already exists"
  elif echo "$CONNECTOR_RESPONSE" | grep -q "\"name\":\"$CONNECTOR_NAME\""; then
    echo "    ✓ Connector created"
  else
    echo "    ⚠ Response: $CONNECTOR_RESPONSE"
  fi
  echo ""

done < "$TOPICS_MAPPING_FILE"

# Step 2: Create rules that bind MQTT topics to Kafka producer connectors
echo "Step 2: Creating forwarding rules..."
echo ""

while IFS='|' read -r mqtt_pattern kafka_topic data_type; do
  # Skip comments and empty lines
  [[ "$mqtt_pattern" =~ ^# ]] && continue
  [[ -z "$mqtt_pattern" ]] && continue

  # Trim whitespace
  mqtt_pattern=$(echo "$mqtt_pattern" | xargs)
  kafka_topic=$(echo "$kafka_topic" | xargs)
  data_type=$(echo "$data_type" | xargs)

  RULE_ID="${data_type}_to_kafka"
  CONNECTOR_NAME="${data_type}_producer"

  echo "  Creating rule: $RULE_ID"
  echo "    MQTT pattern: $mqtt_pattern"
  echo "    Connector: $CONNECTOR_NAME"

  RULE_RESPONSE=$(curl -s -X POST \
    -u "$EMQX_API_KEY:$EMQX_API_SECRET" \
    -H "Content-Type: application/json" \
    "http://$EMQX_HOST:$EMQX_PORT/api/v5/rules" \
    -d "{
      \"id\": \"$RULE_ID\",
      \"sql\": \"SELECT payload FROM \\\"$mqtt_pattern\\\"\",
      \"enable\": true,
      \"actions\": [
        {
          \"name\": \"send_to_kafka\",
          \"connector\": \"$CONNECTOR_NAME\"
        }
      ],
      \"description\": \"Forward $data_type messages from MQTT pattern $mqtt_pattern to Kafka topic $kafka_topic\"
    }")

  if echo "$RULE_RESPONSE" | grep -q '"code":"ALREADY_EXISTS"'; then
    echo "    ✓ Rule already exists"
  elif echo "$RULE_RESPONSE" | grep -q "\"id\":\"$RULE_ID\""; then
    echo "    ✓ Rule created"
  else
    echo "    ⚠ Response: $RULE_RESPONSE"
  fi
  echo ""

done < "$TOPICS_MAPPING_FILE"

echo "=== Setup Complete ==="
echo ""
echo "Verify:"
echo "  1. EMQX Dashboard: http://localhost:18083 (admin/public)"
echo "  2. Check connectors:"
echo "     curl -u \$EMQX_API_KEY:\$EMQX_API_SECRET http://localhost:18083/api/v5/connectors"
echo "  3. Check rules:"
echo "     curl -u \$EMQX_API_KEY:\$EMQX_API_SECRET http://localhost:18083/api/v5/rules"
echo "  4. Kafka UI: http://localhost:8888"
echo ""
echo "Test MQTT → Kafka flow:"
echo "  docker exec emqx mosquitto_pub -h localhost -t 'devices/weather/sensor01' -m '{\"temp\":25.5}'"
echo "  # Then check Kafka UI or:"
echo "  docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic iot-weather-data --from-beginning --max-messages 1"
