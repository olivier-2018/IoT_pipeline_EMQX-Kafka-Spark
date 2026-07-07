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

# Start Docker services
echo "Step 1: Starting Docker containers..."
docker compose up -d

echo "Step 2: Waiting for services to stabilize..."
sleep 15

echo "Step 3: Initializing Kafka topics..."
bash scripts/init-kafka-topics.sh

echo "Step 4: Setting up EMQX Kafka connector..."
bash scripts/setup-emqx-kafka-connector.sh

echo "Step 5: Running health checks..."
bash scripts/health-check.sh

echo ""
echo "=== Startup Complete ==="
echo ""
echo "Next steps:"
echo ""
echo "Terminal 1 - Data Generators:"
echo "  cd data-generators"
echo "  pip install -r requirements.txt"
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
