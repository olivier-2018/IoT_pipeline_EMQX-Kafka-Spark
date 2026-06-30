#!/bin/bash
# Reset Script: Stop Docker containers, clear volumes, restart
# Use this to reset the demo to a clean state

set -e

echo "=== IoT Pipeline Reset Script ==="
echo ""

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if docker-compose.yml exists
if [ ! -f "docker-compose.yml" ]; then
    echo -e "${RED}Error: docker-compose.yml not found in current directory${NC}"
    exit 1
fi

echo -e "${YELLOW}Warning: This will stop all containers and DELETE data volumes${NC}"
read -p "Continue? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Reset cancelled"
    exit 0
fi

echo ""
echo "Step 1: Stopping containers..."
docker compose down 2>/dev/null || true

echo "Step 2: Removing volumes..."
docker volume rm spark-master-data 2>/dev/null || true
docker volume rm spark-worker-1-data 2>/dev/null || true
docker volume rm spark-worker-2-data 2>/dev/null || true

echo "Step 3: Removing local data directories..."
rm -rf ./data/* 2>/dev/null || true
mkdir -p ./data/postgres

echo ""
echo "Step 4: Starting fresh containers..."
docker compose up -d

echo "Step 5: Waiting for services to be ready..."
sleep 15

echo "Step 6: Initializing Kafka topics..."
bash scripts/init-kafka-topics.sh

echo ""
echo -e "${GREEN}✓ Reset complete!${NC}"
echo ""
echo "Services running:"
docker compose ps
echo ""
echo "Next steps:"
echo "  1. Install Python dependencies:"
echo "     pip install -r data-generators/requirements.txt"
echo "  2. Run data generators:"
echo "     cd data-generators && python main.py"
echo "  3. In another terminal, submit Spark jobs:"
echo "     cd spark-jobs && bash ../scripts/submit-spark-jobs.sh"
