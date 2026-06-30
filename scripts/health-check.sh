#!/bin/bash
# Health Check Script
# Verifies all services in the IoT pipeline are running and healthy

set -e

echo "=== IoT Pipeline Health Check ==="
echo ""

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

check_service() {
    local service=$1
    local url=$2
    local port=$3
    
    if docker ps --filter "name=$service" --filter "status=running" | grep -q "$service"; then
        echo -e "${GREEN}✓${NC} $service is running"
        
        if [ -n "$url" ]; then
            if curl -s "$url" > /dev/null 2>&1; then
                echo "  └─ $url is accessible"
            else
                echo -e "  └─ ${YELLOW}⚠${NC} $url not accessible yet"
            fi
        fi
    else
        echo -e "${RED}✗${NC} $service is NOT running"
        return 1
    fi
}

echo "Service Status:"
echo "---------------"
check_service "emqx" "http://localhost:18083" "18083" || true
check_service "kafka" "" "9092" || true
check_service "zookeeper" "" "2181" || true
check_service "postgres" "" "5432" || true
check_service "spark-master" "http://localhost:8080" "8080" || true
check_service "spark-worker-1" "http://localhost:8081" "8081" || true
check_service "spark-worker-2" "http://localhost:8082" "8082" || true

echo ""
echo "Network Connectivity:"
echo "--------------------"

# Test MQTT
if timeout 2 bash -c 'echo > /dev/tcp/localhost/1883' 2>/dev/null; then
    echo -e "${GREEN}✓${NC} MQTT (1883): reachable"
else
    echo -e "${RED}✗${NC} MQTT (1883): NOT reachable"
fi

# Test Kafka
if timeout 2 bash -c 'echo > /dev/tcp/localhost/9092' 2>/dev/null; then
    echo -e "${GREEN}✓${NC} Kafka (9092): reachable"
else
    echo -e "${RED}✗${NC} Kafka (9092): NOT reachable"
fi

# Test PostgreSQL
if timeout 2 bash -c 'echo > /dev/tcp/localhost/5432' 2>/dev/null; then
    echo -e "${GREEN}✓${NC} PostgreSQL (5432): reachable"
else
    echo -e "${RED}✗${NC} PostgreSQL (5432): NOT reachable"
fi

# Test Spark Master
if timeout 2 bash -c 'echo > /dev/tcp/localhost/7077' 2>/dev/null; then
    echo -e "${GREEN}✓${NC} Spark Master (7077): reachable"
else
    echo -e "${RED}✗${NC} Spark Master (7077): NOT reachable"
fi

echo ""
echo "Web Interfaces:"
echo "---------------"
echo "  • EMQX Dashboard:    http://localhost:18083"
echo "  • Spark Master:      http://localhost:8080"
echo "  • Spark Worker 1:    http://localhost:8081"
echo "  • Spark Worker 2:    http://localhost:8082"

echo ""
echo "Database Check:"
echo "---------------"

# Check PostgreSQL tables
if timeout 5 docker exec postgres psql -U postgres -d iot_database -c "\dt iot.*" 2>/dev/null | grep -q weather_data; then
    echo -e "${GREEN}✓${NC} PostgreSQL schema initialized with tables"
else
    echo -e "${YELLOW}⚠${NC} PostgreSQL tables not yet initialized"
fi

echo ""
echo "=== Health Check Complete ==="
