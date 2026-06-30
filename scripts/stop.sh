#!/bin/bash
# Stop Script: Gracefully stop all containers without deleting data

set -e

echo "=== Stopping IoT Pipeline ==="

if [ ! -f "docker-compose.yml" ]; then
    echo "Error: docker-compose.yml not found"
    exit 1
fi

echo "Stopping containers (preserving data)..."
docker compose down

echo "✓ IoT pipeline stopped"
echo ""
echo "To restart without losing data, run:"
echo "  docker compose up -d"
