#!/bin/bash
# Start Script: Launch the mock IoT data (MQTT) generators
# Run this in its own terminal, after scripts/start.sh has brought up the stack

set -e

echo "=== Starting MQTT Data Generators ==="
echo ""

if [ ! -f ".venv/bin/activate" ]; then
    echo "Error: .venv not found. Create it first:"
    echo "  uv venv --python 3.12 .venv"
    echo "  source .venv/bin/activate"
    echo "  uv sync"
    exit 1
fi

if [ ! -f "mqtt-generators/main.py" ]; then
    echo "Error: mqtt-generators/main.py not found"
    exit 1
fi

source .venv/bin/activate
python mqtt-generators/main.py

echo ""
echo "=== Finished MQTT Data Generators ==="

