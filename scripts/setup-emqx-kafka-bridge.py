#!/usr/bin/env python3
"""
Setup EMQX Kafka Bridge via REST API
Configures EMQX to forward MQTT messages to Kafka topics
"""

import os
import requests
import json
import time
import sys
from pathlib import Path

# Load environment variables
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent.parent / ".env"
    load_dotenv(env_path)
except ImportError:
    print("Note: python-dotenv not installed, using environment variables")

# Configuration from environment or defaults
# Use localhost for host connections, emqx for Docker internal connections
EMQX_HOST = os.getenv("EMQX_HOST", "localhost")
EMQX_PORT = os.getenv("EMQX_PORT", "18083")
EMQX_API_KEY = os.getenv("EMQX_API_KEY", "")
EMQX_API_SECRET = os.getenv("EMQX_API_SECRET", "")
KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")

MAX_RETRIES = 30
RETRY_INTERVAL = 2

BASE_URL = f"http://{EMQX_HOST}:{EMQX_PORT}/api/v5"

# Topic mappings: data type -> Kafka topic
TOPIC_MAPPINGS = {
    "weather": "iot-weather-data",
    "orders": "iot-orders-events",
    "logistics": "iot-logistics-dispatch",
    "inventory": "iot-inventory-changes",
    "users": "iot-users-activity",
}


def wait_for_emqx():
    """Wait for EMQX API to be ready"""
    print("Waiting for EMQX API to be ready...")

    for i in range(MAX_RETRIES):
        try:
            resp = requests.get(
                f"{BASE_URL}/nodes",
                auth=(EMQX_API_KEY, EMQX_API_SECRET) if EMQX_API_KEY else None,
                timeout=2
            )
            if resp.status_code == 200:
                print("✓ EMQX API is ready")
                return True
        except Exception:
            pass

        if i < MAX_RETRIES - 1:
            print(f"  Waiting... ({i+1}/{MAX_RETRIES})")
            time.sleep(RETRY_INTERVAL)

    print("✗ EMQX API failed to respond")
    return False


def create_kafka_connector():
    """Create Kafka connector in EMQX"""
    print("\nCreating Kafka connector...")

    connector_config = {
        "type": "kafka",
        "name": "kafka-connector",
        "enable": True,
        "bootstrap_hosts": f"{KAFKA_HOST}:{KAFKA_PORT}",
        "compression": "gzip"
    }

    try:
        resp = requests.post(
            f"{BASE_URL}/connectors",
            json=connector_config,
            auth=(EMQX_API_KEY, EMQX_API_SECRET) if EMQX_API_KEY else None,
            timeout=5
        )

        if resp.status_code in [201, 200]:
            print("✓ Kafka connector created")
            return True
        elif "already exists" in resp.text or resp.status_code == 409:
            print("✓ Kafka connector already exists")
            return True
        else:
            print(f"✗ Failed to create connector: {resp.status_code} - {resp.text}")
            return False
    except Exception as e:
        print(f"✗ Error creating connector: {e}")
        return False


def create_bridge_actions():
    """Create bridge actions for each data type"""
    print("\nCreating bridge actions...")

    success = True
    for data_type, kafka_topic in TOPIC_MAPPINGS.items():
        action_config = {
            "type": "kafka",
            "name": f"kafka-{data_type}",
            "enable": True,
            "connector": "kafka-connector",
            "parameters": {
                "topic": kafka_topic
            }
        }

        try:
            resp = requests.post(
                f"{BASE_URL}/bridges",
                json=action_config,
                auth=(EMQX_API_KEY, EMQX_API_SECRET) if EMQX_API_KEY else None,
                timeout=5
            )

            if resp.status_code in [201, 200]:
                print(f"  ✓ Bridge action '{data_type}' created")
            elif "already exists" in resp.text or resp.status_code == 409:
                print(f"  ✓ Bridge action '{data_type}' already exists")
            else:
                print(f"  ✗ Failed to create bridge '{data_type}': {resp.status_code}")
                success = False
        except Exception as e:
            print(f"  ✗ Error creating bridge '{data_type}': {e}")
            success = False

    return success


def create_rules():
    """Create EMQX rules to forward MQTT to Kafka"""
    print("\nCreating forwarding rules...")

    rules = {
        "weather": {
            "sql": 'SELECT * FROM "devices/weather/+"',
            "action": "kafka_kafka-weather"
        },
        "orders": {
            "sql": 'SELECT * FROM "devices/orders/+"',
            "action": "kafka_kafka-orders"
        },
        "logistics": {
            "sql": 'SELECT * FROM "devices/logistics/+"',
            "action": "kafka_kafka-logistics"
        },
        "inventory": {
            "sql": 'SELECT * FROM "devices/inventory/+"',
            "action": "kafka_kafka-inventory"
        },
        "users": {
            "sql": 'SELECT * FROM "devices/users/+"',
            "action": "kafka_kafka-users"
        }
    }

    for data_type, rule_info in rules.items():
        rule_data = {
            "sql": rule_info["sql"],
            "actions": [rule_info["action"]],
            "enable": True,
            "description": f"Forward {data_type} data to Kafka"
        }

        try:
            resp = requests.post(
                f"{BASE_URL}/rules",
                json=rule_data,
                auth=(EMQX_API_KEY, EMQX_API_SECRET) if EMQX_API_KEY else None,
                timeout=5
            )

            if resp.status_code in [201, 200]:
                print(f"  ✓ Rule '{data_type}' created")
            elif "already exists" in resp.text or resp.status_code == 409:
                print(f"  ✓ Rule '{data_type}' already exists")
            else:
                print(f"  ⚠ Rule '{data_type}' issue: {resp.status_code}")
        except Exception as e:
            print(f"  ⚠ Rule '{data_type}' error: {e}")


def main():
    print("=== EMQX Kafka Bridge Setup ===")
    print(f"EMQX: {EMQX_HOST}:{EMQX_PORT}")
    print(f"Kafka: {KAFKA_HOST}:{KAFKA_PORT}")
    print(f"API Key: {EMQX_API_KEY[:10]}..." if EMQX_API_KEY else "API Key: Not configured")
    print()

    if not wait_for_emqx():
        print("\n⚠ Cannot connect to EMQX. Check if it's running.")
        sys.exit(1)

    create_kafka_connector()
    create_bridge_actions()
    create_rules()

    print("\n=== Setup Complete ===")
    print("\nTo verify:")
    print("  1. Check EMQX Dashboard: http://localhost:18083")
    print("  2. Check Kafka UI: http://localhost:8888")
    print("  3. Test with: docker exec emqx mosquitto_pub -h localhost -t 'devices/weather/test' -m '{}'")


if __name__ == "__main__":
    main()
