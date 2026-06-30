#!/usr/bin/env python3
# Health Check Script (Python version)
# Verifies all services and database connectivity

import subprocess
import socket
import sys
import json
from datetime import datetime

class Colors:
    GREEN = '\033[0;32m'
    RED = '\033[0;31m'
    YELLOW = '\033[1;33m'
    NC = '\033[0m'

def check_port(host, port, timeout=2):
    """Check if a port is open"""
    try:
        socket.create_connection((host, port), timeout=timeout)
        return True
    except (socket.timeout, socket.error):
        return False

def check_docker_service(service_name):
    """Check if a Docker service is running"""
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", f"name={service_name}", "--filter", "status=running"],
            capture_output=True,
            text=True,
            timeout=5
        )
        return service_name in result.stdout
    except Exception as e:
        print(f"Error checking Docker: {e}")
        return False

def get_postgres_stats():
    """Get row counts from PostgreSQL tables"""
    try:
        result = subprocess.run(
            [
                "docker", "exec", "postgres", "psql", "-U", "postgres", "-d", "iot_database",
                "-c", "SELECT schemaname, tablename, n_live_tup as row_count FROM pg_stat_user_tables WHERE schemaname='iot' ORDER BY tablename;"
            ],
            capture_output=True,
            text=True,
            timeout=5
        )
        return result.stdout
    except Exception as e:
        return f"Error: {e}"

def check_kafka_topics():
    """List Kafka topics"""
    try:
        result = subprocess.run(
            [
                "docker", "exec", "kafka", "kafka-topics", "--bootstrap-server", "kafka:9092", "--list"
            ],
            capture_output=True,
            text=True,
            timeout=5
        )
        topics = result.stdout.strip().split('\n')
        return [t for t in topics if 'iot-' in t]
    except Exception as e:
        return []

def main():
    print("=== IoT Pipeline Health Check ===")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("")

    # Service status
    print("Service Status:")
    print("-" * 40)
    services = {
        "emqx": 18083,
        "kafka": 9092,
        "postgres": 5432,
        "spark-master": 8080,
        "spark-worker-1": 8081,
        "spark-worker-2": 8082,
    }

    running_count = 0
    for service, port in services.items():
        is_running = check_docker_service(service)
        status = f"{Colors.GREEN}✓ Running{Colors.NC}" if is_running else f"{Colors.RED}✗ Stopped{Colors.NC}"
        
        if is_running:
            running_count += 1
            if check_port("localhost", port):
                print(f"{status}  {service:20} (:{port})")
            else:
                print(f"{Colors.YELLOW}⚠ Running{Colors.NC}  {service:20} (:{port}) - port not responding")
        else:
            print(f"{status}  {service:20}")

    print("")
    print(f"Services Running: {running_count}/6")

    # Kafka topics
    print("")
    print("Kafka Topics:")
    print("-" * 40)
    topics = check_kafka_topics()
    if topics:
        for topic in topics:
            print(f"  • {topic}")
    else:
        print("  (no iot-* topics found)")

    # PostgreSQL statistics
    print("")
    print("PostgreSQL Table Statistics:")
    print("-" * 40)
    stats = get_postgres_stats()
    if "row_count" in stats:
        print(stats)
    else:
        print("  (unable to connect to PostgreSQL)")

    print("")
    print("=== Health Check Complete ===")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted")
        sys.exit(0)
