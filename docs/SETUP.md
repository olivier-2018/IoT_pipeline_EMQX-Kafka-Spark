# IoT Data Pipeline - Quick Setup Guide

## Prerequisites

- **Docker**: 20.10+ (includes Docker Compose CLI)
- **Python**: 3.9+
- **RAM**: 12 GB available
- **Disk**: 5 GB free space
- **OS**: Linux, macOS, or Windows with Docker Desktop

### Verify Prerequisites
```bash
docker --version          # e.g., Docker version 24.0.0
docker compose version  # e.g., Docker Compose version 2.20.0
python3 --version         # e.g., Python 3.10.12
```

---

## 5-Minute Quick Start

### Step 1: Start All Services (2 min)
```bash
cd /home/sirius/TUTORIALS/Spark_Kafka_Docker

# Start Docker containers
bash scripts/start.sh
```

Expected output:
```
=== Starting IoT Data Pipeline ===
Step 1: Starting Docker containers...
Step 2: Waiting for services to stabilize...
Step 3: Initializing Kafka topics...
Step 4: Running health checks...
=== Startup Complete ===
```

**What just happened:**
- All 7 Docker containers started (EMQX, Kafka, Zookeeper, Spark master, 2 workers, PostgreSQL)
- Kafka topics created with proper partitioning
- PostgreSQL schema initialized with 5 tables

### Step 2: Verify Everything is Running (30 sec)
```bash
# Check service status
docker compose ps

# Should show all services as "Up"
```

Or use the Python health check:
```bash
python3 monitoring/health_check.py
```

### Step 3: Install Data Generator Dependencies (1 min)
```bash
cd data-generators
pip install -r requirements.txt
```

### Step 4: Start Data Generators (in new terminal)
```bash
cd data-generators
python3 main.py
```

Expected output:
```
=== Starting Generator Orchestrator ===
Duration: 30 minutes
Batch Frequency: 60 seconds
Target Throughput: 500 msg/sec

Connecting All Generators...
✓ All generators connected!

[Batch 1] Published: 600 | Total: 600 | Throughput: 10.0 msg/sec | Cycle Time: 0.45s
[Batch 2] Published: 590 | Total: 1190 | Throughput: 9.8 msg/sec | Cycle Time: 0.48s
...
```

### Step 5: Submit Spark Jobs (in another terminal)
```bash
# Go to project root
cd /home/sirius/TUTORIALS/Spark_Kafka_Docker

# Submit all Spark jobs
bash scripts/submit-spark-jobs.sh
```

Expected output:
```
=== Submitting Spark Jobs ===
Spark Master: spark://spark-master:7077

Submitting: ingest_weather.py
  ✓ Submitted (running in background)
Submitting: ingest_orders.py
  ✓ Submitted (running in background)
...
=== All Spark Jobs Submitted ===
```

### Step 6: Verify Data Flow (1 min)
```bash
# Check PostgreSQL for data
docker exec postgres psql -U postgres -d iot_database -c "\
  SELECT \
    (SELECT COUNT(*) FROM iot.weather_data) as weather, \
    (SELECT COUNT(*) FROM iot.sales_orders) as orders, \
    (SELECT COUNT(*) FROM iot.logistics_shipments) as logistics, \
    (SELECT COUNT(*) FROM iot.inventory_changes) as inventory, \
    (SELECT COUNT(*) FROM iot.user_events) as user_events;"
```

Expected output (after 2–3 minutes):
```
 weather | orders | logistics | inventory | user_events
---------+--------+-----------+-----------+-------------
      10 |    100 |        50 |        30 |        500
```

---

## Monitoring the System

### 1. Watch Real-Time Data Growth
```bash
# Terminal 1: Monitor Docker resource usage
docker stats

# Terminal 2: Monitor database in real-time
while true; do
  docker exec postgres psql -U postgres -d iot_database -c "\
    SELECT \
      (SELECT COUNT(*) FROM iot.weather_data) as weather, \
      (SELECT COUNT(*) FROM iot.sales_orders) as orders;"
  sleep 10
done
```

### 2. Open Web Dashboards

| Service | URL | Login |
|---------|-----|-------|
| EMQX | http://localhost:18083 | admin / public |
| Spark Master | http://localhost:8080 | (none) |
| Spark Worker 1 | http://localhost:8081 | (none) |
| Spark Worker 2 | http://localhost:8082 | (none) |

### 3. View Logs

```bash
# View EMQX logs
docker logs emqx -f

# View Kafka logs
docker logs kafka -f

# View Spark master logs
docker logs spark-master -f

# View a specific Spark job
docker logs spark-worker-1 -f | grep "Weather\|Orders\|Logistics"
```

---

## Stopping the System

### Option 1: Stop (preserve data)
```bash
bash scripts/stop.sh
```

Data will persist in `./data/postgres`. Restart with:
```bash
docker compose up -d
```

### Option 2: Reset (clear all data)
```bash
bash scripts/reset.sh
```

This will:
1. Stop all containers
2. Delete volumes
3. Clear local data directories
4. Restart fresh

---

## Testing Individual Components

### Test MQTT → EMQX
```bash
# Subscribe to MQTT topic (in one terminal)
docker exec emqx mosquitto_sub -h localhost -t "devices/+/+" 

# In another terminal, publish a message
docker exec emqx mosquitto_pub -h localhost -t "devices/test/data" -m '{"test": "message"}'

# You should see the message appear in the subscriber
```

### Test Kafka Topics
```bash
# List all topics
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list

# Describe a topic
docker exec kafka kafka-topics --bootstrap-server kafka:9092 \
  --describe --topic iot.weather.data

# Consume messages
docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic iot.weather.data --from-beginning --max-messages 5
```

### Test PostgreSQL Connection
```bash
docker exec postgres psql -U postgres -d iot_database -c \
  "SELECT * FROM iot.weather_data ORDER BY ingested_at DESC LIMIT 3;"
```

### Test Spark Job Submission
```bash
# Submit just the weather job
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 512m \
  --executor-memory 1g \
  --executor-cores 1 \
  --total-executor-cores 2 \
  /spark-jobs/ingest_weather.py

# Monitor in Spark UI: http://localhost:8080
```

---

## Troubleshooting

### Services Won't Start
```bash
# Check Docker daemon is running
docker ps

# Check disk space
df -h /

# Check if ports are already in use
lsof -i :1883 :9092 :5432 :7077 :8080

# If ports in use, reset and try again
bash scripts/reset.sh
```

### No Data in PostgreSQL
1. Check data generators are running: `ps aux | grep python`
2. Check MQTT messages reach EMQX: `docker exec emqx mosquitto_sub -h localhost -t "devices/+/+"`
3. Check Kafka has messages: `docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic iot.weather.data --max-messages 5`
4. Check Spark jobs are running: http://localhost:8080
5. Check Spark logs: `docker logs spark-master`

### Out of Memory
```bash
# Check memory usage
docker stats

# Reduce batch size in .env
# Restart: docker compose down && docker compose up -d
```

### PostgreSQL Connection Refused
```bash
# Verify PostgreSQL is running
docker ps | grep postgres

# Check PostgreSQL logs
docker logs postgres

# Restart PostgreSQL
docker compose restart postgres
```

---

## Next Steps

- Read [DEPLOYMENT.md](DEPLOYMENT.md) for tuning and performance tips
- Read [DATA_SCHEMA.md](DATA_SCHEMA.md) for detailed schema info
- Read [DEVELOPMENT.md](DEVELOPMENT.md) to run Spark jobs locally
- See [ARCHITECTURE.md](ARCHITECTURE.md) for system design details
