# IoT Data Pipeline - Quick Setup Guide

## Prerequisites

- **Docker**: 20.10+ (includes Docker Compose CLI)
- **Python**: 3.9+
- **RAM**: 18 GB available
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
cd Spark_Kafka_Docker

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
- All 8 Docker containers started (EMQX, Kafka, Zookeeper, Kafka UI, Spark master, 2 workers, PostgreSQL)
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


### Step 3: Submit Spark Jobs (in new terminal)

> **These are Spark Structured Streaming jobs, not batch jobs.** Each one, once
> submitted, keeps running forever - continuously reading new Kafka messages
> and writing them to PostgreSQL - until you explicitly stop it or stop the
> containers. There is nothing to "wait for it to finish"; a healthy job simply
> never exits on its own.

Submit these **before** starting the generators: each job uses
`startingOffsets=latest`, so it only sees messages published after it started -
starting it first means no messages get missed once the generators kick in.

```bash
# Go to project root
cd Spark_Kafka_Docker

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

Each job's driver output goes to a log file under `logs/submit-spark-jobs/`
(not `docker logs spark-master`, since the driver runs as a separate
`docker exec` process - see [ARCHITECTURE.md](ARCHITECTURE.md#viewing-spark-job-logs)).
Confirm a job is actually running (rather than having crashed on submit) via
the [Spark Master UI](http://localhost:8080) - it should be listed under
"Running Applications" - or:
```bash
docker exec spark-master ps aux | grep SparkSubmit
```
To stop a specific job:
```bash
docker exec spark-master pkill -f ingest_weather.py
```

### Step 4: Start MQTT Generators (in another terminal)
```bash
source .venv/bin/activate    
cd mqtt-generators
python mqtt-generators/main.py
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

### Step 5: Verify Data Flow (1 min)
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
| Node-Red |  http://localhost:1880 |  NA |
| EMQX | http://localhost:18083 | admin / public |
| Kafka UI | http://localhost:8888 | (none) |
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
# Requirement:  a local install of mosquitto-clients 
sudo apt install mosquitto-clients

# Subscribe to MQTT topic (in one terminal)
mosquitto_sub -h localhost -t "devices/+/+"

# In another terminal, publish a message
mosquitto_pub -h localhost -t "devices/test/data" -m '{"test": "message"}'

# You should see the message appear in the subscriber

### ALTERNATIVE without a mosquitto-client
# Subscribe 
docker run --rm --network iot-network -it efrecon/mqtt-client mosquitto_sub -h emqx -t "devices/+/+"
# publish

```

### Test Kafka Topics
```bash
# List all topics
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list

# Describe a topic
docker exec kafka kafka-topics --bootstrap-server kafka:9092 \
--describe --topic iot-weather-data

# Get number of messages in each partition
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
--broker-list kafka:9092 --topic iot-weather-data

# Check retention period (in seconds)
docker exec kafka kafka-configs --bootstrap-server kafka:9092 \
  --entity-type topics --entity-name iot-weather-data --describe

# Consume 2 messages 
docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic iot-weather-data \
  --from-beginning --group my-shell-group --max-messages 2 \
  --property print.partition=true \
  --property print.offset=true

# Note 1:
# Help on kafka-console-consumer --> docker exec kafka kafka-console-consumer --help 

# Note 2: 
# Other useful flags 
#  --from-beginning --> start with the earliest msg from consumer group 
#  --group <String: consumer group id> --> if no group, an effemeral group is created. Consumer groups are persisted on broker side !!
#  --include <String: Java regex (String)>  --> Regex expression specifying list of topics to include for consumption. 

# Note 3: 
# Other useful --property flags for the same consumer:
#  print.key=true — show the message key (useful here since your devices likely key by device_id)
#  print.timestamp=true — show the Kafka record timestamp
#  print.headers=true — show any headers

# List and delete consumer groups on broker
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --list
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --delete --group my-shell-group
```


### Test PostgreSQL Connection
```bash
docker exec postgres psql -U postgres -d iot_database -c \
  "SELECT * FROM iot.weather_data ORDER BY ingested_at DESC LIMIT 3;"
```


### Test Spark Job Submission
```bash
# Submit just the weather job (shared_utils.zip ships the shared_utils package
# to executors via --py-files; scripts/submit-spark-jobs.sh regenerates it
# automatically if missing or stale)
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --driver-memory 512m \
  --py-files /opt/spark-structured-streaming-jobs/shared_utils.zip \
  /opt/spark-structured-streaming-jobs/ingest_weather.py

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

### Spark 
```bash
# Check Spark master's active applications via REST
docker exec spark-master curl -s http://localhost:8080/json/ | python3 -c "
import json,sys
d = json.load(sys.stdin)
for app in d.get('activeapps', []):
    print(app['id'], app['name'], app['state'], app['cores'], app['memoryperslave'])
"

# Check kafka Offsets for all topics
echo "---kafka offsets now---"
for t in iot-weather-data iot-orders-events iot-logistics-dispatch iot-inventory-changes iot-users-activity; do
  echo -n "$t: "
  docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:9092 --topic "$t" | tr '\n' ' '
  echo
done

# Check latest checkpoint batch numbers for each job
for job in weather orders logistics inventory user_events; do
  echo "=== $job checkpoint ==="
  docker exec spark-master find /tmp/spark-data/checkpoints/$job/offsets -type f -name "[0-9]*" 2>&1 | sort | tail -3
done

# Check structure streaming jobs are running
docker exec spark-master curl -s http://localhost:8080/json/ | python3 -c "
import json,sys
d = json.load(sys.stdin)
for app in d.get('activeapps', []):
    print(app['name'], app['state'])
"

#
```

### No Data in PostgreSQL
1. Check data generators are running: `ps aux | grep python`
2. Check MQTT messages reach EMQX: the `emqx` image has no `mosquitto_sub` binary (see "Test MQTT → EMQX" above for a working alternative)
3. Check Kafka has messages: `docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic iot-weather-data --max-messages 5`
4. Check Spark jobs are running: http://localhost:8080
5. Check Spark driver logs: `docker logs spark-master` only shows the Master daemon's own log, **not** the driver JVMs (`--deploy-mode client` drivers are separate `docker exec` processes) - check `logs/submit-spark-jobs/*.log` instead, or the checkpoint offsets directly (see below) since driver log output can sit in Python's stdout buffer for a while before appearing in the file

# check all postgres row counts and weather's latest ingestion timestamp
```bash
docker exec postgres psql -U postgres -d iot_database -c "
SELECT
  (SELECT COUNT(*) FROM iot.weather_data) as weather,
  (SELECT COUNT(*) FROM iot.sales_orders) as orders,
  (SELECT COUNT(*) FROM iot.logistics_shipments) as logistics,
  (SELECT COUNT(*) FROM iot.inventory_changes) as inventory,
  (SELECT COUNT(*) FROM iot.user_events) as user_events;"
echo "---weather max ingested_at---"
docker exec postgres psql -U postgres -d iot_database -c "SELECT max(ingested_at) FROM iot.weather_data;"
```

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
