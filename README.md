# IoT Data Pipeline Demo

A minimalist **end-to-end IoT data pipeline** designed to run on a **12GB laptop**. 

Demonstrates real-time ingestion of mock IoT data (weather, sales, logistics, inventory, user events) from **Python generators** → **MQTT broker (EMQX)** → **Kafka** → **Spark cluster** → **PostgreSQL**.

---

## Pre-requirements (5 minutes)

### 1. install uv
```bash

```
### 2. install python venv
```bash
uv venv --python 3.12 .venv 
source .venv/bin/activate  

```

### 2. install python venv
```bash
uv sync
```


---

## Quick Start (5 minutes)

### 1. Start Docker Services
```bash
cd Spark_Kafka_Docker
bash scripts/start.sh

# create API key
# docker exec emqx emqx ctl api_keys add --name setup-key --desc "Setup script API key"

```

### 2. Start MQTT Generators (new terminal)
```bash
source .venv/bin/activate    
cd mqtt-generators
python mqtt-generators/main.py
```  
Note: the generators should now be visible as clients in the EMQX dashboards.    

### 3. Check EMQX logs for Kafka bridge:
```bash
docker logs emqx 2>&1 | grep -i "kafka\|bridge" | head -20
# API calls
# curl -s -u <EMQX_API_KEY>:<EMQX_API_SECRET> http://localhost:18083/api/v5/
# curl -s -u <EMQX_API_KEY>:<EMQX_API_SECRET> http://localhost:18083/api/v5/rules
```

### 4. Verify Kafka is receiving messages:
```bash
docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic iot-weather-data --from-beginning --max-messages 5 --timeout-ms 5000
# Check kafka-ui on localhost:8888
```

### 5. Monitor live MQTT messages:

```bash
# In one terminal, subscribe to MQTT
docker exec emqx mosquitto_sub -h localhost -t "devices/weather/+" -v

# In another terminal, publish test message
docker exec emqx mosquitto_pub -h localhost -t "devices/weather/test" -m '{"test":"data","timestamp":1234567890000}'
```

### 6. Submit Spark Jobs (another terminal)
```bash
bash scripts/submit-spark-jobs.sh
```

### 7. View Results & Dashboards
```bash
# Query PostgreSQL
docker exec postgres psql -U postgres -d iot_database -c \
  "SELECT COUNT(*) FROM iot.weather_data;"

# Open web dashboards
# Node-Red (MQTT testing/visualization):  http://localhost:1880
# EMQX (MQTT broker):                      http://localhost:18083 (admin/public)
# Kafka UI (topics, messages, partitions): http://localhost:8888
# Spark Master (job monitoring):           http://localhost:8080
# Spark Worker 1:                          http://localhost:8081
# Spark Worker 2:                          http://localhost:8082
```

---

## Architecture at a Glance

```
Python Generators (5 types) 
  ↓ (MQTT: 500-1000 msg/sec)
EMQX Broker
  ↓ (Kafka Bridge)
Kafka (5 topics, 2 partitions each)
  ↓ (Streaming Read)
Spark Cluster (1 master + 2 workers)
  ↓ (JDBC Write)
PostgreSQL (5 tables, fully indexed)
```

**Key Stats**:
- **Memory**: 10GB (2GB EMQX, 2GB Kafka, 3GB Spark, 512MB PG, 1GB system)
- **Latency**: 15–30 sec end-to-end
- **Throughput**: 500–1000 msg/sec configurable
- **Data**: 5 types (weather, orders, logistics, inventory, user events)

---

## Project Structure

```
iot-pipeline-demo/
├── docker-compose.yml              # All 8 services orchestrated
├── .env                            # Configuration (passwords, ports)
├── scripts/
│   ├── start.sh                    # Full startup with health checks
│   ├── reset.sh                    # ✨ Full reset (clear volumes + restart)
│   ├── stop.sh                     # Stop containers (preserve data)
│   ├── submit-spark-jobs.sh        # Submit 5 Spark jobs to cluster
│   ├── init-kafka-topics.sh        # Initialize Kafka topics
│   └── health-check.sh             # Verify all services ready
├── docs/
│   ├── ARCHITECTURE.md             # System design, data flow, diagrams
│   ├── SETUP.md                    # Quick start + troubleshooting
│   ├── MQTT_SCHEMAS.md             # ✨ MQTT message format definitions (JSON schemas)
│   ├── DATA_SCHEMA.md              # PostgreSQL/Kafka schemas, topics, ranges
│   ├── DEPLOYMENT.md               # Tuning, benchmarks, monitoring
│   └── DEVELOPMENT.md              # ✨ Local testing in IDE, extending
├── mqtt-generators/                # Python mock IoT MQTT producers
│   ├── config.py                   # Shared MQTT configuration
│   ├── weather_generator.py        # Weather data (10–20/min)
│   ├── orders_generator.py         # Sales orders (50–100/min)
│   ├── logistics_generator.py      # Shipment tracking (20–50/min)
│   ├── inventory_generator.py      # Inventory changes (10–30/min)
│   ├── user_events_generator.py    # User events (200–500/min)
│   ├── main.py                     # Orchestrator (parallel generators, throttled)
│   └── requirements.txt
├── spark-jobs/                     # Spark ingestion & transformation
│   ├── shared_utils.py             # Factories, schemas, JDBC pooling
│   ├── ingest_weather.py           # Kafka → PostgreSQL (weather)
│   ├── ingest_orders.py            # Kafka → PostgreSQL (orders)
│   ├── ingest_logistics.py         # Kafka → PostgreSQL (logistics)
│   ├── ingest_inventory.py         # Kafka → PostgreSQL (inventory)
│   ├── ingest_user_events.py       # Kafka → PostgreSQL (user events)
│   └── requirements.txt
├── monitoring/
│   └── health_check.py             # Status & connectivity verification
├── config/
│   ├── emqx/                       # MQTT broker config
│   ├── kafka/                      # Kafka broker config
│   ├── spark/                      # Spark env & logging
│   └── postgres/                   # PostgreSQL schema + init
└── data/                           # Persistent volumes (auto-created)
    └── postgres/                   # Database files
```

---

## Documentation

| Document | Purpose |
|----------|---------|
| [**ARCHITECTURE.md**](docs/ARCHITECTURE.md) | System design, component rationale, data flow diagrams, performance characteristics |
| [**SETUP.md**](docs/SETUP.md) | 5-minute quickstart, prerequisites, troubleshooting |
| [**MQTT_SCHEMAS.md**](docs/MQTT_SCHEMAS.md) | **Complete MQTT message definitions** (JSON schemas for all 5 data types) |
| [**DATA_SCHEMA.md**](docs/DATA_SCHEMA.md) | PostgreSQL schemas, Kafka topics, data ranges, monitoring queries |
| [**DEPLOYMENT.md**](docs/DEPLOYMENT.md) | Performance tuning, benchmarks, monitoring, scaling |
| [**DEVELOPMENT.md**](docs/DEVELOPMENT.md) | **Local Spark testing in IDE**, adding new data types |

---

## Key Features

✅ **5 Mock Data Types**: Weather, sales orders, logistics, inventory, user events  
✅ **EMQX + Kafka**: MQTT broker with native Kafka bridge integration  
✅ **Spark Cluster**: 1 master + 2 workers on Docker with micro-batch streaming  
✅ **PostgreSQL**: Fully indexed schema with 5 tables, persistent volumes  
✅ **Node-Red**: ✨ MQTT testing, visualization, and flow automation dashboard  
✅ **Minimalistic**: Optimized for 12GB laptop (aggressive resource limits)  
✅ **Persistent Storage**: Kafka, Zookeeper, PostgreSQL, and EMQX data preserved across restarts  
✅ **Kafka Partitioning**: 2 partitions per topic for optimal Spark worker distribution  
✅ **Comprehensive Docs**: MQTT schemas, architecture, deployment, and local development guides  
✅ **Reset Script**: `bash scripts/reset.sh` for clean demo restarts  
✅ **Local Testing**: Run Spark jobs locally in IDE for debugging  
✅ **Production-Ready Code**: Proper error handling, logging, validation  

---

## Commands Reference

```bash
# Start full system
bash scripts/start.sh

# View service status
docker compose ps
docker stats

# Reset to clean state
bash scripts/reset.sh

# Verify health
bash scripts/health-check.sh
python3 monitoring/health_check.py

# View web dashboards
# Node-Red: http://localhost:1880
# EMQX: http://localhost:18083 (admin/public)
# Spark: http://localhost:8080
# Kafka UI: http://localhost:8888

# Query data
docker exec postgres psql -U postgres -d iot_database -c \
  "SELECT COUNT(*) FROM iot.weather_data;"

# View logs
docker logs emqx -f
docker logs spark-master -f
docker logs spark-worker-1 -f

# Stop (preserve data)
bash scripts/stop.sh

# Stop and clear all data
docker compose down && rm -rf ./data/*
```

---

## Resource Allocation

| Service | Memory | CPU | Purpose |
|---------|--------|-----|---------|
| EMQX | 2 GB | 0.5 | MQTT broker + Kafka bridge |
| Kafka | 2 GB | 1.0 | Single broker, 5 topics, persistent volume |
| Zookeeper | 1 GB | 0.5 | Kafka coordination, persistent volume |
| Kafka UI | 512 MB | 0.5 | Kafka visualization & monitoring |
| Spark Master | 512 MB | 0.5 | Job coordination & scheduling |
| Spark Worker 1 | 1.5 GB | 1.0 | Task execution & streaming processing |
| Spark Worker 2 | 1.5 GB | 1.0 | Task execution & streaming processing |
| PostgreSQL | 512 MB | 0.5 | Data warehouse, persistent volume |
| Node-Red | 512 MB | 0.5 | ✨ MQTT testing, visualization, flow automation |
| System | ~1 GB | — | OS overhead |
| **Total** | **~12 GB** | **~5.5 CPU** | **12GB Recommended** |

---

## Performance Benchmarks

- **Data Generation**: 500–1000 msg/sec (configurable)
- **EMQX → Kafka**: ~1–2k msg/sec
- **Kafka → Spark**: ~500 msg/sec (limited by Spark parallelism)
- **Spark → PostgreSQL**: ~200–500 rows/sec (JDBC batch limited)
- **E2E Latency**: 15–30 seconds
- **Memory Usage**: ~9–10 GB at capacity
- **Disk I/O**: ~10–20 MB/sec

---

## Next Steps

1. **Quick Start** → Follow [SETUP.md](docs/SETUP.md)
2. **Understand Data Formats** → See [MQTT_SCHEMAS.md](docs/MQTT_SCHEMAS.md) for all 5 data type definitions
3. **Understand Design** → Read [ARCHITECTURE.md](docs/ARCHITECTURE.md)
4. **Local Development** → See [DEVELOPMENT.md](docs/DEVELOPMENT.md) for IDE testing & extending
5. **Production Tuning** → Check [DEPLOYMENT.md](docs/DEPLOYMENT.md) for optimization & monitoring

---

## Known Limitations

- ⚠️ No data replication (Kafka RF=1; data loss on crash)
- ⚠️ No Spark HA (single master)
- ⚠️ No monitoring/alerting (can add Prometheus later)
- ⚠️ No TLS/auth (demo-only setup)
- ⚠️ PostgreSQL single node (no failover)

---

## License & Attribution

This project demonstrates real-world IoT pipeline architecture using open-source components:
- **Apache Kafka** (message streaming)
- **Apache Spark** (stream processing)
- **EMQX** (MQTT broker)
- **PostgreSQL** (data warehouse)
- **Docker** (containerization)

---

## Questions?

- See [SETUP.md](docs/SETUP.md) for troubleshooting
- See [DEPLOYMENT.md](docs/DEPLOYMENT.md) for tuning & monitoring
- See [DEVELOPMENT.md](docs/DEVELOPMENT.md) for local testing & development

## Acknoledgement

[EMQX setup](https://www.youtube.com/watch?v=Xqdg3rUSYRc)  
[EQMX Kafka integration Doc](https://docs.emqx.com/en/emqx/latest/data-integration/data-bridge-kafka.html)  


