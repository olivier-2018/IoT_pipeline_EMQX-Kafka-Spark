# IoT Data Pipeline Demo

A minimalist **end-to-end IoT data pipeline** designed to run on a **12GB laptop**. 

Demonstrates real-time ingestion of mock IoT data (weather, sales, logistics, inventory, user events) from **Python generators** → **MQTT broker (EMQX)** → **Kafka** → **Spark cluster** → **PostgreSQL**.

---

## Quick Start (5 minutes)

### 1. Start Docker Services
```bash
cd /home/sirius/TUTORIALS/Spark_Kafka_Docker
bash scripts/start.sh
```

### 2. Start Data Generators (new terminal)
```bash
cd data-generators
pip install -r requirements.txt
python3 main.py
```

### 3. Submit Spark Jobs (another terminal)
```bash
bash scripts/submit-spark-jobs.sh
```

### 4. View Results
```bash
# Query PostgreSQL
docker exec postgres psql -U postgres -d iot_database -c \
  "SELECT COUNT(*) FROM iot.weather_data;"

# Open dashboards
# EMQX: http://localhost:18083
# Spark Master: http://localhost:8080
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
├── docker-compose.yml              # All 7 services orchestrated
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
│   ├── DEPLOYMENT.md               # Tuning, benchmarks, monitoring
│   ├── DATA_SCHEMA.md              # Schemas, topics, ranges
│   └── DEVELOPMENT.md              # ✨ Local testing in IDE, extending
├── data-generators/                # Python mock IoT producers
│   ├── config.py                   # Shared configuration
│   ├── weather_generator.py        # Weather (5–10/min)
│   ├── orders_generator.py         # Sales orders (50–100/min)
│   ├── logistics_generator.py      # Shipment tracking (20–50/min)
│   ├── inventory_generator.py      # Stock changes (10–30/min)
│   ├── user_events_generator.py    # User events (200–500/min)
│   ├── main.py                     # Orchestrator (parallel, throttled)
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
| [**DEPLOYMENT.md**](docs/DEPLOYMENT.md) | Performance tuning, benchmarks, monitoring, scaling |
| [**DATA_SCHEMA.md**](docs/DATA_SCHEMA.md) | PostgreSQL schemas, Kafka topics, MQTT structure, data ranges |
| [**DEVELOPMENT.md**](docs/DEVELOPMENT.md) | **Local Spark testing in IDE**, adding new data types |

---

## Key Features

✅ **5 Mock Data Types**: Weather, sales orders, logistics packing/dispatch, inventory, user events  
✅ **EMQX + Kafka**: MQTT broker with native Kafka bridge integration  
✅ **Spark Cluster**: 1 master + 2 workers on Docker with micro-batch streaming  
✅ **PostgreSQL**: Fully indexed schema with 5 tables, persistent volumes  
✅ **Minimalistic**: Optimized for 12GB laptop (aggressive resource limits)  
✅ **Kafka Partitioning**: 2 partitions per topic (10 total) for perfect Spark worker distribution  
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

# View dashboards
# EMQX: http://localhost:18083 (admin/public)
# Spark: http://localhost:8080

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
| Kafka | 2 GB | 1.0 | Single broker, 10 topics |
| Spark Master | 512 MB | 0.5 | Coordination |
| Spark Worker 1 | 1.5 GB | 1.0 | Task execution |
| Spark Worker 2 | 1.5 GB | 1.0 | Task execution |
| PostgreSQL | 512 MB | 0.5 | Data warehouse |
| Zookeeper | 1 GB | 0.5 | Kafka coordination |
| System | ~1 GB | — | OS overhead |
| **Total** | **~11 GB** | **~5.0 CPU** | **12GB Recommended** |

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
2. **Understand Design** → Read [ARCHITECTURE.md](docs/ARCHITECTURE.md)
3. **Local Development** → See [DEVELOPMENT.md](docs/DEVELOPMENT.md) for IDE testing
4. **Production Tuning** → Check [DEPLOYMENT.md](docs/DEPLOYMENT.md) for optimization
5. **Extend** → Add new data types following [DEVELOPMENT.md](docs/DEVELOPMENT.md)

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
