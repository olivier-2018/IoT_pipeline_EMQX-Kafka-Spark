# IoT Data Pipeline Demo - Architecture & Design

## Overview

This is a minimalist end-to-end IoT data pipeline designed to run on a **12GB laptop**. It demonstrates real-time data ingestion from multiple mock IoT sources, message brokering, streaming processing, and persistent storage.

**Scope**: Mock data generation → MQTT broker → Kafka → Spark processing → PostgreSQL

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    IoT DATA PIPELINE (12GB LAPTOP)              │
└─────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│ DATA GENERATION LAYER (Python)                                   │
├──────────────────────────────────────────────────────────────────┤
│  • weather_generator      → 10-20 msg/min                        │
│  • orders_generator       → 50-100 msg/min                       │
│  • logistics_generator    → 20-50 msg/min                        │
│  • inventory_generator    → 10-30 msg/min                        │
│  • user_events_generator  → 200-500 msg/min                      │
│                                                                   │
│  Target: 500-1000 msg/sec total (configurable)                   │
└──────────────────────────────────────────────────────────────────┘
                            ↓
                     (MQTT Protocol)
                            ↓
┌──────────────────────────────────────────────────────────────────┐
│ MESSAGE BROKERING LAYER                                          │
├──────────────────────────────────────────────────────────────────┤
│ EMQX (MQTT Broker)                                               │
│  • Receives: devices/{device_type}/{attribute}                   │
│  • Forwards: via Kafka bridge → iot.{device_type}.data           │
│  • Port: 1883 (MQTT), 18083 (Dashboard)                          │
│  • Memory: 2GB                                                   │
└──────────────────────────────────────────────────────────────────┘
                            ↓
                  (Kafka Bridge via Rules)
                            ↓
┌──────────────────────────────────────────────────────────────────┐
│ STREAMING & MESSAGING LAYER                                      │
├──────────────────────────────────────────────────────────────────┤
│ Kafka (Single Broker)                                            │
│  Topics (2 partitions each, 10 total):                           │
│    • iot.weather.data          (2 partitions)                    │
│    • iot.orders.events         (2 partitions)                    │
│    • iot.logistics.dispatch    (2 partitions)                    │
│    • iot.inventory.changes     (2 partitions)                    │
│    • iot.users.activity        (2 partitions)                    │
│                                                                   │
│  Configuration:                                                  │
│    • Replication Factor: 1 (no redundancy for demo)              │
│    • Retention: 1 day (aggressive cleanup)                       │
│    • Retention Size: 512 MB per topic                            │
│    • Compression: GZIP                                           │
│  Port: 9092, 29092 (external)                                    │
│  Memory: 2GB                                                     │
└──────────────────────────────────────────────────────────────────┘
                            ↓
                   (Spark Streaming)
                            ↓
┌──────────────────────────────────────────────────────────────────┐
│ PROCESSING & TRANSFORMATION LAYER                                │
├──────────────────────────────────────────────────────────────────┤
│ Apache Spark Cluster (1 Master + 2 Workers)                      │
│                                                                   │
│  Master:                                                         │
│    • Orchestrates job execution                                  │
│    • Port: 7077 (RPC), 8080 (UI)                                 │
│    • Memory: 512 MB                                              │
│                                                                   │
│  Workers (×2):                                                   │
│    • Execute tasks in parallel                                   │
│    • Ports: 8081, 8082 (UI)                                      │
│    • Memory: 1.5GB each                                          │
│                                                                   │
│  Spark Jobs (Streaming):                                         │
│    • ingest_weather.py       → validates, transforms → postgres  │
│    • ingest_orders.py        → enrichment, dedup → postgres      │
│    • ingest_logistics.py     → GPS to POINT → postgres           │
│    • ingest_inventory.py     → aggregation → postgres            │
│    • ingest_user_events.py   → windowed agg → postgres           │
│                                                                   │
│  Each job:                                                       │
│    • Reads from Kafka topic (earliest from latest offset)        │
│    • Validates schema and data ranges                            │
│    • Transforms/enriches data                                    │
│    • Writes micro-batches to PostgreSQL                          │
│    • Batch size: 1000 records                                    │
└──────────────────────────────────────────────────────────────────┘
                            ↓
                  (JDBC Connection Pool)
                            ↓
┌──────────────────────────────────────────────────────────────────┐
│ PERSISTENCE LAYER                                                │
├──────────────────────────────────────────────────────────────────┤
│ PostgreSQL (Single Node)                                         │
│  Tables:                                                         │
│    • weather_data (device_id, temp, humidity, pressure, time)   │
│    • sales_orders (order_id, customer_id, amount, status)        │
│    • logistics_shipments (shipment_id, location, status)         │
│    • inventory_changes (sku, warehouse_id, qty_delta)            │
│    • user_events (user_id, event_type, page, session_id, time)  │
│                                                                   │
│  Configuration:                                                  │
│    • max_connections: 50 (limited for 12GB)                      │
│    • shared_buffers: 128MB                                       │
│    • WAL: minimal (no archiving)                                 │
│    • synchronous_commit: off (faster writes)                     │
│                                                                   │
│  Port: 5432                                                      │
│  Memory: 512 MB                                                  │
│  Data: Docker volume (./data/postgres)                           │
└──────────────────────────────────────────────────────────────────┘
```

---

## Data Flow

### Weather Data Example
```
Python Generator
  → {"device_id": "weather-sensor-01", "temperature": 23.5, ...}
  → MQTT: devices/weather/data
  → EMQX Bridge Rule
  → Kafka: iot.weather.data (partition 0)
  → Spark Job (ingest_weather.py)
  → Validate schema, convert timestamp
  → PostgreSQL: INSERT INTO iot.weather_data (...)
```

### Sales Orders Example
```
Python Generator
  → {"order_id": UUID, "customer_id": 123, "items": [...], "total_amount": 45.99}
  → MQTT: devices/orders/new_order
  → EMQX Bridge Rule
  → Kafka: iot.orders.events (partition based on order_id hash)
  → Spark Job (ingest_orders.py)
  → Dedup by order_id, enrich metadata
  → PostgreSQL: INSERT INTO iot.sales_orders (...)
```

---

## Resource Allocation (12GB Total)

| Component | Memory | CPU | Rationale |
|-----------|--------|-----|-----------|
| **EMQX** | 2 GB | 0.5 | MQTT broker + Kafka bridge; high message rate |
| **Kafka** | 2 GB | 1.0 | Single broker; no replication overhead |
| **Spark Master** | 512 MB | 0.5 | Coordination only; minimal compute |
| **Spark Worker 1** | 1.5 GB | 1.0 | Task execution + shuffle operations |
| **Spark Worker 2** | 1.5 GB | 1.0 | Task execution + shuffle operations |
| **PostgreSQL** | 512 MB | 0.5 | Small schema; indexed queries |
| **Zookeeper** | 1 GB | 0.5 | Kafka coordination |
| **System/Docker** | ~1 GB | — | OS + overhead |
| **Total** | ~11 GB | ~5.0 CPU | **Usable: ~10 GB** (1 GB headroom) |

---

## Kafka Partitioning Strategy

**Why 2 partitions per topic?**
- 2 Spark workers process in parallel
- 2 partitions = 1 per worker (perfect load distribution)
- Low topic count reduces coordination overhead
- Simple to reason about and test

| Topic | Partitions | Partition Key | Throughput | Rationale |
|-------|-----------|---------------|-----------|-----------|
| `iot.weather.data` | 2 | null | 5–10/min | Low frequency; round-robin OK |
| `iot.orders.events` | 2 | order_id | 50–100/min | Medium; per-order sequencing |
| `iot.logistics.dispatch` | 2 | shipment_id | 20–50/min | Medium; per-shipment ordering |
| `iot.inventory.changes` | 2 | item_sku | 10–30/min | Low; per-SKU consistency |
| `iot.users.activity` | 2 | user_id | 200–500/min | High; per-user session ordering |

**Total: 10 partitions across 5 topics**

---

## Design Decisions & Rationale

### 1. **EMQX over NanoMQ/Kafka Connect**
- ✅ EMQX has native Kafka bridge (built-in connector)
- ✅ Rule engine allows data filtering before forwarding
- ✅ MQTT protocol is industry standard for IoT
- ❌ NanoMQ is too minimal for production-like features
- ❌ Kafka Connect MQTT source less mature

### 2. **Single Kafka Broker (no replication)**
- ✅ Saves 50% memory vs. 3-broker cluster
- ✅ Acceptable for demo/dev (data loss OK on crash)
- ✅ Simpler operational model
- ⚠️ Trade-off: no redundancy (acceptable for demo)

### 3. **Spark Micro-Batch (not Streaming)**
- ✅ Lower memory overhead than Spark Structured Streaming
- ✅ Easier to debug (batch boundaries clear)
- ✅ Simpler error recovery (just rerun batch)
- ✅ Native PostgreSQL JDBC integration (no custom sinks)
- ❌ Slightly higher latency (2–5 sec per batch)
- ⚠️ Not true streaming but sufficient for demo

### 4. **PostgreSQL Single Node**
- ✅ Minimal footprint (512 MB)
- ✅ Full ACID compliance
- ✅ GIS support (POINT type for GPS)
- ✅ Suitable for analytical queries
- ❌ No redundancy (acceptable for demo)
- ⚠️ Scaling limited (single instance)

### 5. **1-Day Kafka Retention**
- ✅ Keeps disk usage low (< 512 MB per topic)
- ✅ Forces immediate processing (no backlog accumulation)
- ✅ Acceptable for real-time use case (data flows to DB)
- ❌ No historical replay capability
- ⚠️ May lose data if Spark falls behind

### 6. **Docker Compose Orchestration**
- ✅ Single command startup (`docker compose up -d`)
- ✅ Reproducible across machines
- ✅ Easy teardown (`docker compose down`)
- ✅ Volume persistence built-in
- ⚠️ Not production-ready (use Kubernetes for prod)

---

## Performance Characteristics

### Throughput
- **Data generators**: 500–1000 msg/sec total (tunable)
- **EMQX bridge**: ~1–2k msg/sec overhead per broker
- **Kafka**: ~5k–10k msg/sec (single broker, light compression)
- **Spark processing**: ~500–1000 msg/sec per micro-batch (constrained by PostgreSQL write speed)

### Latency
- MQTT publish → Kafka: **50–100 ms**
- Kafka → Spark reads: **5–10 sec** (batch interval)
- Spark transforms → PostgreSQL write: **2–5 sec**
- **Total end-to-end**: **~15–30 seconds**

### Storage
- Kafka (1 day retention): ~2.5 GB (5 topics × 512 MB)
- PostgreSQL (30 min data): ~500 MB – 1 GB (estimated)
- Total disk: **< 4 GB** (well within 12GB limit)

---

## Monitoring & Debugging

### Web Interfaces
- **EMQX Dashboard**: http://localhost:18083 (user: admin, pass: public)
- **Spark Master UI**: http://localhost:8080
- **Spark Worker 1 UI**: http://localhost:8081
- **Spark Worker 2 UI**: http://localhost:8082

### Command-Line Monitoring
```bash
# Monitor resource usage
docker stats

# View logs
docker logs emqx
docker logs kafka
docker logs spark-master
docker logs spark-worker-1

# Query PostgreSQL
docker exec postgres psql -U postgres -d iot_database \
  -c "SELECT COUNT(*) FROM iot.weather_data;"

# Check Kafka topics
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list

# Check Kafka topic details
docker exec kafka kafka-topics --bootstrap-server kafka:9092 \
  --describe --topic iot.weather.data

# Consume messages from Kafka
docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic iot.weather.data --from-beginning --max-messages 10
```

---

## Scaling Considerations

### To Add More Data Volume
1. Increase `DATA_GENERATION_BATCH_SIZE` in `.env`
2. Add more worker threads in data generators
3. Increase Kafka topic partitions (up to 10 per topic)

### To Add More Spark Workers
1. Add `spark-worker-3` to `docker-compose.yml`
2. Allocate additional memory (e.g., 1.5 GB each)
3. Increase `spark.total.executor.cores` in job submission

### To Reduce 12GB Constraint
- Reduce Kafka retention to 12 hours
- Reduce Spark executor memory (may impact performance)
- Use PostgreSQL column compression

---

## Known Limitations

1. **No Data Replication**: Kafka RF=1; data loss on broker crash
2. **No Spark HA**: Single master; worker loss = partial job failure
3. **No Monitoring**: No Prometheus/Grafana (can add later)
4. **Limited Error Handling**: Bad data dropped silently (can add dead-letter queue)
5. **No Authentication**: EMQX/Kafka/Postgres all open (demo only)
6. **Single PostgreSQL**: No failover; node loss = data unavailable

---

## Next Steps

- See [SETUP.md](SETUP.md) for quick start (5 minutes to running)
- See [DEPLOYMENT.md](DEPLOYMENT.md) for tuning and troubleshooting
- See [DATA_SCHEMA.md](DATA_SCHEMA.md) for detailed schema definitions
- See [DEVELOPMENT.md](DEVELOPMENT.md) for local testing and extending
