# IoT Data Pipeline - Deployment & Troubleshooting

## Performance Tuning

### Memory Optimization

#### Reduce Memory Footprint
If you're running into OOM errors or want to free up memory:

**Option 1: Reduce Spark Worker Memory**
```yaml
# In docker-compose.yml
spark-worker-1:
  mem_limit: 1g      # Reduced from 1500m
  environment:
    SPARK_WORKER_MEMORY: 512m  # Reduced from 1g
```

**Option 2: Reduce Kafka Retention**
```bash
# Reduce from 1 day to 12 hours
docker exec kafka kafka-topics --bootstrap-server kafka:9092 \
  --alter --topic iot-weather-data \
  --config retention.ms=43200000
```

**Option 3: Reduce Data Generation Batch Size**
```bash
# In .env
DATA_GENERATION_BATCH_SIZE=250  # Reduced from 500
```

**Expected Savings**:
- Option 1: ~500 MB per worker
- Option 2: ~256 MB per topic
- Option 3: Reduced throughput, lower peak memory

### CPU Optimization

#### Pin CPU Cores (Linux only)
```bash
# Limit EMQX to 0.5 CPU cores
docker update --cpus 0.5 emqx

# Restart to apply
docker compose restart
```

### Disk I/O Optimization

#### Enable Kafka Compression
Already enabled (GZIP), but can tune:

```bash
# Use more aggressive compression (slower but smaller)
docker exec kafka kafka-topics --bootstrap-server kafka:9092 \
  --alter --topic iot-weather-data \
  --config compression.type=snappy  # or lz4
```

---

## Performance Benchmarks

### Baseline Performance (12GB laptop, default settings)

| Metric | Value | Notes |
|--------|-------|-------|
| **Data Generation** | 500–1000 msg/sec | Configurable; limited by network I/O |
| **EMQX → Kafka Bridge** | ~1–2k msg/sec | Single bridge with batching |
| **Kafka → Spark Read** | ~500 msg/sec | Limited by Spark parallelism |
| **Spark → PostgreSQL Write** | ~200–500 rows/sec | Limited by JDBC batch size & DB writes |
| **E2E Latency** | 15–30 sec | MQTT → DB |
| **Memory Usage** | ~9–10 GB | All services running at capacity |
| **CPU Usage** | ~3–4 cores | Mostly Spark workers & Kafka |
| **Disk I/O** | ~10–20 MB/sec | Kafka segment rotation & PostgreSQL WAL |

### Bottlenecks

**Most Likely Bottleneck**: PostgreSQL write speed
- JDBC batch inserts limited by `batchsize` (currently 1000)
- Single node; no write parallelism
- Synchronous commit disabled to improve speed

**Mitigation**:
```python
# In spark-jobs/shared_utils/shared_utils.py
jdbc_options = {
    "batchsize": 2000,      # Increase batch size
    "numPartitions": 8,     # Increase partition parallelism
}
```

---

## Troubleshooting Guide

### Issue: "Out of Memory" Error

**Symptoms**:
```
java.lang.OutOfMemoryError: Java heap space
Error: docker: out of memory
```

**Diagnosis**:
```bash
# Check which container is using most memory
docker stats --no-stream | sort -k4 -h

# Check available system memory
free -h
```

**Solutions** (in order):
1. Stop unnecessary services: `docker compose down`
2. Reduce Spark worker memory (see above)
3. Reduce data generation batch size
4. Increase system swap (temporary, slow): `sudo swapon -s`

---

### Issue: "Connection refused" Error

**Symptoms**:
```
Error: org.postgresql.util.PSQLException: Connection to localhost:5432 refused
```

**Diagnosis**:
```bash
# Check if PostgreSQL is running
docker ps | grep postgres

# Check if port 5432 is open
netstat -tuln | grep 5432

# Check PostgreSQL logs
docker logs postgres
```

**Solutions**:
1. Restart PostgreSQL: `docker compose restart postgres`
2. Wait longer for startup (first run takes 30 sec): `sleep 30`
3. Check password in .env matches POSTGRES_PASSWORD
4. Force reset: `bash scripts/reset.sh`

---

### Issue: "No data flowing to PostgreSQL"

**Symptoms**:
```
SELECT COUNT(*) FROM iot.weather_data;  -- Returns 0 after 5 minutes
```

**Diagnosis Flowchart**:
```
1. Are data generators running?
   docker ps | grep -E "emqx|kafka|spark|postgres"
   
2. Are MQTT messages being published?
   docker exec emqx mosquitto_sub -h localhost -t "devices/+/+" &
   
3. Are Kafka topics receiving messages?
   docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 \
     --topic iot-weather-data --max-messages 1
   
4. Are Spark jobs running?
   docker logs spark-master | grep "WeatherDataIngestion"
   
5. Are there Spark errors?
   docker logs spark-worker-1 | grep ERROR
   
6. Can Spark connect to PostgreSQL?
   docker exec spark-master nc -zv postgres 5432
```

**Common Causes & Fixes**:

| Cause | Fix |
|-------|-----|
| MQTT generators not started | `cd mqtt-generators && python main.py` |
| MQTT → Kafka bridge not configured | Restart EMQX: `docker compose restart emqx` |
| Spark job submission failed | Check `docker logs spark-master` |
| PostgreSQL connection pool exhausted | Reduce Spark parallelism: `--total-executor-cores 1` |
| Schema mismatch | Verify column names match in `shared_utils/shared_utils.py` |

---

### Issue: "Spark Job Fails with JDBC Error"

**Symptoms**:
```
org.postgresql.util.PSQLException: ERROR: duplicate key value violates unique constraint
```

**Cause**: Duplicate records being inserted (not idempotent)

**Solution**:
```python
# In ingest_weather.py, use upsert instead of append:
batch_df.write \
    .format("jdbc") \
    .options(**jdbc_options) \
    .mode("ignore")  # Skip duplicates instead of append
    .save()
```

---

### Issue: "Kafka Topics Not Created"

**Symptoms**:
```bash
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list
# (empty or missing iot-* topics)
```

**Solution**:
```bash
# Manually run topic initialization
bash scripts/init-kafka-topics.sh

# Or create manually
docker exec kafka kafka-topics --bootstrap-server kafka:9092 \
  --create --if-not-exists --topic iot-weather-data \
  --partitions 2 --replication-factor 1
```

---

### Issue: "Docker Compose Won't Start"

**Symptoms**:
```
error: no such file or directory: docker-compose.yml
ERROR: Service 'spark-master' failed to build
```

**Diagnosis**:
```bash
# Check docker-compose.yml exists
ls -la docker-compose.yml

# Validate YAML syntax
docker compose config

# Check Docker daemon is running
docker ps
```

**Solutions**:
1. Ensure you're in the correct directory
2. Run `docker compose config` to check syntax
3. Ensure Docker daemon is running: `sudo systemctl start docker`
4. Check disk space: `df -h /`

---

### Issue: "High CPU Usage"

**Symptoms**:
```
docker stats shows Spark or Kafka using 100% CPU
System becomes unresponsive
```

**Diagnosis**:
```bash
# Check which process is CPU-bound
docker stats --no-stream | grep -E "spark|kafka"

# Check Spark job status
curl http://localhost:8080/api/v1/applications
```

**Solutions**:
1. Reduce Spark executor cores: `--executor-cores 0.5`
2. Reduce data generation batch size
3. Increase Spark micro-batch interval (in Spark job code)
4. Add memory-based throttling

---

### Issue: "Slow Data Ingestion"

**Symptoms**:
```
Data appears in Kafka but takes 30+ seconds to reach PostgreSQL
Spark job shows long task times
```

**Diagnosis**:
```bash
# Check Spark UI for slow tasks
# http://localhost:8080 → Click running app → View task details

# Check PostgreSQL query performance
docker exec postgres psql -U postgres -d iot_database -c "\
  SELECT * FROM pg_stat_statements ORDER BY mean_exec_time DESC LIMIT 5;"
```

**Causes & Fixes**:

| Cause | Fix |
|-------|-----|
| JDBC batch size too small | Increase `batchsize` to 2000 in shared_utils/shared_utils.py |
| PostgreSQL too busy | Increase `shared_buffers` in docker-compose.yml |
| Spark shuffle overhead | Reduce partition count in Spark jobs |
| Network latency | Ensure Docker network is bridge mode (default) |

---

## Monitoring & Observability

### View Real-Time Metrics

```bash
# 1. Monitor resource usage
watch -n 1 'docker stats --no-stream'

# 2. Monitor database row counts
watch -n 5 'docker exec postgres psql -U postgres -d iot_database -c \
  "SELECT (SELECT COUNT(*) FROM iot.weather_data) as weather, \
          (SELECT COUNT(*) FROM iot.sales_orders) as orders;"'

# 3. Monitor Kafka consumer lag
watch -n 5 'docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 \
  --group spark --describe'

# 4. Monitor Spark job progress
curl -s http://localhost:8080/api/v1/applications | jq '.[-1].status'
```

### Custom Monitoring Queries

```sql
-- Data freshness (lag between ingestion and now)
SELECT table_name, MAX(ingested_at) as latest_data, 
       EXTRACT(EPOCH FROM NOW() - MAX(ingested_at)) as lag_seconds
FROM (
  SELECT 'weather_data' as table_name, ingested_at FROM iot.weather_data
  UNION ALL
  SELECT 'sales_orders', ingested_at FROM iot.sales_orders
  UNION ALL
  SELECT 'logistics_shipments', ingested_at FROM iot.logistics_shipments
  UNION ALL
  SELECT 'inventory_changes', ingested_at FROM iot.inventory_changes
  UNION ALL
  SELECT 'user_events', ingested_at FROM iot.user_events
) t
GROUP BY table_name;

-- Data volume trends (messages/hour)
SELECT DATE_TRUNC('hour', recorded_at) as hour, COUNT(*) as count
FROM iot.weather_data
WHERE recorded_at > NOW() - INTERVAL '24 hours'
GROUP BY hour
ORDER BY hour DESC;

-- Database size
SELECT 
  schemaname,
  tablename,
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables
WHERE schemaname = 'iot'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

---

## Scaling Considerations

### Scale Up (More Data, More Performance)

**Add More Spark Workers**:
1. Add to `docker-compose.yml`, building from the same `config/spark/Dockerfile`
   used by spark-master/spark-worker-1/spark-worker-2 (not a different image -
   it needs the same baked-in Kafka connector + JDBC jars):
   ```yaml
   spark-worker-3:
     build:
       context: .
       dockerfile: config/spark/Dockerfile
     command: >
       bash -c "/opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077"
     volumes:
       - ./spark-jobs:/opt/spark-jobs:ro
       - ./data-spark-worker-3:/tmp/spark-data
     mem_limit: 1500m
     memswap_limit: 1500m
     cpus: '1.0'
   ```
2. Allocate +1.5GB memory (total becomes ~17GB+)
3. Increase partitions: `--total-executor-cores 4`

**Increase Kafka Partitions**:
```bash
docker exec kafka kafka-topics --bootstrap-server kafka:9092 \
  --alter --topic iot-weather-data --partitions 4
```

**Increase PostgreSQL Connections**:
```bash
# In docker-compose.yml
postgres:
  command:
    - -c
    - max_connections=100  # Up from 50
```

### Scale Down (Minimal Footprint)

**Use Single Spark Worker**:
```bash
# In docker-compose.yml, remove spark-worker-2
# Keep only spark-master + spark-worker-1
# Save ~1.5GB memory
```

**Reduce Kafka Retention**:
```bash
# 6 hours instead of 1 day
docker exec kafka kafka-topics --bootstrap-server kafka:9092 \
  --alter --topic iot-weather-data \
  --config retention.ms=21600000
```

---

## Disaster Recovery

### Backup PostgreSQL
```bash
docker exec postgres pg_dump -U postgres -d iot_database > backup.sql

# Restore
docker exec -i postgres psql -U postgres -d iot_database < backup.sql
```

### Recover from Data Loss

```bash
# Full reset (clear all data, restart clean)
bash scripts/reset.sh

# Partial reset (keep PostgreSQL data, reset Kafka)
docker compose down
docker volume rm spark-master-data spark-worker-1-data spark-worker-2-data
docker compose up -d
bash scripts/init-kafka-topics.sh
```

---

## Next Steps

- See [DEVELOPMENT.md](DEVELOPMENT.md) for local Spark testing
- See [SETUP.md](SETUP.md) for quick start verification
- See [ARCHITECTURE.md](ARCHITECTURE.md) for system design details
