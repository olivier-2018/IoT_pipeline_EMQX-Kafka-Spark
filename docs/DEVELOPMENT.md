# IoT Data Pipeline - Development & Local Testing

## Local Testing Setup

This guide explains how to test Spark jobs locally without Docker, useful for debugging and development.

---

## Prerequisites

### 1. Install Python & PySpark

```bash
# Python 3.9+
python3 --version

# Install PySpark locally
pip install pyspark==3.5.0 psycopg2-binary==2.9.9
```

### 2. Download Spark Locally (Optional but Recommended)

```bash
# Download Spark
wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
tar -xzf spark-3.5.0-bin-hadoop3.tgz
export SPARK_HOME=$(pwd)/spark-3.5.0-bin-hadoop3

# Add to PATH
export PATH=$SPARK_HOME/bin:$PATH
```

### 3. PostgreSQL Driver for Spark

```bash
# Download PostgreSQL JDBC driver
wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
export CLASSPATH=$CLASSPATH:$(pwd)/postgresql-42.6.0.jar
```

---

## Test 1: Local Data Frame Operations

Test Spark DataFrame operations without connecting to any external services.

### Create `test_local_spark.py`

```python
#!/usr/bin/env python3
"""Test Spark operations locally without Docker"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Create Spark session (local mode)
spark = SparkSession.builder \
    .appName("LocalWeatherTest") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema (same as in shared_utils.py)
schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("timestamp", LongType(), True),
])

# Create sample data
sample_data = [
    '{"device_id": "sensor-01", "temperature": 23.5, "humidity": 65, "pressure": 1013.25, "timestamp": 1705319445000}',
    '{"device_id": "sensor-02", "temperature": 22.1, "humidity": 70, "pressure": 1013.10, "timestamp": 1705319446000}',
    '{"device_id": "sensor-01", "temperature": 24.0, "humidity": 64, "pressure": 1013.30, "timestamp": 1705319447000}',
]

# Create DataFrame from raw strings
df = spark.createDataFrame(
    [(data,) for data in sample_data],
    schema=StructType([StructField("value", StringType(), True)])
)

print("Raw data:")
df.show(truncate=False)

# Parse JSON
parsed_df = df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

print("\nParsed data:")
parsed_df.show()

# Transform
transformed_df = (
    parsed_df
    .withColumn("recorded_at", to_timestamp(col("timestamp") / 1000))
    .withColumn("ingested_at", current_timestamp())
    .select(
        col("device_id"),
        col("temperature"),
        col("humidity"),
        col("pressure"),
        col("recorded_at"),
        col("ingested_at")
    )
)

print("\nTransformed data:")
transformed_df.show()

print("\nSchema:")
transformed_df.printSchema()

print("\nBasic stats:")
transformed_df.describe().show()

print("\n✓ Local test passed!")
spark.stop()
```

### Run the Test

```bash
cd spark-jobs
python3 test_local_spark.py
```

Expected output:
```
Raw data:
+----+
|                                                                value|
+----+
|{"device_id": "sensor-01", "temperature": 23.5, "humidity": 65...}|
...

Parsed data:
+----------+--------+--------+------+--------------+
|device_id |temperature|humidity|pressure|timestamp   |
+----------+--------+--------+------+--------------+
|sensor-01 |23.5    |65      |1013.25|1705319445000|
...

✓ Local test passed!
```

---

## Test 2: PostgreSQL Connection Test

Test connecting to PostgreSQL from local machine (without Docker).

### Create `test_postgres_connection.py`

```python
#!/usr/bin/env python3
"""Test PostgreSQL connection from local machine"""

import psycopg2
from psycopg2 import sql

def test_connection():
    """Test basic PostgreSQL connection"""
    try:
        # Connect to PostgreSQL
        # Adjust host/port if not using Docker
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="iot_database",
            user="postgres",
            password="iot_demo_pass"
        )
        
        cursor = conn.cursor()
        
        # Test: Get row counts
        cursor.execute("""
            SELECT 
                (SELECT COUNT(*) FROM iot.weather_data) as weather,
                (SELECT COUNT(*) FROM iot.sales_orders) as orders,
                (SELECT COUNT(*) FROM iot.logistics_shipments) as logistics
        """)
        
        row = cursor.fetchone()
        print("Row counts:")
        print(f"  Weather: {row[0]}")
        print(f"  Orders: {row[1]}")
        print(f"  Logistics: {row[2]}")
        
        # Test: Get latest weather data
        cursor.execute("""
            SELECT device_id, temperature, humidity, recorded_at
            FROM iot.weather_data
            ORDER BY ingested_at DESC
            LIMIT 3
        """)
        
        print("\nLatest weather data:")
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]}°C, {row[2]}% humidity @ {row[3]}")
        
        cursor.close()
        conn.close()
        print("\n✓ PostgreSQL connection test passed!")
        
    except Exception as e:
        print(f"✗ PostgreSQL connection failed: {e}")
        return False
    
    return True


if __name__ == "__main__":
    test_connection()
```

### Run the Test

```bash
python3 test_postgres_connection.py
```

Expected output:
```
Row counts:
  Weather: 150
  Orders: 300
  Logistics: 75

Latest weather data:
  sensor-01: 23.5°C, 65% humidity @ 2024-01-15 10:30:45
  sensor-02: 22.1°C, 70% humidity @ 2024-01-15 10:30:46
  sensor-01: 24.0°C, 64% humidity @ 2024-01-15 10:30:47

✓ PostgreSQL connection test passed!
```

---

## Test 3: Spark JDBC Write Test

Test writing a DataFrame to PostgreSQL via JDBC (critical for production debugging).

### Create `test_spark_jdbc.py`

```python
#!/usr/bin/env python3
"""Test Spark JDBC write to PostgreSQL"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import time

# Create Spark session
spark = SparkSession.builder \
    .appName("SparkJDBCTest") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

try:
    # Create sample weather data
    schema = StructType([
        StructField("device_id", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("timestamp", LongType(), True),
    ])

    sample_data = [
        '{"device_id": "test-sensor-01", "temperature": 25.0, "humidity": 60, "pressure": 1013.0, "timestamp": ' + str(int(time.time() * 1000)) + '}',
        '{"device_id": "test-sensor-02", "temperature": 23.5, "humidity": 65, "pressure": 1012.5, "timestamp": ' + str(int(time.time() * 1000)) + '}',
    ]

    df = spark.createDataFrame(
        [(data,) for data in sample_data],
        StructType([StructField("value", StringType(), True)])
    )

    # Parse and transform
    parsed_df = df.select(
        from_json(col("value"), schema).alias("data")
    ).select("data.*")

    transformed_df = (
        parsed_df
        .withColumn("recorded_at", to_timestamp(col("timestamp") / 1000))
        .withColumn("ingested_at", current_timestamp())
        .select(
            col("device_id"),
            col("temperature"),
            col("humidity"),
            col("pressure"),
            col("recorded_at"),
            col("ingested_at")
        )
    )

    print("DataFrame to write:")
    transformed_df.show()

    # JDBC options
    jdbc_url = "jdbc:postgresql://localhost:5432/iot_database?currentSchema=iot"
    jdbc_options = {
        "url": jdbc_url,
        "dbtable": "iot.weather_data",
        "user": "postgres",
        "password": "iot_demo_pass",
        "driver": "org.postgresql.Driver",
        "batchsize": "1000",
    }

    # Write to PostgreSQL
    print("\nWriting to PostgreSQL...")
    transformed_df.write \
        .format("jdbc") \
        .options(**jdbc_options) \
        .mode("append") \
        .save()

    print("✓ Successfully wrote to PostgreSQL!")

    # Verify write
    print("\nVerifying data in PostgreSQL...")
    read_df = spark.read \
        .format("jdbc") \
        .options(**jdbc_options) \
        .load()

    read_df.filter(col("device_id").startswith("test-sensor")) \
        .orderBy(col("ingested_at").desc()) \
        .limit(5) \
        .show()

    print("✓ Spark JDBC test passed!")

except Exception as e:
    print(f"✗ Spark JDBC test failed: {e}")
    import traceback
    traceback.print_exc()

finally:
    spark.stop()
```

### Run the Test

```bash
python3 test_spark_jdbc.py
```

Expected output:
```
DataFrame to write:
+----------+----------+--------+--------+-------------------+-------------------+
|device_id |temperature|humidity|pressure|recorded_at        |ingested_at        |
+----------+----------+--------+--------+-------------------+-------------------+
|test-sensor-01|25.0        |60      |1013.0  |2024-01-15 10:35:00|2024-01-15 10:35:01|
|test-sensor-02|23.5        |65      |1012.5  |2024-01-15 10:35:00|2024-01-15 10:35:01|
+----------+----------+--------+--------+-------------------+-------------------+

Writing to PostgreSQL...
✓ Successfully wrote to PostgreSQL!

Verifying data in PostgreSQL...
+----------+----------+--------+--------+-------------------+-------------------+
|device_id |temperature|humidity|pressure|recorded_at        |ingested_at        |
+----------+----------+--------+--------+-------------------+-------------------+
|test-sensor-01|25.0        |60      |1013.0  |2024-01-15 10:35:00|2024-01-15 10:35:01|
|test-sensor-02|23.5        |65      |1012.5  |2024-01-15 10:35:00|2024-01-15 10:35:01|
+----------+----------+--------+--------+-------------------+-------------------+

✓ Spark JDBC test passed!
```

---

## Test 4: Kafka Consumer Test (Local)

Test reading from Kafka without Spark Streaming (simpler debugging).

### Create `test_kafka_consumer.py`

```python
#!/usr/bin/env python3
"""Test consuming from Kafka topic"""

from confluent_kafka import Consumer, KafkaError
import json
import sys

def consume_messages(topic, max_messages=5):
    """Consume messages from Kafka topic"""
    
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'test-consumer',
        'auto.offset.reset': 'latest',
    }

    consumer = Consumer(conf)
    consumer.subscribe([topic])

    print(f"Listening to topic: {topic}")
    print("Waiting for messages (5 sec timeout)...\n")

    messages_consumed = 0
    while messages_consumed < max_messages:
        msg = consumer.poll(timeout=5.0)

        if msg is None:
            print("(No more messages, timeout)")
            break

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                break
            else:
                print(f"Error: {msg.error()}")
                break

        try:
            value = json.loads(msg.value().decode('utf-8'))
            print(f"Message {messages_consumed + 1}:")
            print(json.dumps(value, indent=2))
            messages_consumed += 1
        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON: {e}")

    consumer.close()
    print(f"\n✓ Consumed {messages_consumed} messages")


if __name__ == "__main__":
    # Install confluent-kafka first:
    # pip install confluent-kafka
    
    topic = sys.argv[1] if len(sys.argv) > 1 else "iot.weather.data"
    consume_messages(topic)
```

### Install & Run

```bash
pip install confluent-kafka

python3 test_kafka_consumer.py iot.weather.data
```

Expected output:
```
Listening to topic: iot.weather.data
Waiting for messages (5 sec timeout)...

Message 1:
{
  "device_id": "weather-sensor-03",
  "temperature": 23.2,
  "humidity": 68,
  "pressure": 1012.9,
  "timestamp": 1705319445000
}

Message 2:
{
  "device_id": "weather-sensor-01",
  "temperature": 24.1,
  "humidity": 62,
  "pressure": 1013.4,
  "timestamp": 1705319446000
}

✓ Consumed 2 messages
```

---

## Test 5: Unit Test Data Generators

Test individual data generators in isolation.

### Create `test_generators.py`

```python
#!/usr/bin/env python3
"""Unit test for data generators"""

import sys
sys.path.insert(0, '/home/sirius/TUTORIALS/Spark_Kafka_Docker/data-generators')

from weather_generator import WeatherGenerator
from orders_generator import OrdersGenerator
from logistics_generator import LogisticsGenerator
import json

def test_weather_generator():
    """Test weather data generation"""
    gen = WeatherGenerator()
    
    # Test single record
    record = gen.generate_record("test-sensor")
    assert record["device_id"] == "test-sensor"
    assert 15 <= record["temperature"] <= 35
    assert 30 <= record["humidity"] <= 80
    print("✓ Weather generator: single record OK")
    
    # Test batch
    batch = gen.generate_batch(batch_size=5)
    assert len(batch) == 5
    print(f"✓ Weather generator: batch of {len(batch)} OK")
    
    print("\nSample weather record:")
    print(json.dumps(batch[0], indent=2))


def test_orders_generator():
    """Test orders data generation"""
    gen = OrdersGenerator()
    
    batch = gen.generate_batch(batch_size=3)
    assert len(batch) == 3
    assert "order_id" in batch[0]
    assert "items" in batch[0]
    assert len(batch[0]["items"]) > 0
    
    print("\n✓ Orders generator: batch of 3 OK")
    print("\nSample order record:")
    print(json.dumps(batch[0], indent=2, default=str))


def test_logistics_generator():
    """Test logistics data generation"""
    gen = LogisticsGenerator()
    
    batch = gen.generate_batch(batch_size=2)
    assert len(batch) == 2
    assert "shipment_id" in batch[0]
    assert "current_location" in batch[0]
    
    print("\n✓ Logistics generator: batch of 2 OK")
    print("\nSample logistics record:")
    print(json.dumps(batch[0], indent=2, default=str))


if __name__ == "__main__":
    test_weather_generator()
    test_orders_generator()
    test_logistics_generator()
    print("\n✓ All generator tests passed!")
```

### Run the Test

```bash
python3 test_generators.py
```

---

## Adding New Data Types

To add a new data type (e.g., "machine metrics"):

### Step 1: Create Generator
Create `data-generators/machine_metrics_generator.py`:

```python
class MachineMetricsGenerator:
    def __init__(self):
        self.mqtt_client = mqtt.Client(client_id="metrics_generator")
        self.config = DEVICE_CONFIG["machine_metrics"]
        self.connected = False

    def generate_record(self):
        return {
            "machine_id": f"machine-{random.randint(1, 5):02d}",
            "cpu_usage": random.uniform(0, 100),
            "memory_usage": random.uniform(0, 100),
            "disk_io": random.randint(0, 1000),
            "timestamp": int(datetime.now().timestamp() * 1000),
        }
    # ... rest of implementation
```

### Step 2: Add to Orchestrator
Update `data-generators/main.py`:

```python
from machine_metrics_generator import MachineMetricsGenerator

self.generators = {
    "weather": WeatherGenerator(),
    "orders": OrdersGenerator(),
    "machine_metrics": MachineMetricsGenerator(),  # NEW
    ...
}
```

### Step 3: Create Spark Job
Create `spark-jobs/ingest_machine_metrics.py`:

```python
def main():
    spark = SparkSessionFactory.create_session("MachineMetricsIngestion")
    
    kafka_df = spark.readStream.format("kafka") \
        .option("subscribe", "iot.machine.metrics") \
        .load()
    
    # ... transform and write
```

### Step 4: Add Kafka Topic
```bash
docker exec kafka kafka-topics --bootstrap-server kafka:9092 \
  --create --topic iot.machine.metrics --partitions 2 --replication-factor 1
```

### Step 5: Add PostgreSQL Table
Update `config/postgres/init.sql`:

```sql
CREATE TABLE IF NOT EXISTS iot.machine_metrics (
    id SERIAL PRIMARY KEY,
    machine_id VARCHAR(50) NOT NULL,
    cpu_usage DECIMAL(5,2),
    memory_usage DECIMAL(5,2),
    disk_io INT,
    recorded_at TIMESTAMP NOT NULL,
    ingested_at TIMESTAMP NOT NULL
);
```

---

## Next Steps

- Try running tests locally to debug Spark jobs
- Create custom unit tests for your additions
- Use IDE debugging with PySpark (set breakpoints in Spark code)
- See [SETUP.md](SETUP.md) for full system deployment
