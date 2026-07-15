# IoT Data Pipeline - Data Schemas & Topic Design

## Database Schema (PostgreSQL)

All tables are in the `iot` schema. Access via: `psql -d iot_database -c "SELECT * FROM iot.table_name;"`

---

### 1. Weather Data Table

**Purpose**: Store temperature, humidity, and pressure readings from weather sensors

```sql
CREATE TABLE iot.weather_data (
    id SERIAL PRIMARY KEY,
    message_id UUID UNIQUE,             -- generator-issued id; lets Spark upsert idempotently
    device_id VARCHAR(50) NOT NULL,
    temperature DECIMAL(5,2),           -- Celsius, range: -50 to +60
    humidity DECIMAL(5,2),              -- Percentage, range: 0 to 100
    pressure DECIMAL(7,2),              -- hPa, range: 950 to 1050
    location VARCHAR(100),              -- e.g., "NYC", "warehouse-A"
    recorded_at TIMESTAMP NOT NULL,     -- When sensor measured (UTC)
    ingested_at TIMESTAMP NOT NULL      -- When inserted to DB (UTC)
);

CREATE INDEX idx_weather_device_time 
    ON weather_data(device_id, recorded_at DESC);
CREATE INDEX idx_weather_ingested 
    ON weather_data(ingested_at DESC);
```

**Example Row**:
```json
{
  "id": 1,
  "device_id": "weather-sensor-01",
  "temperature": 23.50,
  "humidity": 65.0,
  "pressure": 1013.25,
  "location": "NYC",
  "recorded_at": "2024-01-15 10:30:45",
  "ingested_at": "2024-01-15 10:30:51"
}
```

**Typical Query**:
```sql
-- Get average temperature per device (last 24h)
SELECT device_id, AVG(temperature) as avg_temp, COUNT(*) as readings
FROM iot.weather_data
WHERE recorded_at > NOW() - INTERVAL '24 hours'
GROUP BY device_id
ORDER BY avg_temp DESC;
```

---

### 2. Sales Orders Table

**Purpose**: Store sales order transactions with customer and financial data

```sql
CREATE TABLE iot.sales_orders (
    order_id UUID PRIMARY KEY,
    customer_id INT NOT NULL,
    order_status VARCHAR(20) DEFAULT 'pending',  -- pending, confirmed, shipped, delivered, cancelled
    total_amount DECIMAL(10,2) NOT NULL,         -- USD, range: $0.01 to $99,999.99
    item_count INT DEFAULT 0,                    -- Number of items in order
    created_at TIMESTAMP NOT NULL,               -- When order was placed (UTC)
    updated_at TIMESTAMP NOT NULL,               -- Last status update (UTC)
    ingested_at TIMESTAMP NOT NULL               -- When inserted to DB (UTC)
);

CREATE INDEX idx_orders_customer_time 
    ON sales_orders(customer_id, created_at DESC);
CREATE INDEX idx_orders_status 
    ON sales_orders(order_status, updated_at DESC);
CREATE INDEX idx_orders_ingested 
    ON sales_orders(ingested_at DESC);
```

**Example Row**:
```json
{
  "order_id": "550e8400-e29b-41d4-a716-446655440000",
  "customer_id": 12345,
  "order_status": "confirmed",
  "total_amount": 125.50,
  "item_count": 3,
  "created_at": "2024-01-15 10:15:00",
  "updated_at": "2024-01-15 10:20:30",
  "ingested_at": "2024-01-15 10:20:35"
}
```

**Typical Query**:
```sql
-- Revenue by customer (last 7 days)
SELECT customer_id, COUNT(*) as order_count, SUM(total_amount) as revenue
FROM iot.sales_orders
WHERE created_at > NOW() - INTERVAL '7 days' AND order_status = 'confirmed'
GROUP BY customer_id
ORDER BY revenue DESC
LIMIT 10;
```

---

### 3. Logistics Shipments Table

**Purpose**: Track shipment status and GPS location through delivery pipeline

```sql
CREATE TABLE iot.logistics_shipments (
    shipment_id UUID PRIMARY KEY,
    order_id UUID NOT NULL,                     -- Reference to sales order
    shipment_status VARCHAR(20) DEFAULT 'pending',  -- pending, in_transit, delivered
    current_location POINT,                     -- PostgreSQL POINT type (lat, lon)
    origin_location VARCHAR(100),               -- e.g., "NYC Warehouse"
    destination_location VARCHAR(100),          -- e.g., "Customer Address"
    last_update TIMESTAMP NOT NULL,             -- Last status update (UTC)
    ingested_at TIMESTAMP NOT NULL              -- When inserted to DB (UTC)
);

CREATE INDEX idx_logistics_order_id 
    ON logistics_shipments(order_id);
CREATE INDEX idx_logistics_status 
    ON logistics_shipments(shipment_status, last_update DESC);
CREATE INDEX idx_logistics_location 
    ON logistics_shipments USING GIST(current_location);
CREATE INDEX idx_logistics_ingested 
    ON logistics_shipments(ingested_at DESC);
```

**Example Row**:
```json
{
  "shipment_id": "660e8400-e29b-41d4-a716-446655440000",
  "order_id": "550e8400-e29b-41d4-a716-446655440000",
  "shipment_status": "in_transit",
  "current_location": "(40.7128,-74.0060)",
  "origin_location": "NYC Warehouse",
  "destination_location": "Boston, MA",
  "last_update": "2024-01-15 11:45:00",
  "ingested_at": "2024-01-15 11:45:05"
}
```

**Typical Query**:
```sql
-- Shipments in transit (realtime tracking)
SELECT shipment_id, order_id, current_location, destination_location
FROM iot.logistics_shipments
WHERE shipment_status = 'in_transit'
ORDER BY last_update DESC;
```

---

### 4. Inventory Changes Table

**Purpose**: Log inventory stock changes per SKU and warehouse

```sql
CREATE TABLE iot.inventory_changes (
    id SERIAL PRIMARY KEY,
    message_id UUID UNIQUE,                     -- generator-issued id; lets Spark upsert idempotently
    item_sku VARCHAR(50) NOT NULL,              -- Product SKU (e.g., "WIDGET-001")
    warehouse_id VARCHAR(50),                   -- Warehouse code (e.g., "WH1", "WH2")
    quantity_delta INT NOT NULL,                -- Change amount (pos/neg)
    current_stock INT DEFAULT 0,                -- Stock level after change
    change_reason VARCHAR(100),                 -- reason: purchase, sale, adjustment, return, restock
    changed_at TIMESTAMP NOT NULL,              -- When change occurred (UTC)
    ingested_at TIMESTAMP NOT NULL              -- When inserted to DB (UTC)
);

CREATE INDEX idx_inventory_sku_time 
    ON inventory_changes(item_sku, changed_at DESC);
CREATE INDEX idx_inventory_warehouse 
    ON inventory_changes(warehouse_id, changed_at DESC);
CREATE INDEX idx_inventory_ingested 
    ON inventory_changes(ingested_at DESC);
```

**Example Row**:
```json
{
  "id": 501,
  "item_sku": "WIDGET-001",
  "warehouse_id": "WH1",
  "quantity_delta": -5,
  "current_stock": 245,
  "change_reason": "sale",
  "changed_at": "2024-01-15 12:10:30",
  "ingested_at": "2024-01-15 12:10:35"
}
```

**Typical Query**:
```sql
-- Low stock items (< 100 units)
SELECT item_sku, warehouse_id, current_stock, changed_at
FROM iot.inventory_changes
WHERE current_stock < 100
ORDER BY current_stock ASC;
```

---

### 5. User Events Table

**Purpose**: Log user interactions and behavior tracking

```sql
CREATE TABLE iot.user_events (
    id SERIAL PRIMARY KEY,
    message_id UUID UNIQUE,                     -- generator-issued id; lets Spark upsert idempotently
    user_id INT NOT NULL,
    event_type VARCHAR(50) NOT NULL,            -- click, view, add_to_cart, purchase, login, logout
    page_or_resource VARCHAR(200),              -- e.g., "/products", "/checkout"
    event_value VARCHAR(500),                   -- e.g., product ID, amount
    session_id VARCHAR(100),                    -- Session identifier
    event_timestamp TIMESTAMP NOT NULL,         -- When event occurred (UTC)
    ingested_at TIMESTAMP NOT NULL              -- When inserted to DB (UTC)
);

CREATE INDEX idx_user_events_user_time 
    ON user_events(user_id, event_timestamp DESC);
CREATE INDEX idx_user_events_type 
    ON user_events(event_type, event_timestamp DESC);
CREATE INDEX idx_user_events_session 
    ON user_events(session_id, event_timestamp DESC);
CREATE INDEX idx_user_events_ingested 
    ON user_events(ingested_at DESC);
```

**Example Row**:
```json
{
  "id": 10001,
  "user_id": 5678,
  "event_type": "click",
  "page_or_resource": "/products",
  "event_value": "WIDGET-001",
  "session_id": "sess_abc123xyz",
  "event_timestamp": "2024-01-15 12:20:15",
  "ingested_at": "2024-01-15 12:20:17"
}
```

**Typical Query**:
```sql
-- User session timeline
SELECT event_type, page_or_resource, event_timestamp
FROM iot.user_events
WHERE user_id = 5678 AND session_id = 'sess_abc123xyz'
ORDER BY event_timestamp ASC;
```

---

## Kafka Topic Design

All Kafka topics use **2 partitions** for optimal Spark parallelism on 2 workers.

### Topic: `iot-weather-data`
- **Partitions**: 2
- **Replication Factor**: 1
- **Retention**: 1 day (86,400,000 ms)
- **Max Size**: 512 MB per topic
- **Compression**: GZIP
- **Partition Key**: `null` (round-robin)
- **Message Format**: JSON

**Example Message**:
```json
{
  "device_id": "weather-sensor-01",
  "temperature": 23.5,
  "humidity": 65,
  "pressure": 1013.25,
  "timestamp": 1705319445000
}
```

---

### Topic: `iot-orders-events`
- **Partitions**: 2
- **Replication Factor**: 1
- **Retention**: 1 day
- **Max Size**: 512 MB
- **Compression**: GZIP
- **Partition Key**: `order_id` (ensures order sequencing)
- **Message Format**: JSON

**Example Message**:
```json
{
  "order_id": "550e8400-e29b-41d4-a716-446655440000",
  "customer_id": 12345,
  "status": "pending",
  "items": [
    {"product": "WIDGET-A", "quantity": 2, "price": 45.99}
  ],
  "total_amount": 125.50,
  "timestamp": 1705319445000
}
```

---

### Topic: `iot-logistics-dispatch`
- **Partitions**: 2
- **Replication Factor**: 1
- **Retention**: 1 day
- **Max Size**: 512 MB
- **Compression**: GZIP
- **Partition Key**: `shipment_id` (per-shipment ordering)
- **Message Format**: JSON

**Example Message**:
```json
{
  "shipment_id": "660e8400-e29b-41d4-a716-446655440000",
  "order_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "in_transit",
  "current_location": {
    "latitude": 40.7128,
    "longitude": -74.0060
  },
  "origin": "NYC Warehouse",
  "destination": "Boston, MA",
  "timestamp": 1705319445000
}
```

---

### Topic: `iot-inventory-changes`
- **Partitions**: 2
- **Replication Factor**: 1
- **Retention**: 1 day
- **Max Size**: 512 MB
- **Compression**: GZIP
- **Partition Key**: `item_sku` (per-SKU consistency)
- **Message Format**: JSON

**Example Message**:
```json
{
  "item_sku": "WIDGET-001",
  "warehouse_id": "WH1",
  "quantity_delta": -5,
  "change_reason": "sale",
  "timestamp": 1705319445000
}
```

---

### Topic: `iot-users-activity`
- **Partitions**: 2
- **Replication Factor**: 1
- **Retention**: 1 day
- **Max Size**: 512 MB
- **Compression**: GZIP
- **Partition Key**: `user_id` (per-user session ordering)
- **Message Format**: JSON

**Example Message**:
```json
{
  "user_id": 5678,
  "event_type": "click",
  "page": "/products",
  "session_id": "sess_abc123xyz",
  "event_value": "WIDGET-001",
  "timestamp": 1705319445000
}
```

---

## MQTT Topic Structure

Data generators publish to MQTT, which EMQX forwards to Kafka via bridge rules.

**For comprehensive MQTT message format definitions (full JSON schemas), see [MQTT_SCHEMAS.md](MQTT_SCHEMAS.md).**

### MQTT Topics Overview

| Data Type | MQTT Topic | Frequency | Partition Key | Kafka Topic |
|-----------|-----------|-----------|---------|---------|
| Weather | `devices/weather/data` | 10–20/min | none | `iot-weather-data` |
| Orders | `devices/orders/new_order` | 50–100/min | `order_id` | `iot-orders-events` |
| Logistics | `devices/logistics/dispatch` | 20–50/min | `shipment_id` | `iot-logistics-dispatch` |
| Inventory | `devices/inventory/change` | 10–30/min | `item_sku` | `iot-inventory-changes` |
| User Events | `devices/users/event` | 200–500/min | `user_id` | `iot-users-activity` |

---

## Data Type Ranges

### Weather
- **Temperature**: -50°C to +60°C
- **Humidity**: 0–100%
- **Pressure**: 950–1050 hPa

### Orders
- **Customer IDs**: 1–1000
- **Amount**: $10–$500
- **Item Count**: 1–10 items

### Logistics
- **Location**: NYC area (40.0–41.0 lat, -74.0–-73.0 lon)
- **Statuses**: pending, in_transit, delivered
- **Active Shipments**: ~10 concurrent

### Inventory
- **Warehouse IDs**: WH1, WH2, WH3, WH4
- **Quantity Delta**: -50 to +50 units
- **Reasons**: purchase, sale, adjustment, return, restock

### User Events
- **User IDs**: 1–10,000
- **Actions**: click, view, add_to_cart, purchase, login, logout
- **Pages**: home, products, checkout, profile, search, wishlist
- **Active Sessions**: ~50 concurrent

---

## Retention & Cleanup

- **Kafka Topic Retention**: 1 day (86,400,000 ms)
- **Kafka Size Limit**: 512 MB per topic
- **Cleanup Policy**: Delete (remove oldest data first)
- **PostgreSQL Retention**: Indefinite (all data persisted)
- **Expected DB Size**: ~1 GB for 30 minutes of data at 500–1000 msg/sec

---

## Monitoring Data Flow

### Query Message Counts (by hour)
```sql
SELECT 
  DATE_TRUNC('hour', recorded_at) as hour,
  COUNT(*) as message_count
FROM iot.weather_data
GROUP BY DATE_TRUNC('hour', recorded_at)
ORDER BY hour DESC
LIMIT 24;
```

### Query Processing Lag (MQTT → PostgreSQL)
```sql
SELECT 
  AVG(EXTRACT(EPOCH FROM (ingested_at - recorded_at))) as avg_lag_seconds,
  MAX(EXTRACT(EPOCH FROM (ingested_at - recorded_at))) as max_lag_seconds
FROM iot.weather_data;
```

### Monitor Kafka Consumer Lag
```bash
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 \
  --group spark --describe
```
