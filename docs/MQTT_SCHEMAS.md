# MQTT Message Schemas

This document provides detailed JSON schema definitions for all MQTT messages in the IoT Data Pipeline. All data is transmitted via MQTT to the EMQX broker, with each message published to a topic following the pattern `devices/{data_type}/{subtype}`.

---

## 1. Weather Data

**MQTT Topic**: `devices/weather/data`  
**Message Rate**: 10–20 msg/min per sensor  
**Sensor Count**: 10 device instances  
**Partition Key**: None (round-robin)  
**Kafka Topic**: `iot-weather-data`

### Message Format

```json
{
  "message_id": "a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5",
  "device_id": "weather-sensor-01",
  "temperature": 23.5,
  "humidity": 65,
  "pressure": 1013.25,
  "timestamp": 1688044200000
}
```

### Field Definitions

| Field | Type | Range | Required | Description |
|-------|------|-------|----------|-------------|
| `message_id` | string (UUID) | - | ✓ | Unique message identifier, generated at publish time - lets Spark upsert idempotently (`ON CONFLICT DO NOTHING`) instead of risking duplicate rows on a replay |
| `device_id` | string | `weather-sensor-01` to `weather-sensor-10` | ✓ | Unique sensor identifier |
| `temperature` | float | 15.0–35.0 °C | ✓ | Ambient temperature in Celsius |
| `humidity` | integer | 30–80 % | ✓ | Relative humidity percentage |
| `pressure` | float | 950–1050 hPa | ✓ | Atmospheric pressure in hectopascals |
| `timestamp` | integer | Unix epoch ms | ✓ | Measurement timestamp (milliseconds since epoch) |

### Example MQTT Publish Command

```bash
mosquitto_pub -h localhost -t "devices/weather/data" \
  -m '{"message_id":"a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5","device_id":"weather-sensor-01","temperature":23.5,"humidity":65,"pressure":1013.25,"timestamp":1688044200000}'
```

---

## 2. Sales Orders

**MQTT Topic**: `devices/orders/new_order`  
**Message Rate**: 50–100 msg/min  
**Order Sources**: 5 device instances  
**Partition Key**: `order_id` (per-order sequencing)  
**Kafka Topic**: `iot-orders-events`

### Message Format

```json
{
  "order_id": "a1b2c3d4-e5f6-4789-ab10-cd11ef121314",
  "customer_id": 523,
  "status": "pending",
  "items": [
    {
      "product": "Widget-A",
      "quantity": 2,
      "price": 45.99
    },
    {
      "product": "Gadget-X",
      "quantity": 1,
      "price": 120.50
    }
  ],
  "total_amount": 212.48,
  "timestamp": 1688044200000
}
```

### Field Definitions

| Field | Type | Range | Required | Description |
|-------|------|-------|----------|-------------|
| `order_id` | string (UUID) | — | ✓ | Unique order identifier (UUIDv4) |
| `customer_id` | integer | 1–1000 | ✓ | Customer reference ID |
| `status` | string | `pending` | ✓ | Order status (always "pending" at creation) |
| `items` | array | 1–10 items | ✓ | Array of line items |
| `items[].product` | string | see products list | ✓ | Product name or SKU |
| `items[].quantity` | integer | 1–5 | ✓ | Units ordered |
| `items[].price` | float | 10.0–100.0 | ✓ | Unit price |
| `total_amount` | float | — | ✓ | Sum of all (item.quantity × item.price) |
| `timestamp` | integer | Unix epoch ms | ✓ | Order creation timestamp |

### Valid Product Names

- `Widget-A`
- `Widget-B`
- `Gadget-X`
- `Tool-Y`
- `Accessory-Z`

### Example MQTT Publish Command

```bash
mosquitto_pub -h localhost -t "devices/orders/new_order" \
  -m '{"order_id":"a1b2c3d4-e5f6-4789-ab10-cd11ef121314","customer_id":523,"status":"pending","items":[{"product":"Widget-A","quantity":2,"price":45.99}],"total_amount":91.98,"timestamp":1688044200000}'
```

---

## 3. Logistics / Shipment Tracking

**MQTT Topic**: `devices/logistics/dispatch`  
**Message Rate**: 20–50 msg/min  
**Active Shipments**: 10–20 concurrent shipment IDs  
**Partition Key**: `shipment_id` (per-shipment ordering)  
**Kafka Topic**: `iot-logistics-dispatch`

### Message Format

```json
{
  "shipment_id": "s5f8a9b2-c1d4-47e3-a9f2-b3c6d7e8f9a0",
  "order_id": "a1b2c3d4-e5f6-4789-ab10-cd11ef121314",
  "status": "in_transit",
  "current_location": {
    "latitude": 40.712776,
    "longitude": -74.005974
  },
  "origin": "NYC Warehouse",
  "destination": "Customer Location",
  "timestamp": 1688044200000
}
```

### Field Definitions

| Field | Type | Range | Required | Description |
|-------|------|-------|----------|-------------|
| `shipment_id` | string (UUID) | — | ✓ | Unique shipment tracking ID (UUIDv4) |
| `order_id` | string (UUID) | — | ✓ | Associated order reference |
| `status` | string | see statuses | ✓ | Current shipment status |
| `current_location.latitude` | float | 40.0–41.0 | ✓ | GPS latitude (example: NY area) |
| `current_location.longitude` | float | -74.0 to -73.0 | ✓ | GPS longitude (example: NY area) |
| `origin` | string | — | ✓ | Shipping source location |
| `destination` | string | — | ✓ | Delivery destination address |
| `timestamp` | integer | Unix epoch ms | ✓ | Location update timestamp |

### Valid Status Values

- `pending` — Awaiting pickup
- `in_transit` — Currently being delivered
- `delivered` — Successfully delivered

### Example MQTT Publish Command

```bash
mosquitto_pub -h localhost -t "devices/logistics/dispatch" \
  -m '{"shipment_id":"s5f8a9b2-c1d4-47e3-a9f2-b3c6d7e8f9a0","order_id":"a1b2c3d4-e5f6-4789-ab10-cd11ef121314","status":"in_transit","current_location":{"latitude":40.712776,"longitude":-74.005974},"origin":"NYC Warehouse","destination":"Customer Location","timestamp":1688044200000}'
```

---

## 4. Inventory Changes

**MQTT Topic**: `devices/inventory/change`  
**Message Rate**: 10–30 msg/min  
**Warehouses**: 4 locations  
**SKU Count**: 10 unique items  
**Partition Key**: `item_sku` (per-SKU consistency)  
**Kafka Topic**: `iot-inventory-changes`

### Message Format

```json
{
  "message_id": "b2c3d4e5-f6a7-4b8c-9d0e-f1a2b3c4d5e6",
  "item_sku": "SKU-003",
  "warehouse_id": "WH2",
  "quantity_delta": 50,
  "change_reason": "restock",
  "timestamp": 1688044200000
}
```

### Field Definitions

| Field | Type | Range | Required | Description |
|-------|------|-------|----------|-------------|
| `message_id` | string (UUID) | - | ✓ | Unique message identifier, generated at publish time - lets Spark upsert idempotently (`ON CONFLICT DO NOTHING`) instead of risking duplicate rows on a replay |
| `item_sku` | string | `SKU-001` to `SKU-010` | ✓ | Stock keeping unit identifier |
| `warehouse_id` | string | `WH1`, `WH2`, `WH3`, `WH4` | ✓ | Warehouse location code |
| `quantity_delta` | integer | -50 to +50 | ✓ | Change in stock (+ restock, - sale/loss) |
| `change_reason` | string | see reasons | ✓ | Reason for inventory change |
| `timestamp` | integer | Unix epoch ms | ✓ | Change event timestamp |

### Valid Change Reasons

- `purchase` — Stock purchased/received
- `sale` — Stock sold/shipped
- `adjustment` — Inventory count adjustment
- `return` — Customer/vendor return
- `restock` — Internal replenishment

### Example MQTT Publish Command

```bash
mosquitto_pub -h localhost -t "devices/inventory/change" \
  -m '{"message_id":"b2c3d4e5-f6a7-4b8c-9d0e-f1a2b3c4d5e6","item_sku":"SKU-003","warehouse_id":"WH2","quantity_delta":50,"change_reason":"restock","timestamp":1688044200000}'
```

---

## 5. User Events / Activity

**MQTT Topic**: `devices/users/event`  
**Message Rate**: 200–500 msg/min  
**Unique Users**: 100 simulated user IDs  
**Active Sessions**: ~50 concurrent sessions  
**Partition Key**: `user_id` (per-user session ordering)  
**Kafka Topic**: `iot-users-activity`

### Message Format

```json
{
  "message_id": "c3d4e5f6-a7b8-4c9d-0e1f-a2b3c4d5e6f7",
  "user_id": 5234,
  "event_type": "click",
  "page": "products",
  "session_id": "sess-a7b8c9d0-e1f2-4g3h-i4j5-k6l7m8n9o0p1",
  "event_value": "42",
  "timestamp": 1688044200000
}
```

### Field Definitions

| Field | Type | Range | Required | Description |
|-------|------|-------|----------|-------------|
| `message_id` | string (UUID) | - | ✓ | Unique message identifier, generated at publish time - lets Spark upsert idempotently (`ON CONFLICT DO NOTHING`) instead of risking duplicate rows on a replay |
| `user_id` | integer | 1–10000 | ✓ | Unique user identifier |
| `event_type` | string | see event types | ✓ | Type of user action |
| `page` | string | see pages | ✓ | Page or view where event occurred |
| `session_id` | string (UUID) | — | ✓ | User session identifier (persists across events for same user) |
| `event_value` | string | `1–100` | ✓ | Numeric event value (e.g., click count, price, score) |
| `timestamp` | integer | Unix epoch ms | ✓ | Event occurrence timestamp |

### Valid Event Types

- `click` — User clicked element
- `view` — Page view or impression
- `add_to_cart` — Item added to shopping cart
- `purchase` — Completed purchase
- `login` — User session started
- `logout` — User session ended

### Valid Page Values

- `home` — Homepage
- `products` — Product listing/catalog
- `checkout` — Checkout/payment page
- `profile` — User profile page
- `search` — Search results page
- `wishlist` — Saved items/wishlist

### Example MQTT Publish Command

```bash
mosquitto_pub -h localhost -t "devices/users/event" \
  -m '{"message_id":"c3d4e5f6-a7b8-4c9d-0e1f-a2b3c4d5e6f7","user_id":5234,"event_type":"click","page":"products","session_id":"sess-a7b8c9d0-e1f2-4g3h-i4j5-k6l7m8n9o0p1","event_value":"42","timestamp":1688044200000}'
```

---

## General Message Characteristics

### Timestamp Field

All messages include a `timestamp` field in **milliseconds since Unix epoch** (not seconds). This is important for:
- Accurate ordering in Kafka partitions
- Spark job timestamp processing
- PostgreSQL `recorded_at` column mapping

**Example**: `1688044200000` = 2023-06-29 10:30:00 UTC

### JSON Format

- All messages are valid JSON (UTF-8 encoded)
- No nested objects deeper than 2 levels (except items array in orders)
- String fields use double quotes
- Numeric fields (temperature, quantity) are not quoted

### MQTT Publish Behavior

- **QoS Level**: 1 (at least once delivery)
- **Retain**: false (messages not persisted in MQTT)
- **Payload Size**: typically 100–500 bytes per message

### Validation Rules

Each Spark ingestion job validates:
1. All required fields present
2. Data types match schema
3. Numeric values within specified ranges
4. Timestamp is reasonable (within 24 hours)

Invalid messages are **silently dropped** (no dead-letter queue in demo version).

---

## Testing & Debugging

### Publish Test Message

The `emqx` image has no `mosquitto_pub`/`mosquitto_sub` binaries - install
`mosquitto-clients` on the host (`sudo apt install mosquitto-clients`), or use
the Dockerized alternative (see [SETUP.md](SETUP.md) "Test MQTT → EMQX"):
```bash
# Test weather message
mosquitto_pub -h localhost -t "devices/weather/data" \
  -m '{"device_id":"weather-sensor-test","temperature":25.0,"humidity":50,"pressure":1013.25,"timestamp":'$(date +%s)'000}'

# List MQTT messages in Kafka
docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic iot-weather-data --from-beginning --max-messages 10 --timeout-ms 5000
```

### Monitor Message Flow

```bash
# Watch MQTT messages in real-time
mosquitto_sub -h localhost -t "devices/+" -v

# Monitor Kafka topic partitions
docker exec kafka kafka-topics --bootstrap-server kafka:9092 \
  --describe --topic iot-weather-data
```

---

## References

- **MQTT Protocol**: [mqtt.org](https://mqtt.org/)
- **JSON Schema**: [json-schema.org](https://json-schema.org/)
- **Kafka Topics**: See [ARCHITECTURE.md](ARCHITECTURE.md) — Kafka Partitioning Strategy
- **MQTT Generators**: See source code in `mqtt-generators/`
