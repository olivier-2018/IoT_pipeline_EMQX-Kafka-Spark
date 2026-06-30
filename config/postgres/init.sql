-- PostgreSQL Schema Initialization for IoT Data Pipeline
-- Automatically run on first container startup

-- Enable necessary extensions
CREATE EXTENSION IF NOT EXISTS uuid_ossp;

-- Create schema
CREATE SCHEMA IF NOT EXISTS iot;
SET search_path TO iot;

-- Weather Data Table
-- Stores temperature, humidity, and other weather metrics
CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(50) NOT NULL,
    temperature DECIMAL(5,2),
    humidity DECIMAL(5,2),
    pressure DECIMAL(7,2),
    location VARCHAR(100),
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_weather_device_time 
    ON weather_data(device_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_weather_ingested 
    ON weather_data(ingested_at DESC);

-- Sales Orders Table
-- Stores customer orders with order details
CREATE TABLE IF NOT EXISTS sales_orders (
    order_id UUID PRIMARY KEY,
    customer_id INT NOT NULL,
    order_status VARCHAR(20) DEFAULT 'pending',
    total_amount DECIMAL(10,2) NOT NULL,
    item_count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_orders_customer_time 
    ON sales_orders(customer_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_status 
    ON sales_orders(order_status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_ingested 
    ON sales_orders(ingested_at DESC);

-- Logistics Shipments Table
-- Tracks shipment status and location updates
CREATE TABLE IF NOT EXISTS logistics_shipments (
    shipment_id UUID PRIMARY KEY,
    order_id UUID NOT NULL,
    shipment_status VARCHAR(20) DEFAULT 'pending',
    current_location POINT,
    origin_location VARCHAR(100),
    destination_location VARCHAR(100),
    last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_logistics_order_id 
    ON logistics_shipments(order_id);
CREATE INDEX IF NOT EXISTS idx_logistics_status 
    ON logistics_shipments(shipment_status, last_update DESC);
CREATE INDEX IF NOT EXISTS idx_logistics_ingested 
    ON logistics_shipments(ingested_at DESC);

-- Inventory Changes Table
-- Records inventory stock changes per SKU
CREATE TABLE IF NOT EXISTS inventory_changes (
    id SERIAL PRIMARY KEY,
    item_sku VARCHAR(50) NOT NULL,
    warehouse_id VARCHAR(50),
    quantity_delta INT NOT NULL,
    current_stock INT DEFAULT 0,
    change_reason VARCHAR(100),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_inventory_sku_time 
    ON inventory_changes(item_sku, changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_inventory_warehouse 
    ON inventory_changes(warehouse_id, changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_inventory_ingested 
    ON inventory_changes(ingested_at DESC);

-- User Events Table
-- Logs user activity events (clicks, page views, add-to-cart, etc.)
CREATE TABLE IF NOT EXISTS user_events (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_action VARCHAR(100),
    page_or_resource VARCHAR(200),
    event_value VARCHAR(500),
    session_id VARCHAR(100),
    event_timestamp TIMESTAMP NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_user_events_user_time 
    ON user_events(user_id, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_user_events_type 
    ON user_events(event_type, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_user_events_session 
    ON user_events(session_id, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_user_events_ingested 
    ON user_events(ingested_at DESC);

-- Create a simple stats table for monitoring
CREATE TABLE IF NOT EXISTS ingest_stats (
    metric_name VARCHAR(100) NOT NULL,
    metric_value INT NOT NULL,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ingest_stats_metric_time 
    ON ingest_stats(metric_name, recorded_at DESC);

-- Grant privileges (optional - for demo only)
GRANT ALL PRIVILEGES ON SCHEMA iot TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA iot TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA iot TO postgres;

-- Initialize schema info
INSERT INTO iot.ingest_stats (metric_name, metric_value) VALUES 
    ('schema_initialized', 1)
ON CONFLICT DO NOTHING;
