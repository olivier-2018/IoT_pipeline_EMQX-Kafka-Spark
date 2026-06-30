## EMQX Rule Engine SQL: Forward MQTT messages to Kafka
## Topics: devices/{device_type}/{attribute} -> Kafka iot-{device_type}-data

-- Weather Data Rule
-- MQTT: devices/weather/* -> Kafka: iot-weather-data
CREATE RULE weather_to_kafka AS
SELECT * FROM "devices/weather/+"
FOREACH do_to_kafka('weather', payload)
;

-- Sales Orders Rule
-- MQTT: devices/orders/* -> Kafka: iot-orders-events
CREATE RULE orders_to_kafka AS
SELECT * FROM "devices/orders/+"
FOREACH do_to_kafka('orders', payload)
;

-- Logistics Rule
-- MQTT: devices/logistics/* -> Kafka: iot-logistics-dispatch
CREATE RULE logistics_to_kafka AS
SELECT * FROM "devices/logistics/+"
FOREACH do_to_kafka('logistics', payload)
;

-- Inventory Rule
-- MQTT: devices/inventory/* -> Kafka: iot-inventory-changes
CREATE RULE inventory_to_kafka AS
SELECT * FROM "devices/inventory/+"
FOREACH do_to_kafka('inventory', payload)
;

-- User Events Rule
-- MQTT: devices/users/* -> Kafka: iot-users-activity
CREATE RULE users_to_kafka AS
SELECT * FROM "devices/users/+"
FOREACH do_to_kafka('users', payload)
;

-- Helper action to send to Kafka (configured via EMQX Kafka bridge connector)
-- Note: Actual Kafka bridge connection is configured in docker-entrypoint
-- This file documents the rule structure; bridge setup via EMQX console or environment
