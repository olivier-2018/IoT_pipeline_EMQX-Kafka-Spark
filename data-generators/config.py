# IoT Data Generation Configuration
# Shared settings for all mock data generators

import os
from typing import Dict, Any

# MQTT Configuration
MQTT_CONFIG = {
    "host": os.getenv("MQTT_HOST", "localhost"),
    "port": int(os.getenv("MQTT_PORT", "1883")),
    "qos": int(os.getenv("MQTT_QOS", "1")),
    "keepalive": 60,
}

# Data Generation Configuration
GENERATION_CONFIG = {
    "frequency_seconds": int(os.getenv("DATA_GENERATION_FREQUENCY_SECONDS", "60")),
    "batch_size_target": int(os.getenv("DATA_GENERATION_BATCH_SIZE", "500")),
    "duration_minutes": 30,  # Run for 30 minutes per default
}

# Device/Entity Configuration
DEVICE_CONFIG = {
    "weather": {
        "device_count": 10,
        "records_per_batch": 10,
        "topic_prefix": "devices/weather",
    },
    "orders": {
        "device_count": 5,  # Simulated order sources
        "records_per_batch": 100,
        "topic_prefix": "devices/orders",
    },
    "logistics": {
        "device_count": 20,  # Active shipments
        "records_per_batch": 50,
        "topic_prefix": "devices/logistics",
    },
    "inventory": {
        "device_count": 5,  # Warehouses
        "records_per_batch": 30,
        "topic_prefix": "devices/inventory",
    },
    "users": {
        "device_count": 100,  # Simulated users
        "records_per_batch": 500,
        "topic_prefix": "devices/users",
    },
}

# Realistic data ranges
DATA_RANGES = {
    "weather": {
        "temperature": (15.0, 35.0),  # Celsius
        "humidity": (30, 80),  # Percentage
        "pressure": (950, 1050),  # hPa
    },
    "orders": {
        "customer_id_range": (1, 1000),
        "amount_range": (10.0, 500.0),
        "item_count_range": (1, 10),
    },
    "logistics": {
        "lat_range": (40.0, 41.0),  # Example: New York area
        "lon_range": (-74.0, -73.0),
        "statuses": ["pending", "in_transit", "delivered"],
    },
    "inventory": {
        "qty_delta_range": (-50, 50),
        "warehouse_ids": ["WH1", "WH2", "WH3", "WH4"],
    },
    "users": {
        "user_id_range": (1, 10000),
        "actions": ["click", "view", "add_to_cart", "purchase", "login", "logout"],
        "pages": ["home", "products", "checkout", "profile", "search", "wishlist"],
    },
}

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
