# Inventory Changes Generator
# Generates inventory stock changes per SKU and warehouse

import json
import random
import logging
import uuid
from datetime import datetime
import paho.mqtt.client as mqtt
from config import MQTT_CONFIG, DEVICE_CONFIG, DATA_RANGES, LOG_FORMAT

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

class InventoryGenerator:
    def __init__(self):
        self.mqtt_client = mqtt.Client(client_id="inventory_generator")
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_disconnect = self.on_disconnect
        self.config = DEVICE_CONFIG["inventory"]
        self.ranges = DATA_RANGES["inventory"]
        self.connected = False
        self.sample_skus = [
            "SKU-001", "SKU-002", "SKU-003", "SKU-004", "SKU-005",
            "SKU-006", "SKU-007", "SKU-008", "SKU-009", "SKU-010",
        ]
        self.reasons = ["purchase", "sale", "adjustment", "return", "restock"]

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.connected = True
            logger.info("Inventory Generator: Connected to MQTT broker")
        else:
            logger.error(f"Inventory Generator: Failed to connect, return code {rc}")

    def on_disconnect(self, client, userdata, rc):
        self.connected = False
        if rc != 0:
            logger.warning(f"Inventory Generator: Unexpected disconnect, code {rc}")

    def connect(self):
        """Connect to MQTT broker"""
        try:
            self.mqtt_client.connect(
                MQTT_CONFIG["host"],
                MQTT_CONFIG["port"],
                MQTT_CONFIG["keepalive"],
            )
            self.mqtt_client.loop_start()
        except Exception as e:
            logger.error(f"Inventory Generator: Connection error - {e}")
            raise

    def disconnect(self):
        """Disconnect from MQTT broker"""
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()

    def generate_record(self) -> dict:
        """Generate a single inventory change record"""
        return {
            "message_id": str(uuid.uuid4()),
            "item_sku": random.choice(self.sample_skus),
            "warehouse_id": random.choice(self.ranges["warehouse_ids"]),
            "quantity_delta": random.randint(*self.ranges["qty_delta_range"]),
            "change_reason": random.choice(self.reasons),
            "timestamp": int(datetime.now().timestamp() * 1000),
        }

    def generate_batch(self, batch_size: int = None) -> list:
        """Generate a batch of inventory records"""
        if batch_size is None:
            batch_size = self.config["records_per_batch"]

        records = [self.generate_record() for _ in range(batch_size)]
        return records

    def publish_batch(self, records: list) -> int:
        """Publish batch of records to MQTT"""
        if not self.connected:
            logger.warning("Inventory Generator: Not connected to MQTT")
            return 0

        published_count = 0
        for record in records:
            try:
                topic = f"{self.config['topic_prefix']}/change"
                payload = json.dumps(record)
                self.mqtt_client.publish(
                    topic, payload, qos=MQTT_CONFIG["qos"], retain=False
                )
                published_count += 1
            except Exception as e:
                logger.error(f"Inventory Generator: Publish error - {e}")

        return published_count

    def run_batch(self) -> int:
        """Generate and publish a single batch"""
        records = self.generate_batch()
        published = self.publish_batch(records)
        logger.info(f"Inventory Generator: Published {published}/{len(records)} records")
        return published


if __name__ == "__main__":
    generator = InventoryGenerator()
    try:
        generator.connect()
        generator.run_batch()
    except KeyboardInterrupt:
        logger.info("Inventory Generator: Interrupted by user")
    finally:
        generator.disconnect()
