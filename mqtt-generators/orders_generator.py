# Sales Orders Generator
# Generates realistic sales order data (order_id, customer_id, items, amount)

import json
import random
import logging
import uuid
from datetime import datetime
import paho.mqtt.client as mqtt
from faker import Faker
from config import MQTT_CONFIG, DEVICE_CONFIG, DATA_RANGES, LOG_FORMAT

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)
fake = Faker()

class OrdersGenerator:
    def __init__(self):
        self.mqtt_client = mqtt.Client(client_id="orders_generator")
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_disconnect = self.on_disconnect
        self.config = DEVICE_CONFIG["orders"]
        self.ranges = DATA_RANGES["orders"]
        self.connected = False
        self.sample_products = [
            "Widget-A", "Widget-B", "Gadget-X", "Tool-Y", "Accessory-Z"
        ]

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.connected = True
            logger.info("Orders Generator: Connected to MQTT broker")
        else:
            logger.error(f"Orders Generator: Failed to connect, return code {rc}")

    def on_disconnect(self, client, userdata, rc):
        self.connected = False
        if rc != 0:
            logger.warning(f"Orders Generator: Unexpected disconnect, code {rc}")

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
            logger.error(f"Orders Generator: Connection error - {e}")
            raise

    def disconnect(self):
        """Disconnect from MQTT broker"""
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()

    def generate_record(self) -> dict:
        """Generate a single sales order record"""
        item_count = random.randint(*self.ranges["item_count_range"])
        items = [
            {
                "product": random.choice(self.sample_products),
                "quantity": random.randint(1, 5),
                "price": round(random.uniform(10.0, 100.0), 2),
            }
            for _ in range(item_count)
        ]
        total = sum(item["quantity"] * item["price"] for item in items)

        return {
            "order_id": str(uuid.uuid4()),
            "customer_id": random.randint(*self.ranges["customer_id_range"]),
            "status": "pending",
            "items": items,
            "total_amount": round(total, 2),
            "timestamp": int(datetime.now().timestamp() * 1000),
        }

    def generate_batch(self, batch_size: int = None) -> list:
        """Generate a batch of sales orders"""
        if batch_size is None:
            batch_size = self.config["records_per_batch"]

        records = [self.generate_record() for _ in range(batch_size)]
        return records

    def publish_batch(self, records: list) -> int:
        """Publish batch of records to MQTT"""
        if not self.connected:
            logger.warning("Orders Generator: Not connected to MQTT")
            return 0

        published_count = 0
        for record in records:
            try:
                topic = f"{self.config['topic_prefix']}/new_order"
                payload = json.dumps(record)
                self.mqtt_client.publish(
                    topic, payload, qos=MQTT_CONFIG["qos"], retain=False
                )
                published_count += 1
            except Exception as e:
                logger.error(f"Orders Generator: Publish error - {e}")

        return published_count

    def run_batch(self) -> int:
        """Generate and publish a single batch"""
        records = self.generate_batch()
        published = self.publish_batch(records)
        logger.info(f"Orders Generator: Published {published}/{len(records)} records")
        return published


if __name__ == "__main__":
    generator = OrdersGenerator()
    try:
        generator.connect()
        generator.run_batch()
    except KeyboardInterrupt:
        logger.info("Orders Generator: Interrupted by user")
    finally:
        generator.disconnect()
