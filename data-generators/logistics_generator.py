# Logistics/Shipment Tracking Generator
# Generates shipment status updates with GPS coordinates

import json
import random
import logging
import uuid
from datetime import datetime
import paho.mqtt.client as mqtt
from config import MQTT_CONFIG, DEVICE_CONFIG, DATA_RANGES, LOG_FORMAT

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

class LogisticsGenerator:
    def __init__(self):
        self.mqtt_client = mqtt.Client(client_id="logistics_generator")
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_disconnect = self.on_disconnect
        self.config = DEVICE_CONFIG["logistics"]
        self.ranges = DATA_RANGES["logistics"]
        self.connected = False
        # Pre-generate shipment IDs for realistic tracking
        self.active_shipments = [str(uuid.uuid4()) for _ in range(10)]

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.connected = True
            logger.info("Logistics Generator: Connected to MQTT broker")
        else:
            logger.error(f"Logistics Generator: Failed to connect, return code {rc}")

    def on_disconnect(self, client, userdata, rc):
        self.connected = False
        if rc != 0:
            logger.warning(f"Logistics Generator: Unexpected disconnect, code {rc}")

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
            logger.error(f"Logistics Generator: Connection error - {e}")
            raise

    def disconnect(self):
        """Disconnect from MQTT broker"""
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()

    def generate_record(self) -> dict:
        """Generate a single logistics/shipment record"""
        return {
            "shipment_id": random.choice(self.active_shipments),
            "order_id": str(uuid.uuid4()),
            "status": random.choice(self.ranges["statuses"]),
            "current_location": {
                "latitude": round(random.uniform(*self.ranges["lat_range"]), 6),
                "longitude": round(random.uniform(*self.ranges["lon_range"]), 6),
            },
            "origin": "NYC Warehouse",
            "destination": "Customer Location",
            "timestamp": int(datetime.now().timestamp() * 1000),
        }

    def generate_batch(self, batch_size: int = None) -> list:
        """Generate a batch of logistics records"""
        if batch_size is None:
            batch_size = self.config["records_per_batch"]

        records = [self.generate_record() for _ in range(batch_size)]
        return records

    def publish_batch(self, records: list) -> int:
        """Publish batch of records to MQTT"""
        if not self.connected:
            logger.warning("Logistics Generator: Not connected to MQTT")
            return 0

        published_count = 0
        for record in records:
            try:
                topic = f"{self.config['topic_prefix']}/dispatch"
                payload = json.dumps(record)
                self.mqtt_client.publish(
                    topic, payload, qos=MQTT_CONFIG["qos"], retain=False
                )
                published_count += 1
            except Exception as e:
                logger.error(f"Logistics Generator: Publish error - {e}")

        return published_count

    def run_batch(self) -> int:
        """Generate and publish a single batch"""
        records = self.generate_batch()
        published = self.publish_batch(records)
        logger.info(f"Logistics Generator: Published {published}/{len(records)} records")
        return published


if __name__ == "__main__":
    generator = LogisticsGenerator()
    try:
        generator.connect()
        generator.run_batch()
    except KeyboardInterrupt:
        logger.info("Logistics Generator: Interrupted by user")
    finally:
        generator.disconnect()
