# User Events Generator
# Generates user activity events (clicks, page views, add-to-cart, etc.)

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

class UserEventsGenerator:
    def __init__(self):
        self.mqtt_client = mqtt.Client(client_id="user_events_generator")
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_disconnect = self.on_disconnect
        self.config = DEVICE_CONFIG["users"]
        self.ranges = DATA_RANGES["users"]
        self.connected = False
        # Pre-generate session IDs for realistic user sessions
        self.active_sessions = {
            random.randint(*self.ranges["user_id_range"]): str(uuid.uuid4())
            for _ in range(50)
        }

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.connected = True
            logger.info("User Events Generator: Connected to MQTT broker")
        else:
            logger.error(f"User Events Generator: Failed to connect, return code {rc}")

    def on_disconnect(self, client, userdata, rc):
        self.connected = False
        if rc != 0:
            logger.warning(f"User Events Generator: Unexpected disconnect, code {rc}")

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
            logger.error(f"User Events Generator: Connection error - {e}")
            raise

    def disconnect(self):
        """Disconnect from MQTT broker"""
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()

    def generate_record(self) -> dict:
        """Generate a single user event record"""
        user_id = random.randint(*self.ranges["user_id_range"])
        
        # Get or create session for this user
        if user_id not in self.active_sessions:
            self.active_sessions[user_id] = str(uuid.uuid4())
        
        return {
            "user_id": user_id,
            "event_type": random.choice(self.ranges["actions"]),
            "page": random.choice(self.ranges["pages"]),
            "session_id": self.active_sessions[user_id],
            "event_value": str(random.randint(1, 100)),
            "timestamp": int(datetime.now().timestamp() * 1000),
        }

    def generate_batch(self, batch_size: int = None) -> list:
        """Generate a batch of user event records"""
        if batch_size is None:
            batch_size = self.config["records_per_batch"]

        records = [self.generate_record() for _ in range(batch_size)]
        return records

    def publish_batch(self, records: list) -> int:
        """Publish batch of records to MQTT"""
        if not self.connected:
            logger.warning("User Events Generator: Not connected to MQTT")
            return 0

        published_count = 0
        for record in records:
            try:
                topic = f"{self.config['topic_prefix']}/event"
                payload = json.dumps(record)
                self.mqtt_client.publish(
                    topic, payload, qos=MQTT_CONFIG["qos"], retain=False
                )
                published_count += 1
            except Exception as e:
                logger.error(f"User Events Generator: Publish error - {e}")

        return published_count

    def run_batch(self) -> int:
        """Generate and publish a single batch"""
        records = self.generate_batch()
        published = self.publish_batch(records)
        logger.info(f"User Events Generator: Published {published}/{len(records)} records")
        return published


if __name__ == "__main__":
    generator = UserEventsGenerator()
    try:
        generator.connect()
        generator.run_batch()
    except KeyboardInterrupt:
        logger.info("User Events Generator: Interrupted by user")
    finally:
        generator.disconnect()
