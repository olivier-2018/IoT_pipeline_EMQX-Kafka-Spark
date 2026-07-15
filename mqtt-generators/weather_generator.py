# Weather Data Generator
# Generates realistic weather data (temperature, humidity, pressure)

import json
import random
import logging
import uuid
from datetime import datetime
import paho.mqtt.client as mqtt
from config import MQTT_CONFIG, DEVICE_CONFIG, DATA_RANGES, LOG_FORMAT

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

class WeatherGenerator:
    def __init__(self):
        self.mqtt_client = mqtt.Client(client_id="weather_generator")
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_disconnect = self.on_disconnect
        self.config = DEVICE_CONFIG["weather"]
        self.ranges = DATA_RANGES["weather"]
        self.connected = False

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.connected = True
            logger.info("Weather Generator: Connected to MQTT broker")
        else:
            logger.error(f"Weather Generator: Failed to connect, return code {rc}")

    def on_disconnect(self, client, userdata, rc):
        self.connected = False
        if rc != 0:
            logger.warning(f"Weather Generator: Unexpected disconnect, code {rc}")

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
            logger.error(f"Weather Generator: Connection error - {e}")
            raise

    def disconnect(self):
        """Disconnect from MQTT broker"""
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()

    def generate_record(self, device_id: str) -> dict:
        """Generate a single weather record"""
        return {
            "message_id": str(uuid.uuid4()),
            "device_id": device_id,
            "temperature": round(random.uniform(*self.ranges["temperature"]), 2),
            "humidity": random.randint(*self.ranges["humidity"]),
            "pressure": round(random.uniform(*self.ranges["pressure"]), 2),
            "timestamp": int(datetime.now().timestamp() * 1000),
        }

    def generate_batch(self, batch_size: int = None) -> list:
        """Generate a batch of weather records"""
        if batch_size is None:
            batch_size = self.config["records_per_batch"]

        records = []
        for _ in range(batch_size):
            device_id = f"weather-sensor-{random.randint(1, self.config['device_count']):02d}"
            record = self.generate_record(device_id)
            records.append(record)

        return records

    def publish_batch(self, records: list) -> int:
        """Publish batch of records to MQTT"""
        if not self.connected:
            logger.warning("Weather Generator: Not connected to MQTT")
            return 0

        published_count = 0
        for record in records:
            try:
                topic = f"{self.config['topic_prefix']}/data"
                payload = json.dumps(record)
                self.mqtt_client.publish(
                    topic, payload, qos=MQTT_CONFIG["qos"], retain=False
                )
                published_count += 1
            except Exception as e:
                logger.error(f"Weather Generator: Publish error - {e}")

        return published_count

    def run_batch(self) -> int:
        """Generate and publish a single batch"""
        records = self.generate_batch()
        published = self.publish_batch(records)
        logger.info(f"Weather Generator: Published {published}/{len(records)} records")
        return published


if __name__ == "__main__":
    generator = WeatherGenerator()
    try:
        generator.connect()
        # Run one batch
        generator.run_batch()
    except KeyboardInterrupt:
        logger.info("Weather Generator: Interrupted by user")
    finally:
        generator.disconnect()
