# Spark Job: Ingest Weather Data
# Reads from iot-weather-data Kafka topic, validates, transforms, writes to PostgreSQL

import logging
import json
from pyspark.sql.functions import col, from_json, current_timestamp, to_timestamp

from shared_utils import (
    SparkSessionFactory, SchemaRegistry, KafkaConfig, DataValidator
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Main function for weather data ingestion"""
    
    # Create Spark session
    spark = SparkSessionFactory.create_session("WeatherDataIngestion")
    logger.info("✓ Spark session created")

    try:
        # Read from Kafka
        kafka_df = (
            spark.readStream
            .format("kafka")
            .options(**KafkaConfig.get_kafka_options("iot-weather-data"))
            .load()
        )

        # Parse JSON from Kafka value
        schema = SchemaRegistry.get_weather_schema()
        parsed_df = kafka_df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")

        # Transform and enrich
        transformed_df = (
            parsed_df
            .withColumn("recorded_at", to_timestamp(col("timestamp") / 1000))
            .withColumn("ingested_at", current_timestamp())
            .select(
                col("device_id"),
                col("temperature"),
                col("humidity"),
                col("pressure"),
                col("recorded_at"),
                col("ingested_at")
            )
        )

        # Write to PostgreSQL in micro-batches
        def write_to_postgres(batch_df, batch_id):
            """Write batch to PostgreSQL"""
            count = batch_df.count()
            if count == 0:
                logger.info(f"[Batch {batch_id}] No data to write")
                return

            DataValidator.validate_required_fields(
                batch_df, ["device_id", "temperature", "humidity", "pressure"]
            )

            jdbc_options = SparkSessionFactory.get_jdbc_options("weather_data")

            try:
                batch_df.write \
                    .format("jdbc") \
                    .options(**jdbc_options) \
                    .mode("append") \
                    .save()

                logger.info(f"[Batch {batch_id}] ✓ Wrote {count} weather records to PostgreSQL")
            except Exception as e:
                logger.error(f"[Batch {batch_id}] Error writing to PostgreSQL: {e}")
                raise

        # Start streaming
        query = (
            transformed_df
            .writeStream
            .foreachBatch(write_to_postgres)
            .option("checkpointLocation", "/tmp/spark-data/checkpoints/weather")
            .start()
        )

        logger.info("✓ Weather ingestion stream started")
        logger.info("Listening for messages on iot-weather-data...")
        
        # Keep the stream running
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Fatal error in weather ingestion: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
