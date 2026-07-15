# Spark Job: Ingest Logistics/Shipment Data
# Reads from iot-logistics-dispatch Kafka topic, transforms coordinates, writes to PostgreSQL

import logging
from pyspark.sql.functions import col, from_json, current_timestamp, to_timestamp, concat_ws

from shared_utils import (
    SparkSessionFactory, SchemaRegistry, KafkaConfig, DataValidator
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Main function for logistics data ingestion"""
    
    # Create Spark session
    spark = SparkSessionFactory.create_session("LogisticsDataIngestion")
    logger.info("✓ Spark session created")

    try:
        # Read from Kafka
        kafka_df = (
            spark.readStream
            .format("kafka")
            .options(**KafkaConfig.get_kafka_options("iot-logistics-dispatch"))
            .load()
        )

        # Parse JSON from Kafka value
        schema = SchemaRegistry.get_logistics_schema()
        parsed_df = kafka_df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")

        # Transform: convert GPS coordinates to PostgreSQL POINT format
        transformed_df = (
            parsed_df
            .withColumn("last_update", to_timestamp(col("timestamp") / 1000))
            .withColumn("ingested_at", current_timestamp())
            .withColumn(
                "current_location",
                concat_ws(",", col("current_location.latitude"), col("current_location.longitude"))
            )
            .select(
                col("shipment_id"),
                col("order_id"),
                col("status").alias("shipment_status"),
                col("current_location"),
                col("origin").alias("origin_location"),
                col("destination").alias("destination_location"),
                col("last_update"),
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
                batch_df, ["shipment_id", "order_id", "shipment_status"]
            )

            jdbc_options = SparkSessionFactory.get_jdbc_options("logistics_shipments")

            try:
                batch_df.write \
                    .format("jdbc") \
                    .options(**jdbc_options) \
                    .mode("append") \
                    .save()

                logger.info(f"[Batch {batch_id}] ✓ Wrote {count} logistics records to PostgreSQL")
            except Exception as e:
                logger.error(f"[Batch {batch_id}] Error writing to PostgreSQL: {e}")
                raise

        # Start streaming
        query = (
            transformed_df
            .writeStream
            .foreachBatch(write_to_postgres)
            .option("checkpointLocation", "/tmp/spark-data/checkpoints/logistics")
            .start()
        )

        logger.info("✓ Logistics ingestion stream started")
        logger.info("Listening for messages on iot-logistics-dispatch...")
        
        # Keep the stream running
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Fatal error in logistics ingestion: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
