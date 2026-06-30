# Spark Job: Ingest Inventory Changes
# Reads from iot-inventory-changes Kafka topic, aggregates per SKU, writes to PostgreSQL

import logging
from pyspark.sql.functions import col, from_json, current_timestamp, to_timestamp

from shared_utils import (
    SparkSessionFactory, SchemaRegistry, KafkaConfig, DataValidator
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Main function for inventory data ingestion"""
    
    # Create Spark session
    spark = SparkSessionFactory.create_session("InventoryDataIngestion")
    logger.info("✓ Spark session created")

    try:
        # Read from Kafka
        kafka_df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", "iot-inventory-changes")
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        # Parse JSON from Kafka value
        schema = SchemaRegistry.get_inventory_schema()
        parsed_df = kafka_df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")

        # Transform and enrich
        transformed_df = (
            parsed_df
            .withColumn("changed_at", to_timestamp(col("timestamp") / 1000))
            .withColumn("ingested_at", current_timestamp())
            .select(
                col("item_sku"),
                col("warehouse_id"),
                col("quantity_delta"),
                col("change_reason"),
                col("changed_at"),
                col("ingested_at")
            )
        )

        # Write to PostgreSQL in micro-batches
        def write_to_postgres(batch_df, batch_id):
            """Write batch to PostgreSQL"""
            if batch_df.count() == 0:
                logger.info(f"[Batch {batch_id}] No data to write")
                return

            jdbc_options = SparkSessionFactory.get_jdbc_options("inventory_changes")
            
            try:
                batch_df.write \
                    .format("jdbc") \
                    .options(**jdbc_options) \
                    .mode("append") \
                    .save()
                
                count = batch_df.count()
                logger.info(f"[Batch {batch_id}] ✓ Wrote {count} inventory records to PostgreSQL")
            except Exception as e:
                logger.error(f"[Batch {batch_id}] Error writing to PostgreSQL: {e}")

        # Start streaming
        query = (
            transformed_df
            .writeStream
            .foreachBatch(write_to_postgres)
            .option("checkpointLocation", "/tmp/inventory_checkpoint")
            .start()
        )

        logger.info("✓ Inventory ingestion stream started")
        logger.info("Listening for messages on iot-inventory-changes...")
        
        # Keep the stream running
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Fatal error in inventory ingestion: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
