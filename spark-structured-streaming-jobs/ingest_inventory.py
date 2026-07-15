# Spark Job: Ingest Inventory Changes
# Reads from iot-inventory-changes Kafka topic, aggregates per SKU, writes to PostgreSQL

import logging
import psycopg2
from psycopg2.extras import execute_values
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
            .options(**KafkaConfig.get_kafka_options("iot-inventory-changes"))
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
                col("message_id"),
                col("item_sku"),
                col("warehouse_id"),
                col("quantity_delta"),
                col("change_reason"),
                col("changed_at"),
                col("ingested_at")
            )
        )

        # Write each partition directly via psycopg2 with ON CONFLICT DO NOTHING,
        # keyed on the generator-issued message_id, so replaying an
        # already-partially-written micro-batch (checkpoint restart, or a
        # retried task within the same batch) skips rows that already made it
        # into Postgres instead of inserting silent duplicates.
        def write_partition(rows):
            rows = list(rows)
            if not rows:
                return
            conn = psycopg2.connect(**SparkSessionFactory.get_psycopg2_dsn())
            try:
                with conn, conn.cursor() as cur:
                    execute_values(
                        cur,
                        """
                        INSERT INTO iot.inventory_changes
                            (message_id, item_sku, warehouse_id, quantity_delta, change_reason, changed_at, ingested_at)
                        VALUES %s
                        ON CONFLICT (message_id) DO NOTHING
                        """,
                        [
                            (r.message_id, r.item_sku, r.warehouse_id, r.quantity_delta,
                             r.change_reason, r.changed_at, r.ingested_at)
                            for r in rows
                        ],
                    )
            finally:
                conn.close()

        # Write to PostgreSQL in micro-batches
        def write_to_postgres(batch_df, batch_id):
            """Write batch to PostgreSQL"""
            count = batch_df.count()
            if count == 0:
                logger.info(f"[Batch {batch_id}] No data to write")
                return

            DataValidator.validate_required_fields(
                batch_df, ["message_id", "item_sku", "warehouse_id", "quantity_delta"]
            )

            try:
                batch_df.foreachPartition(write_partition)
                logger.info(f"[Batch {batch_id}] ✓ Wrote {count} inventory records to PostgreSQL")
            except Exception as e:
                logger.error(f"[Batch {batch_id}] Error writing to PostgreSQL: {e}")
                raise

        # Start streaming
        query = (
            transformed_df
            .writeStream
            .foreachBatch(write_to_postgres)
            .option("checkpointLocation", "/tmp/spark-data/checkpoints/inventory")
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
