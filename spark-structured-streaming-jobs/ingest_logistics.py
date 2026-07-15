# Spark Job: Ingest Logistics/Shipment Data
# Reads from iot-logistics-dispatch Kafka topic, transforms coordinates, writes to PostgreSQL

import logging
import psycopg2
from psycopg2.extras import execute_values
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

        # Write each partition directly via psycopg2 with ON CONFLICT DO UPDATE,
        # keyed on shipment_id. Unlike order_id (fresh per message), the mock
        # generator deliberately reuses a small pool of shipment_ids across many
        # messages to simulate a shipment's tracking history (new status, new
        # GPS fix) - so a repeat shipment_id is a legitimate update, not a
        # replay/retry duplicate, and must overwrite the row rather than be
        # dropped. This also means a single micro-batch can contain the same
        # shipment_id more than once; Postgres forbids ON CONFLICT DO UPDATE
        # from affecting the same row twice in one statement, so we keep only
        # the last (most recent) row per shipment_id before upserting.
        # Spark's JDBC DataFrameWriter has no upsert mode, hence the raw SQL here.
        def write_partition(rows):
            rows = list(rows)
            if not rows:
                return
            deduped = list({
                r.shipment_id: r for r in sorted(rows, key=lambda r: r.last_update)
            }.values())
            conn = psycopg2.connect(**SparkSessionFactory.get_psycopg2_dsn())
            try:
                with conn, conn.cursor() as cur:
                    execute_values(
                        cur,
                        """
                        INSERT INTO iot.logistics_shipments
                            (shipment_id, order_id, shipment_status, current_location,
                             origin_location, destination_location, last_update, ingested_at)
                        VALUES %s
                        ON CONFLICT (shipment_id) DO UPDATE SET
                            order_id = EXCLUDED.order_id,
                            shipment_status = EXCLUDED.shipment_status,
                            current_location = EXCLUDED.current_location,
                            origin_location = EXCLUDED.origin_location,
                            destination_location = EXCLUDED.destination_location,
                            last_update = EXCLUDED.last_update,
                            ingested_at = EXCLUDED.ingested_at
                        """,
                        [
                            (r.shipment_id, r.order_id, r.shipment_status, r.current_location,
                             r.origin_location, r.destination_location, r.last_update, r.ingested_at)
                            for r in deduped
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
                batch_df, ["shipment_id", "order_id", "shipment_status"]
            )

            try:
                batch_df.foreachPartition(write_partition)
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
