# Spark Job: Ingest Sales Orders Data
# Reads from iot-orders-events Kafka topic, validates, transforms, writes to PostgreSQL

import logging
from pyspark.sql.functions import col, from_json, current_timestamp, to_timestamp, explode, size

from shared_utils import (
    SparkSessionFactory, SchemaRegistry, KafkaConfig, DataValidator
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Main function for sales orders data ingestion"""
    
    # Create Spark session
    spark = SparkSessionFactory.create_session("OrdersDataIngestion")
    logger.info("✓ Spark session created")

    try:
        # Read from Kafka
        kafka_df = (
            spark.readStream
            .format("kafka")
            .options(**KafkaConfig.get_kafka_options("iot-orders-events"))
            .load()
        )

        # Parse JSON from Kafka value
        schema = SchemaRegistry.get_orders_schema()
        parsed_df = kafka_df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")

        # Transform and enrich
        transformed_df = (
            parsed_df
            .withColumn("created_at", to_timestamp(col("timestamp") / 1000))
            .withColumn("updated_at", to_timestamp(col("timestamp") / 1000))
            .withColumn("ingested_at", current_timestamp())
            .withColumn("item_count", size(col("items")))
            .select(
                col("order_id"),
                col("customer_id"),
                col("status").alias("order_status"),
                col("total_amount"),
                col("item_count"),
                col("created_at"),
                col("updated_at"),
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
                batch_df, ["order_id", "customer_id", "order_status", "total_amount"]
            )

            jdbc_options = SparkSessionFactory.get_jdbc_options("sales_orders")

            try:
                batch_df.write \
                    .format("jdbc") \
                    .options(**jdbc_options) \
                    .mode("append") \
                    .save()

                logger.info(f"[Batch {batch_id}] ✓ Wrote {count} order records to PostgreSQL")
            except Exception as e:
                logger.error(f"[Batch {batch_id}] Error writing to PostgreSQL: {e}")
                raise

        # Start streaming
        query = (
            transformed_df
            .writeStream
            .foreachBatch(write_to_postgres)
            .option("checkpointLocation", "/tmp/spark-data/checkpoints/orders")
            .start()
        )

        logger.info("✓ Orders ingestion stream started")
        logger.info("Listening for messages on iot-orders-events...")
        
        # Keep the stream running
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Fatal error in orders ingestion: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
