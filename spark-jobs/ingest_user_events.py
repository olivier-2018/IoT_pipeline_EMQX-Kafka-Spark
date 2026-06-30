# Spark Job: Ingest User Events Data
# Reads from iot-users-activity Kafka topic, aggregates by user/action, writes to PostgreSQL

import logging
from pyspark.sql.functions import col, from_json, current_timestamp, to_timestamp

from shared_utils import (
    SparkSessionFactory, SchemaRegistry, KafkaConfig, DataValidator
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Main function for user events data ingestion"""
    
    # Create Spark session
    spark = SparkSessionFactory.create_session("UserEventsDataIngestion")
    logger.info("✓ Spark session created")

    try:
        # Read from Kafka
        kafka_df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", "iot-users-activity")
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        # Parse JSON from Kafka value
        schema = SchemaRegistry.get_user_events_schema()
        parsed_df = kafka_df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")

        # Transform and enrich
        transformed_df = (
            parsed_df
            .withColumn("event_timestamp", to_timestamp(col("timestamp") / 1000))
            .withColumn("ingested_at", current_timestamp())
            .select(
                col("user_id"),
                col("event_type"),
                col("page").alias("page_or_resource"),
                col("event_value"),
                col("session_id"),
                col("event_timestamp"),
                col("ingested_at")
            )
        )

        # Write to PostgreSQL in micro-batches
        def write_to_postgres(batch_df, batch_id):
            """Write batch to PostgreSQL"""
            if batch_df.count() == 0:
                logger.info(f"[Batch {batch_id}] No data to write")
                return

            jdbc_options = SparkSessionFactory.get_jdbc_options("user_events")
            
            try:
                batch_df.write \
                    .format("jdbc") \
                    .options(**jdbc_options) \
                    .mode("append") \
                    .save()
                
                count = batch_df.count()
                logger.info(f"[Batch {batch_id}] ✓ Wrote {count} user event records to PostgreSQL")
            except Exception as e:
                logger.error(f"[Batch {batch_id}] Error writing to PostgreSQL: {e}")

        # Start streaming
        query = (
            transformed_df
            .writeStream
            .foreachBatch(write_to_postgres)
            .option("checkpointLocation", "/tmp/user_events_checkpoint")
            .start()
        )

        logger.info("✓ User events ingestion stream started")
        logger.info("Listening for messages on iot-users-activity...")
        
        # Keep the stream running
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Fatal error in user events ingestion: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
