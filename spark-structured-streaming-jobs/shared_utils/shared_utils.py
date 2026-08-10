# Shared Spark Utilities for IoT Data Pipeline
# Provides Spark session factory, JDBC connection pooling, schema definitions

import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    LongType, ArrayType, TimestampType, DecimalType
)

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

class SparkSessionFactory:
    """Factory for creating Spark sessions configured for IoT pipeline"""

    @staticmethod
    def create_session(app_name: str, master_url: str = None) -> SparkSession:
        """
        Create a Spark session configured for IoT data processing
        
        Args:
            app_name: Name of the Spark application
            master_url: Spark master URL (defaults to environment variable or local)
        
        Returns:
            SparkSession configured with PostgreSQL driver and optimizations
        """
        if master_url is None:
            master_url = "spark://spark-master:7077"

        spark = (
            SparkSession.builder
            .appName(app_name)
            .master(master_url)
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.default.parallelism", "4")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.network.timeout", "600s")
            .config("spark.executor.heartbeatInterval", "60s")
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")
        return spark

    @staticmethod
    def get_psycopg2_dsn(username: str = "postgres", password: str = "iot_demo_pass",
                         host: str = "postgres", port: int = 5432,
                         database: str = "iot_database", schema: str = "iot") -> dict:
        """
        Connection kwargs for a raw psycopg2 connection (as opposed to Spark's
        JDBC DataFrameWriter, which has no upsert/ON CONFLICT support). Used by
        foreachPartition-based writes that need idempotent inserts.
        """
        return {
            "host": host,
            "port": port,
            "dbname": database,
            "user": username,
            "password": password,
            "options": f"-c search_path={schema}",
        }


class SchemaRegistry:
    """Registry of DataFrame schemas for all data types"""

    @staticmethod
    def get_weather_schema() -> StructType:
        """Schema for weather data from Kafka"""
        return StructType([
            StructField("message_id", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("humidity", IntegerType(), True),
            StructField("pressure", DoubleType(), True),
            StructField("timestamp", LongType(), True),
        ])

    @staticmethod
    def get_orders_schema() -> StructType:
        """Schema for sales orders from Kafka"""
        return StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("status", StringType(), True),
            StructField("items", ArrayType(StructType([
                StructField("product", StringType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("price", DoubleType(), True),
            ])), True),
            StructField("total_amount", DoubleType(), True),
            StructField("timestamp", LongType(), True),
        ])

    @staticmethod
    def get_logistics_schema() -> StructType:
        """Schema for logistics/shipment data from Kafka"""
        return StructType([
            StructField("shipment_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("current_location", StructType([
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
            ]), True),
            StructField("origin", StringType(), True),
            StructField("destination", StringType(), True),
            StructField("timestamp", LongType(), True),
        ])

    @staticmethod
    def get_inventory_schema() -> StructType:
        """Schema for inventory changes from Kafka"""
        return StructType([
            StructField("message_id", StringType(), True),
            StructField("item_sku", StringType(), True),
            StructField("warehouse_id", StringType(), True),
            StructField("quantity_delta", IntegerType(), True),
            StructField("change_reason", StringType(), True),
            StructField("timestamp", LongType(), True),
        ])

    @staticmethod
    def get_user_events_schema() -> StructType:
        """Schema for user events from Kafka"""
        return StructType([
            StructField("message_id", StringType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("event_type", StringType(), True),
            StructField("page", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("event_value", StringType(), True),
            StructField("timestamp", LongType(), True),
        ])


class KafkaConfig:
    """Kafka configuration for Spark consumers"""

    @staticmethod
    def get_kafka_options(topic: str, bootstrap_servers: str = "kafka:9092") -> dict:
        """Get Kafka options for reading from a topic"""
        return {
            "kafka.bootstrap.servers": bootstrap_servers,
            "subscribe": topic,
            "startingOffsets": "latest",
            "failOnDataLoss": "false",
            "maxOffsetsPerTrigger": "50000",
        }


class DataValidator:
    """Validation utilities for data quality checks"""

    @staticmethod
    def validate_required_fields(df, required_fields: list):
        """Check if all required fields are present in DataFrame"""
        missing = [f for f in required_fields if f not in df.columns]
        if missing:
            logger.warning(f"Missing fields: {missing}")
            return False
        return True

    @staticmethod
    def validate_non_null_count(df, column: str, min_ratio: float = 0.9):
        """Check if column has acceptable non-null ratio"""
        total = df.count()
        non_null = df.filter(f"{column} IS NOT NULL").count()
        ratio = non_null / total if total > 0 else 0
        if ratio < min_ratio:
            logger.warning(f"Column {column}: {ratio:.2%} non-null (threshold: {min_ratio:.2%})")
            return False
        return True


if __name__ == "__main__":
    # Test schema creation
    logger.info("Testing schema definitions...")
    print(SchemaRegistry.get_weather_schema())
    print(SchemaRegistry.get_orders_schema())
    print(SchemaRegistry.get_logistics_schema())
    print(SchemaRegistry.get_inventory_schema())
    print(SchemaRegistry.get_user_events_schema())
    logger.info("✓ All schemas defined successfully")
