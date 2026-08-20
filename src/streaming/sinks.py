"""
Streaming output sink handlers for Apache Kafka, S3/MinIO Parquet tables, and Console output.
"""

import os
from typing import Any, Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_json, struct
from pyspark.sql.streaming import StreamingQuery

from src.common.logging_utils import get_logger

logger = get_logger(__name__)


class S3ParquetSink:
    """
    Writes streaming DataFrames to S3/MinIO in partitioned Parquet format with checkpointing.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.storage_config = config.get("storage", {})
        self.events_path = self.storage_config.get("events_path", "s3a://clickstream-warehouse/events/")
        self.anomalies_path = self.storage_config.get("anomalies_path", "s3a://clickstream-warehouse/anomalies/")
        self.checkpoint_base = self.storage_config.get(
            "checkpoint_base_path", "s3a://clickstream-warehouse/checkpoints/"
        )
        self.trigger_time = config.get("spark", {}).get("trigger_processing_time", "5 seconds")

    def write_events_stream(self, df: DataFrame, checkpoint_dir: str = "raw_events") -> StreamingQuery:
        """
        Streams raw parsed events into S3 Parquet dataset partitioned by action.
        """
        checkpoint_location = f"{self.checkpoint_base.rstrip('/')}/{checkpoint_dir}"
        logger.info("Configuring S3 Parquet Sink for events: %s (checkpoint: %s)", self.events_path, checkpoint_location)

        query = (
            df.writeStream
            .format("parquet")
            .option("path", self.events_path)
            .option("checkpointLocation", checkpoint_location)
            .partitionBy("action")
            .trigger(processingTime=self.trigger_time)
            .outputMode("append")
            .start()
        )
        return query

    def write_anomalies_stream(self, df: DataFrame, checkpoint_dir: str = "anomalies_s3") -> StreamingQuery:
        """
        Streams detected anomalies to S3 Parquet dataset.
        """
        checkpoint_location = f"{self.checkpoint_base.rstrip('/')}/{checkpoint_dir}"
        logger.info("Configuring S3 Parquet Sink for anomalies: %s", self.anomalies_path)

        query = (
            df.writeStream
            .format("parquet")
            .option("path", self.anomalies_path)
            .option("checkpointLocation", checkpoint_location)
            .trigger(processingTime=self.trigger_time)
            .outputMode("append")
            .start()
        )
        return query


class KafkaAnomalySink:
    """
    Publishes detected anomaly records back into Apache Kafka as JSON strings.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.kafka_config = config.get("kafka", {})
        self.storage_config = config.get("storage", {})
        self.bootstrap_servers = self.kafka_config.get("bootstrap_servers", "localhost:9092")
        self.anomaly_topic = self.kafka_config.get("output_anomaly_topic", "events.anomalies")
        self.checkpoint_base = self.storage_config.get(
            "checkpoint_base_path", "s3a://clickstream-warehouse/checkpoints/"
        )
        self.trigger_time = config.get("spark", {}).get("trigger_processing_time", "5 seconds")

    def write_anomaly_stream(self, df: DataFrame, checkpoint_dir: str = "anomalies_kafka") -> StreamingQuery:
        """
        Serializes anomaly rows into JSON messages and writes to Kafka topic.
        """
        checkpoint_location = f"{self.checkpoint_base.rstrip('/')}/{checkpoint_dir}"
        logger.info("Configuring Kafka Anomaly Sink topic: %s", self.anomaly_topic)

        kafka_formatted_df = df.select(
            col("user_id").cast("string").alias("key"),
            to_json(
                struct(
                    col("window_start"),
                    col("window_end"),
                    col("user_id"),
                    col("ip_address"),
                    col("anomaly_type"),
                    col("severity"),
                    col("event_count"),
                    col("total_amount"),
                    col("details"),
                    col("detection_timestamp"),
                )
            ).alias("value"),
        )

        query = (
            kafka_formatted_df.writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("topic", self.anomaly_topic)
            .option("checkpointLocation", checkpoint_location)
            .trigger(processingTime=self.trigger_time)
            .outputMode("update")
            .start()
        )
        return query


def create_console_sink(df: DataFrame, query_name: str = "console_anomalies", output_mode: str = "update") -> StreamingQuery:
    """
    Creates an in-console monitoring sink for debugging.
    """
    return (
        df.writeStream
        .queryName(query_name)
        .format("console")
        .option("truncate", "false")
        .outputMode(output_mode)
        .start()
    )
