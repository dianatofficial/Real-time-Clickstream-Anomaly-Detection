"""
Main PySpark Structured Streaming driver application.
"""

import sys
from typing import Any, Dict

from pyspark.sql import SparkSession

from src.common.config import load_config
from src.common.logging_utils import get_logger
from src.streaming.anomaly_detector import AnomalyDetectionEngine
from src.streaming.sinks import S3ParquetSink, KafkaAnomalySink, create_console_sink

logger = get_logger(__name__)


def create_spark_session(config: Dict[str, Any]) -> SparkSession:
    """
    Initializes SparkSession configured with S3A Hadoop filesystem and Kafka connector properties.
    """
    spark_conf = config.get("spark", {})
    storage_conf = config.get("storage", {})

    app_name = spark_conf.get("app_name", "ClickstreamAnomalyDetector")
    master = spark_conf.get("master", "local[*]")
    shuffle_partitions = str(spark_conf.get("shuffle_partitions", 4))

    s3_endpoint = storage_conf.get("s3_endpoint", "http://localhost:9000")
    s3_access_key = storage_conf.get("s3_access_key", "minioadmin")
    s3_secret_key = storage_conf.get("s3_secret_key", "minioadmin")

    logger.info("Initializing SparkSession '%s' (Master: %s)...", app_name, master)

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(spark_conf.get("log_level", "WARN"))
    logger.info("SparkSession initialized successfully.")
    return spark


def run_streaming_pipeline() -> None:
    """
    Coordinates reading Kafka stream, running anomaly detection, and starting multi-sink writes.
    """
    config = load_config("spark")
    kafka_conf = config.get("kafka", {})

    bootstrap_servers = kafka_conf.get("bootstrap_servers", "localhost:9092")
    input_topic = kafka_conf.get("input_topic", "events.raw")
    starting_offsets = kafka_conf.get("starting_offsets", "latest")
    fail_on_data_loss = str(kafka_conf.get("fail_on_data_loss", False)).lower()

    spark = create_spark_session(config)

    logger.info("Connecting stream to Kafka brokers at %s on topic '%s'...", bootstrap_servers, input_topic)

    # 1. Ingest Kafka Raw Stream
    raw_stream_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", input_topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", fail_on_data_loss)
        .load()
    )

    # 2. Transformation & Watermarking Engine
    engine = AnomalyDetectionEngine(config)
    parsed_events_df = engine.parse_raw_stream(raw_stream_df)
    anomalies_df = engine.process_all_anomalies(parsed_events_df)

    # 3. Setup Sinks
    s3_sink = S3ParquetSink(config)
    kafka_sink = KafkaAnomalySink(config)

    queries = []

    try:
        # A. Write raw events to S3 Parquet dataset
        logger.info("Launching Raw Events S3 Sink...")
        q_raw_s3 = s3_sink.write_events_stream(parsed_events_df, checkpoint_dir="raw_events_chk")
        queries.append(q_raw_s3)

        # B. Write anomalies to Kafka topic
        logger.info("Launching Anomaly Kafka Sink...")
        q_anom_kafka = kafka_sink.write_anomaly_stream(anomalies_df, checkpoint_dir="anomalies_kafka_chk")
        queries.append(q_anom_kafka)

        # C. Console Monitoring Sink for real-time driver visibility
        logger.info("Launching Anomaly Console Sink...")
        q_console = create_console_sink(anomalies_df, query_name="live_anomalies", output_mode="update")
        queries.append(q_console)

        logger.info("All streaming queries active. Awaiting new events...")
        spark.streams.awaitAnyTermination()

    except KeyboardInterrupt:
        logger.info("Received interrupt. Stopping all active streaming queries...")
        for q in queries:
            if q.isActive:
                q.stop()
        spark.stop()
        logger.info("PySpark pipeline cleanly stopped.")
    except Exception as err:
        logger.error("Fatal streaming pipeline execution failure: %s", err)
        for q in queries:
            if q.isActive:
                q.stop()
        spark.stop()
        sys.exit(1)


if __name__ == "__main__":
    run_streaming_pipeline()
