"""
PySpark Structured Streaming transformations, sliding window aggregations, and anomaly rule definitions.
"""

from typing import Any, Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    current_timestamp,
    from_json,
    lit,
    sum as _sum,
    to_timestamp,
    when,
    window,
)

from src.common.logging_utils import get_logger
from src.common.schemas import CLICKSTREAM_SCHEMA

logger = get_logger(__name__)


class AnomalyDetectionEngine:
    """
    Applies sliding window stateful aggregations and multi-pattern anomaly detection over clickstreams.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.streaming_config = config.get("streaming", {})
        self.rules_config = self.streaming_config.get("rules", {})

        self.watermark_duration = self.streaming_config.get("watermark_duration", "10 minutes")
        self.window_duration = self.streaming_config.get("window_duration", "5 minutes")
        self.slide_duration = self.streaming_config.get("slide_duration", "1 minute")

        # Thresholds
        self.max_failed_logins = self.rules_config.get("max_failed_logins", 5)
        self.max_transaction_amount = self.rules_config.get("max_transaction_amount", 10000.0)
        self.max_transaction_velocity = self.rules_config.get("max_transaction_velocity", 3)
        self.max_clicks_per_window = self.rules_config.get("max_clicks_per_window", 100)

    def parse_raw_stream(self, raw_kafka_df: DataFrame) -> DataFrame:
        """
        Extracts and parses JSON string payloads from Kafka 'value' column into structured columns.
        Applies watermarking on the parsed event timestamp.
        """
        parsed_df = (
            raw_kafka_df.selectExpr("CAST(value AS STRING) as json_payload")
            .select(from_json(col("json_payload"), CLICKSTREAM_SCHEMA).alias("data"))
            .select("data.*")
            .withColumn("event_time", to_timestamp(col("timestamp")))
            .filter(col("event_time").isNotNull())
            .withWatermark("event_time", self.watermark_duration)
        )
        return parsed_df

    def detect_login_brute_force(self, parsed_df: DataFrame) -> DataFrame:
        """
        Detects credential stuffing and brute-force attacks:
        Flags IP and user targets with >= max_failed_logins within sliding window.
        """
        aggregated_df = (
            parsed_df.filter(col("action") == "login_failed")
            .groupBy(
                window(col("event_time"), self.window_duration, self.slide_duration),
                col("ip_address"),
                col("user_id"),
            )
            .agg(count(lit(1)).alias("failed_login_count"))
            .filter(col("failed_login_count") >= self.max_failed_logins)
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("user_id"),
                col("ip_address"),
                lit("CREDENTIAL_STUFFING_BRUTE_FORCE").alias("anomaly_type"),
                lit("HIGH").alias("severity"),
                col("failed_login_count").cast("int").alias("event_count"),
                lit(0.0).cast("double").alias("total_amount"),
                lit("Multiple failed login attempts exceeding security threshold").alias("details"),
                current_timestamp().alias("detection_timestamp"),
            )
        )
        return aggregated_df

    def detect_transaction_fraud(self, parsed_df: DataFrame) -> DataFrame:
        """
        Detects transaction velocity spikes and high single/cumulative transfer amounts within sliding window.
        """
        aggregated_df = (
            parsed_df.filter(col("action") == "transaction_success")
            .groupBy(
                window(col("event_time"), self.window_duration, self.slide_duration),
                col("user_id"),
                col("ip_address"),
            )
            .agg(
                count(lit(1)).alias("txn_count"),
                _sum(col("amount")).alias("cumulative_amount"),
            )
            .filter(
                (col("txn_count") >= self.max_transaction_velocity)
                | (col("cumulative_amount") >= self.max_transaction_amount)
            )
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("user_id"),
                col("ip_address"),
                lit("TRANSACTION_VELOCITY_SPIKE").alias("anomaly_type"),
                lit("CRITICAL").alias("severity"),
                col("txn_count").cast("int").alias("event_count"),
                col("cumulative_amount").cast("double").alias("total_amount"),
                lit("High volume or high cumulative transaction amount in window").alias("details"),
                current_timestamp().alias("detection_timestamp"),
            )
        )
        return aggregated_df

    def detect_bot_click_burst(self, parsed_df: DataFrame) -> DataFrame:
        """
        Detects automated scraping or volumetric bot bursts based on rapid page requests.
        """
        aggregated_df = (
            parsed_df.filter(col("action").isin("page_view", "product_click"))
            .groupBy(
                window(col("event_time"), self.window_duration, self.slide_duration),
                col("ip_address"),
                col("user_id"),
            )
            .agg(count(lit(1)).alias("click_count"))
            .filter(col("click_count") >= self.max_clicks_per_window)
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("user_id"),
                col("ip_address"),
                lit("BOT_SCRAPING_BURST").alias("anomaly_type"),
                lit("MEDIUM").alias("severity"),
                col("click_count").cast("int").alias("event_count"),
                lit(0.0).cast("double").alias("total_amount"),
                lit("Rapid clickstream request frequency matching bot signature").alias("details"),
                current_timestamp().alias("detection_timestamp"),
            )
        )
        return aggregated_df

    def process_all_anomalies(self, parsed_df: DataFrame) -> DataFrame:
        """
        Unions all detected anomaly event streams into a unified anomaly DataFrame.
        """
        brute_force_df = self.detect_login_brute_force(parsed_df)
        fraud_df = self.detect_transaction_fraud(parsed_df)
        bot_df = self.detect_bot_click_burst(parsed_df)

        unified_anomalies = brute_force_df.unionByName(fraud_df).unionByName(bot_df)
        return unified_anomalies
