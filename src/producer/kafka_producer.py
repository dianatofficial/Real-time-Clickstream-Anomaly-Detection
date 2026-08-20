"""
Event publisher service streaming clickstream events to Apache Kafka.
"""

import json
import random
import signal
import sys
import time
from typing import Any, Dict, Optional

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from src.common.config import load_config
from src.common.logging_utils import get_logger
from src.producer.event_generator import EventGenerator

logger = get_logger(__name__)


class EventProducerService:
    """
    Manages Kafka producer lifecycle, event batching, and anomaly injection schedules.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or load_config("producer")
        self.kafka_config = self.config.get("kafka", {})
        self.sim_config = self.config.get("simulation", {})
        self.anomaly_config = self.config.get("anomaly_injection", {})

        self.bootstrap_servers = self.kafka_config.get("bootstrap_servers", "localhost:9092")
        self.topic = self.kafka_config.get("topic", "events.raw")
        self.events_per_second = self.sim_config.get("events_per_second", 20)

        self.generator = EventGenerator(
            total_users=self.sim_config.get("total_users", 200),
            concurrent_sessions=self.sim_config.get("concurrent_sessions", 40),
        )

        self.running = False
        self.producer = self._create_producer()

    def _create_producer(self) -> KafkaProducer:
        """Connects to Kafka broker with retry backoff."""
        max_retries = 15
        retry_interval = 2

        for attempt in range(1, max_retries + 1):
            try:
                logger.info(
                    "Connecting to Kafka brokers at %s (Attempt %d/%d)...",
                    self.bootstrap_servers,
                    attempt,
                    max_retries,
                )
                producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    acks=self.kafka_config.get("acks", "all"),
                    retries=self.kafka_config.get("retries", 3),
                    linger_ms=self.kafka_config.get("linger_ms", 10),
                )
                logger.info("Successfully connected to Kafka brokers.")
                return producer
            except NoBrokersAvailable:
                logger.warning(
                    "Kafka brokers unavailable at %s. Retrying in %d seconds...",
                    self.bootstrap_servers,
                    retry_interval,
                )
                time.sleep(retry_interval)

        logger.error("Could not connect to Kafka brokers after %d attempts.", max_retries)
        sys.exit(1)

    def publish_event(self, event: Dict[str, Any]) -> None:
        """Publishes a single event keyed by user_id for partition ordering."""
        key = event.get("user_id", "")
        self.producer.send(self.topic, key=key, value=event)

    def run(self) -> None:
        """Main event generation loop with periodic anomaly injection."""
        self.running = True
        logger.info(
            "Starting event publisher to topic '%s' at ~%d events/sec",
            self.topic,
            self.events_per_second,
        )

        sleep_interval = 1.0 / max(self.events_per_second, 1)
        last_anomaly_check = time.time()
        anomaly_interval = self.anomaly_config.get("injection_interval_seconds", 15)
        is_anomaly_enabled = self.anomaly_config.get("enabled", True)

        total_published = 0

        while self.running:
            try:
                # Organic event stream
                event = self.generator.generate_single_event()
                self.publish_event(event)
                total_published += 1

                # Anomaly injection logic
                current_time = time.time()
                if is_anomaly_enabled and (current_time - last_anomaly_check >= anomaly_interval):
                    last_anomaly_check = current_time
                    self._trigger_anomaly_injection()

                if total_published % 100 == 0:
                    logger.info("Published %d total events to %s", total_published, self.topic)
                    self.producer.flush()

                time.sleep(sleep_interval)

            except KeyboardInterrupt:
                logger.info("Shutdown signal received.")
                break
            except Exception as e:
                logger.error("Error during event publishing: %s", e)
                time.sleep(1)

        self.close()

    def _trigger_anomaly_injection(self) -> None:
        """Selects and injects an anomaly attack pattern."""
        scenarios = self.anomaly_config.get("scenarios", [])
        if not scenarios:
            return

        selected_scenario = random.choice(scenarios)
        scenario_name = selected_scenario.get("name")

        logger.info("Injecting anomaly scenario: %s", scenario_name)

        if scenario_name == "credential_stuffing":
            attempts = selected_scenario.get("failed_attempts", 7)
            events = self.generator.generate_credential_stuffing_burst(attempts=attempts)
            for ev in events:
                self.publish_event(ev)

        elif scenario_name == "transaction_velocity_spike":
            burst_count = selected_scenario.get("burst_count", 4)
            events = self.generator.generate_transaction_velocity_burst(burst_count=burst_count)
            for ev in events:
                self.publish_event(ev)

        elif scenario_name == "bot_click_burst":
            count = selected_scenario.get("click_count", 120)
            events = self.generator.generate_bot_scraping_burst(count=count)
            for ev in events:
                self.publish_event(ev)

        self.producer.flush()

    def close(self) -> None:
        """Flushes remaining messages and closes the producer."""
        logger.info("Flushing and closing Kafka producer...")
        self.running = False
        if self.producer:
            self.producer.flush(timeout=5)
            self.producer.close(timeout=5)
        logger.info("Kafka producer successfully terminated.")


def handle_signals(service: EventProducerService) -> None:
    def _sig_handler(sig, frame):
        logger.info("Intercepted signal %s, initiating graceful shutdown...", sig)
        service.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)


if __name__ == "__main__":
    producer_service = EventProducerService()
    handle_signals(producer_service)
    producer_service.run()
