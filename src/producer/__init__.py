"""
Synthetic clickstream generator and Kafka event publishing modules.
"""

__all__ = ["EventGenerator", "EventProducerService"]


def __getattr__(name):
    if name == "EventGenerator":
        from src.producer.event_generator import EventGenerator
        return EventGenerator
    if name == "EventProducerService":
        from src.producer.kafka_producer import EventProducerService
        return EventProducerService
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
