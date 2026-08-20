"""
PySpark Structured Streaming pipeline, anomaly detection logic, and sink connectors.
"""

__all__ = ["AnomalyDetectionEngine", "S3ParquetSink", "KafkaAnomalySink"]


def __getattr__(name):
    if name == "AnomalyDetectionEngine":
        from src.streaming.anomaly_detector import AnomalyDetectionEngine
        return AnomalyDetectionEngine
    if name == "S3ParquetSink":
        from src.streaming.sinks import S3ParquetSink
        return S3ParquetSink
    if name == "KafkaAnomalySink":
        from src.streaming.sinks import KafkaAnomalySink
        return KafkaAnomalySink
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
