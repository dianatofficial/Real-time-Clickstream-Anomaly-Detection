"""
Common utilities, schema definitions, and configuration helpers.
"""

__all__ = ["load_config", "get_logger", "CLICKSTREAM_SCHEMA", "ANOMALY_SCHEMA"]


def __getattr__(name):
    if name == "load_config":
        from src.common.config import load_config
        return load_config
    if name == "get_logger":
        from src.common.logging_utils import get_logger
        return get_logger
    if name in ("CLICKSTREAM_SCHEMA", "ANOMALY_SCHEMA", "ClickstreamEvent"):
        from src.common.schemas import CLICKSTREAM_SCHEMA, ANOMALY_SCHEMA, ClickstreamEvent
        if name == "CLICKSTREAM_SCHEMA":
            return CLICKSTREAM_SCHEMA
        if name == "ANOMALY_SCHEMA":
            return ANOMALY_SCHEMA
        if name == "ClickstreamEvent":
            return ClickstreamEvent
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
