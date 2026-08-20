"""
Data schemas and type definitions for event serialization and stream parsing.
"""

from typing import Optional, Any
from pydantic import BaseModel, Field


class ClickstreamEvent(BaseModel):
    """
    Pydantic validation model for raw user interaction clickstream events.
    """
    event_id: str = Field(..., description="Unique event UUID")
    timestamp: str = Field(..., description="ISO 8601 formatted event timestamp")
    user_id: str = Field(..., description="Identifier of the user account")
    session_id: str = Field(..., description="User browsing or session token")
    ip_address: str = Field(..., description="Origin IPv4 or IPv6 client address")
    device_type: str = Field(..., description="Client device category (desktop, mobile, tablet)")
    location_country: str = Field(..., description="Geographic country code")
    location_city: str = Field(..., description="Geographic city name")
    action: str = Field(..., description="Action type (login_success, login_failed, page_view, etc.)")
    url_path: str = Field(..., description="Target application path or route")
    amount: Optional[float] = Field(default=0.0, description="Monetary transaction amount if applicable")
    user_agent: str = Field(..., description="Browser or client user agent string")


def get_clickstream_spark_schema() -> Any:
    """
    Constructs and returns PySpark StructType schema for raw clickstream events.
    """
    from pyspark.sql.types import (
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    return StructType([
        StructField("event_id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("ip_address", StringType(), False),
        StructField("device_type", StringType(), True),
        StructField("location_country", StringType(), True),
        StructField("location_city", StringType(), True),
        StructField("action", StringType(), False),
        StructField("url_path", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("user_agent", StringType(), True),
    ])


def get_anomaly_spark_schema() -> Any:
    """
    Constructs and returns PySpark StructType schema for detected anomaly records.
    """
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType([
        StructField("window_start", TimestampType(), False),
        StructField("window_end", TimestampType(), False),
        StructField("user_id", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("anomaly_type", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("event_count", IntegerType(), False),
        StructField("total_amount", DoubleType(), True),
        StructField("details", StringType(), True),
        StructField("detection_timestamp", TimestampType(), False),
    ])


def __getattr__(name: str) -> Any:
    if name == "CLICKSTREAM_SCHEMA":
        return get_clickstream_spark_schema()
    if name == "ANOMALY_SCHEMA":
        return get_anomaly_spark_schema()
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
