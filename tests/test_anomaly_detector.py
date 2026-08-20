from datetime import datetime, timezone, timedelta
import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.streaming.anomaly_detector import AnomalyDetectionEngine


@pytest.fixture(scope="session")
def spark_session():
    """Initializes an in-memory SparkSession for testing."""
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("AnomalyDetectorTests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def sample_config():
    """Returns test engine configuration with low thresholds."""
    return {
        "streaming": {
            "watermark_duration": "10 minutes",
            "window_duration": "5 minutes",
            "slide_duration": "1 minute",
            "rules": {
                "max_failed_logins": 3,
                "max_transaction_amount": 5000.0,
                "max_transaction_velocity": 2,
                "max_clicks_per_window": 10,
            },
        }
    }


def test_detect_login_brute_force_logic(spark_session, sample_config):
    """Verifies that brute-force failed logins trigger anomaly detection."""
    engine = AnomalyDetectionEngine(sample_config)

    now = datetime(2026, 8, 20, 10, 0, 0, tzinfo=timezone.utc)
    schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("ip_address", StringType(), False),
        StructField("action", StringType(), False),
        StructField("event_time", TimestampType(), False),
    ])

    # 4 failed logins for usr_victim
    data = [
        ("usr_victim", "192.168.1.50", "login_failed", now + timedelta(seconds=10)),
        ("usr_victim", "192.168.1.50", "login_failed", now + timedelta(seconds=20)),
        ("usr_victim", "192.168.1.50", "login_failed", now + timedelta(seconds=30)),
        ("usr_victim", "192.168.1.50", "login_failed", now + timedelta(seconds=40)),
        ("usr_normal", "192.168.1.51", "login_failed", now + timedelta(seconds=10)),  # 1 failed login
    ]

    df = spark_session.createDataFrame(data, schema)
    anomalies_df = engine.detect_login_brute_force(df)
    results = anomalies_df.collect()

    assert len(results) > 0
    victim_records = [r for r in results if r["user_id"] == "usr_victim"]
    assert len(victim_records) > 0
    assert victim_records[0]["anomaly_type"] == "CREDENTIAL_STUFFING_BRUTE_FORCE"
    assert victim_records[0]["severity"] == "HIGH"


def test_detect_transaction_velocity_logic(spark_session, sample_config):
    """Verifies that high-velocity transactions trigger financial fraud anomaly."""
    engine = AnomalyDetectionEngine(sample_config)

    now = datetime(2026, 8, 20, 10, 0, 0, tzinfo=timezone.utc)
    schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("ip_address", StringType(), False),
        StructField("action", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("event_time", TimestampType(), False),
    ])

    # 3 rapid transactions exceeding threshold of 2
    data = [
        ("usr_fraud", "10.0.0.1", "transaction_success", 2000.0, now + timedelta(seconds=5)),
        ("usr_fraud", "10.0.0.1", "transaction_success", 3500.0, now + timedelta(seconds=15)),
        ("usr_fraud", "10.0.0.1", "transaction_success", 1000.0, now + timedelta(seconds=25)),
    ]

    df = spark_session.createDataFrame(data, schema)
    anomalies_df = engine.detect_transaction_fraud(df)
    results = anomalies_df.collect()

    assert len(results) > 0
    fraud_record = results[0]
    assert fraud_record["user_id"] == "usr_fraud"
    assert fraud_record["anomaly_type"] == "TRANSACTION_VELOCITY_SPIKE"
    assert fraud_record["severity"] == "CRITICAL"
