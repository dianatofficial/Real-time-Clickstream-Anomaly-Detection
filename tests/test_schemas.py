"""
Unit tests for data schema validation and serialization models.
"""

from datetime import datetime, timezone
import pytest
from pydantic import ValidationError

from src.common.schemas import ClickstreamEvent


def test_clickstream_event_valid_model():
    """Verifies that a well-formed dictionary satisfies ClickstreamEvent Pydantic schema."""
    valid_data = {
        "event_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": "usr_test123",
        "session_id": "sess_abc456",
        "ip_address": "192.168.1.100",
        "device_type": "desktop",
        "location_country": "US",
        "location_city": "San Francisco",
        "action": "page_view",
        "url_path": "/products",
        "amount": 0.0,
        "user_agent": "Mozilla/5.0",
    }
    event = ClickstreamEvent(**valid_data)
    assert event.user_id == "usr_test123"
    assert event.action == "page_view"
    assert event.amount == 0.0


def test_clickstream_event_missing_required_field():
    """Ensures ValidationError is raised when required schema fields are absent."""
    invalid_data = {
        "event_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        # missing user_id
        "session_id": "sess_abc456",
        "ip_address": "192.168.1.100",
    }
    with pytest.raises(ValidationError):
        ClickstreamEvent(**invalid_data)


def test_pyspark_schema_definitions():
    """Verifies PySpark schema field counts and data type assignments."""
    pytest.importorskip("pyspark")
    from src.common.schemas import CLICKSTREAM_SCHEMA, ANOMALY_SCHEMA

    assert len(CLICKSTREAM_SCHEMA.fields) == 12
    field_names = [f.name for f in CLICKSTREAM_SCHEMA.fields]
    assert "event_id" in field_names
    assert "timestamp" in field_names
    assert "user_id" in field_names
    assert "action" in field_names
    assert "amount" in field_names

    assert len(ANOMALY_SCHEMA.fields) == 10
    anomaly_fields = [f.name for f in ANOMALY_SCHEMA.fields]
    assert "window_start" in anomaly_fields
    assert "anomaly_type" in anomaly_fields
    assert "severity" in anomaly_fields
