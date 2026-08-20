"""
Unit tests for event generator scenarios and anomaly simulation logic.
"""

from src.producer.event_generator import EventGenerator


def test_generator_single_event_structure():
    """Validates structure and value types of generated individual events."""
    generator = EventGenerator(total_users=20, concurrent_sessions=5)
    event = generator.generate_single_event()

    assert isinstance(event, dict)
    assert "event_id" in event
    assert "timestamp" in event
    assert "user_id" in event
    assert "session_id" in event
    assert "action" in event
    assert "ip_address" in event


def test_credential_stuffing_burst():
    """Checks that credential stuffing burst generates the specified number of failed logins."""
    generator = EventGenerator(total_users=10, concurrent_sessions=2)
    attempts = 8
    target_user = "usr_target_999"
    attacker_ip = "198.51.100.25"

    events = generator.generate_credential_stuffing_burst(
        target_user=target_user, attacker_ip=attacker_ip, attempts=attempts
    )

    assert len(events) == attempts
    for ev in events:
        assert ev["user_id"] == target_user
        assert ev["ip_address"] == attacker_ip
        assert ev["action"] == "login_failed"


def test_transaction_velocity_burst():
    """Validates rapid transaction burst amounts and actions."""
    generator = EventGenerator(total_users=10, concurrent_sessions=2)
    burst_count = 5
    target_user = "usr_whale_1"

    events = generator.generate_transaction_velocity_burst(
        target_user=target_user, burst_count=burst_count
    )

    assert len(events) == burst_count
    for ev in events:
        assert ev["user_id"] == target_user
        assert ev["action"] == "transaction_success"
        assert ev["amount"] >= 2000.0


def test_bot_scraping_burst():
    """Validates high-frequency page view requests generated during bot simulation."""
    generator = EventGenerator(total_users=5, concurrent_sessions=2)
    count = 50
    attacker_ip = "203.0.113.88"

    events = generator.generate_bot_scraping_burst(attacker_ip=attacker_ip, count=count)

    assert len(events) == count
    for ev in events:
        assert ev["ip_address"] == attacker_ip
        assert ev["action"] == "page_view"
