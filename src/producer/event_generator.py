"""
High-throughput synthetic event generator with configurable anomaly injection scenarios.
"""

from datetime import datetime, timezone, timedelta
import random
from typing import Any, Dict, List, Optional
import uuid

from faker import Faker

fake = Faker()


class EventGenerator:
    """
    Simulates organic clickstream traffic and injects abnormal behavior patterns.
    """

    ACTIONS = [
        ("page_view", 0.55),
        ("product_click", 0.20),
        ("add_to_cart", 0.10),
        ("checkout_start", 0.05),
        ("transaction_success", 0.04),
        ("transaction_failed", 0.01),
        ("login_success", 0.03),
        ("login_failed", 0.015),
        ("password_reset_request", 0.005),
    ]

    URL_PATHS = [
        "/",
        "/products",
        "/products/electronics",
        "/products/clothing",
        "/products/home-appliances",
        "/cart",
        "/checkout",
        "/account/login",
        "/account/security",
        "/search",
    ]

    DEVICE_TYPES = ["desktop", "mobile", "tablet"]

    def __init__(self, total_users: int = 200, concurrent_sessions: int = 40):
        self.user_pool: List[str] = [f"usr_{uuid.uuid4().hex[:8]}" for _ in range(total_users)]
        self.session_pool: Dict[str, Dict[str, Any]] = {}
        self.concurrent_sessions = concurrent_sessions
        self._initialize_sessions()

    def _initialize_sessions(self) -> None:
        """Populates the active session pool with realistic client profiles."""
        for _ in range(self.concurrent_sessions):
            user_id = random.choice(self.user_pool)
            session_id = f"sess_{uuid.uuid4().hex[:12]}"
            self.session_pool[session_id] = {
                "user_id": user_id,
                "ip_address": fake.ipv4_public(),
                "device_type": random.choice(self.DEVICE_TYPES),
                "location_country": fake.country_code(),
                "location_city": fake.city(),
                "user_agent": fake.user_agent(),
            }

    def generate_single_event(
        self,
        action: Optional[str] = None,
        user_id: Optional[str] = None,
        ip_address: Optional[str] = None,
        amount: Optional[float] = None,
        timestamp_override: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Generates an individual event record.
        """
        session_id = random.choice(list(self.session_pool.keys()))
        session_meta = self.session_pool[session_id]

        selected_user = user_id or session_meta["user_id"]
        selected_ip = ip_address or session_meta["ip_address"]

        if not action:
            actions, weights = zip(*self.ACTIONS)
            action = random.choices(actions, weights=weights, k=1)[0]

        event_time = timestamp_override or datetime.now(timezone.utc)

        event_amount = 0.0
        if amount is not None:
            event_amount = float(amount)
        elif action in ["transaction_success", "transaction_failed"]:
            event_amount = round(random.uniform(5.0, 450.0), 2)

        return {
            "event_id": str(uuid.uuid4()),
            "timestamp": event_time.isoformat(),
            "user_id": selected_user,
            "session_id": session_id,
            "ip_address": selected_ip,
            "device_type": session_meta["device_type"],
            "location_country": session_meta["location_country"],
            "location_city": session_meta["location_city"],
            "action": action,
            "url_path": random.choice(self.URL_PATHS),
            "amount": event_amount,
            "user_agent": session_meta["user_agent"],
        }

    def generate_credential_stuffing_burst(
        self, target_user: Optional[str] = None, attacker_ip: Optional[str] = None, attempts: int = 7
    ) -> List[Dict[str, Any]]:
        """
        Simulates brute-force authentication attacks against a single user/IP.
        """
        target = target_user or random.choice(self.user_pool)
        ip = attacker_ip or fake.ipv4_public()
        now = datetime.now(timezone.utc)
        events = []

        for i in range(attempts):
            event_time = now - timedelta(seconds=(attempts - i) * 2)
            event = self.generate_single_event(
                action="login_failed",
                user_id=target,
                ip_address=ip,
                timestamp_override=event_time,
            )
            events.append(event)
        return events

    def generate_transaction_velocity_burst(
        self, target_user: Optional[str] = None, burst_count: int = 4
    ) -> List[Dict[str, Any]]:
        """
        Simulates high-velocity financial transactions in a short time frame.
        """
        target = target_user or random.choice(self.user_pool)
        now = datetime.now(timezone.utc)
        events = []

        for i in range(burst_count):
            event_time = now - timedelta(seconds=(burst_count - i) * 3)
            amount = round(random.uniform(3000.0, 9500.0), 2)
            event = self.generate_single_event(
                action="transaction_success",
                user_id=target,
                amount=amount,
                timestamp_override=event_time,
            )
            events.append(event)
        return events

    def generate_bot_scraping_burst(
        self, attacker_ip: Optional[str] = None, count: int = 120
    ) -> List[Dict[str, Any]]:
        """
        Simulates automated web crawler / scraper rapid page requests.
        """
        ip = attacker_ip or fake.ipv4_public()
        bot_user = f"bot_{uuid.uuid4().hex[:6]}"
        now = datetime.now(timezone.utc)
        events = []

        for i in range(count):
            event_time = now - timedelta(milliseconds=i * 200)
            event = self.generate_single_event(
                action="page_view",
                user_id=bot_user,
                ip_address=ip,
                timestamp_override=event_time,
            )
            events.append(event)
        return events
