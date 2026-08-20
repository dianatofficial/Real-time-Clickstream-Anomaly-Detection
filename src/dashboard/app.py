"""
Real-time Clickstream & Anomaly Monitoring Dashboard (Streamlit).
Supports both Distributed Kafka Mode (Docker/Production) and Interactive Live Simulation Mode (Streamlit Cloud).
"""

import os
import sys
from pathlib import Path

# Add project root directory to sys.path for Streamlit Cloud execution
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from collections import deque
from datetime import datetime, timezone, timedelta
import json
import random
import time
from typing import Any, Dict, List, Optional
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Import event generator for live cloud demo mode
from src.producer.event_generator import EventGenerator

# Set Page Config
st.set_page_config(
    page_title="StreamWatch | Real-Time Anomaly Detection",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom Styling
st.markdown(
    """
    <style>
    .main {
        background-color: #0e1117;
    }
    .metric-container {
        background-color: #1e222d;
        border: 1px solid #2d3139;
        border-radius: 8px;
        padding: 14px;
        margin-bottom: 10px;
    }
    .status-badge-cloud {
        background-color: #007acc;
        color: white;
        padding: 4px 10px;
        border-radius: 12px;
        font-size: 13px;
        font-weight: 600;
    }
    .status-badge-kafka {
        background-color: #2e7d32;
        color: white;
        padding: 4px 10px;
        border-radius: 12px;
        font-size: 13px;
        font-weight: 600;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Environment & Connection Parameters
KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ANOMALY_TOPIC = os.getenv("ANOMALY_TOPIC", "events.anomalies")
RAW_TOPIC = os.getenv("RAW_TOPIC", "events.raw")

# Session State Buffers
if "anomaly_records" not in st.session_state:
    st.session_state.anomaly_records = deque(maxlen=200)

if "raw_events_buffer" not in st.session_state:
    st.session_state.raw_events_buffer = deque(maxlen=1000)

if "generator" not in st.session_state:
    st.session_state.generator = EventGenerator(total_users=150, concurrent_sessions=30)

if "total_events_processed" not in st.session_state:
    st.session_state.total_events_processed = 0

if "total_anomalies_detected" not in st.session_state:
    st.session_state.total_anomalies_detected = 0

if "sim_active" not in st.session_state:
    st.session_state.sim_active = True


def try_kafka_consumer() -> Optional[Any]:
    """Attempts to connect to Kafka broker. Returns None if unreachable."""
    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            ANOMALY_TOPIC,
            bootstrap_servers=KAFKA_BROKERS,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="streamlit_cloud_consumer",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            consumer_timeout_ms=300,
        )
        return consumer
    except Exception:
        return None


def run_sliding_window_detection(events: List[Dict[str, Any]], window_minutes: int = 5) -> List[Dict[str, Any]]:
    """
    In-memory stateful sliding-window anomaly evaluation engine for cloud demo environments.
    Applies the exact same threshold logic as the PySpark Structured Streaming job.
    """
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=window_minutes)

    # Filter events inside the active sliding window
    window_events = []
    for ev in events:
        try:
            ev_time = datetime.fromisoformat(ev["timestamp"])
            if ev_time >= cutoff:
                window_events.append(ev)
        except Exception:
            continue

    detected_anomalies = []

    # 1. Credential Stuffing Rule (>= 5 failed logins per IP/User in window)
    failed_logins = {}
    for ev in window_events:
        if ev.get("action") == "login_failed":
            key = (ev.get("ip_address"), ev.get("user_id"))
            failed_logins[key] = failed_logins.get(key, 0) + 1

    for (ip, user), count in failed_logins.items():
        if count >= 5:
            detected_anomalies.append({
                "window_start": cutoff.strftime("%H:%M:%S"),
                "window_end": now.strftime("%H:%M:%S"),
                "user_id": user,
                "ip_address": ip,
                "anomaly_type": "CREDENTIAL_STUFFING_BRUTE_FORCE",
                "severity": "HIGH",
                "event_count": count,
                "total_amount": 0.0,
                "details": f"{count} failed login attempts in 5-minute sliding window",
                "detection_timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
            })

    # 2. Transaction Velocity Spike Rule (>= 3 txns or amount >= $10,000)
    user_txns = {}
    for ev in window_events:
        if ev.get("action") == "transaction_success":
            key = (ev.get("user_id"), ev.get("ip_address"))
            stats = user_txns.get(key, {"count": 0, "amount": 0.0})
            stats["count"] += 1
            stats["amount"] += ev.get("amount", 0.0)
            user_txns[key] = stats

    for (user, ip), stats in user_txns.items():
        if stats["count"] >= 3 or stats["amount"] >= 10000.0:
            detected_anomalies.append({
                "window_start": cutoff.strftime("%H:%M:%S"),
                "window_end": now.strftime("%H:%M:%S"),
                "user_id": user,
                "ip_address": ip,
                "anomaly_type": "TRANSACTION_VELOCITY_SPIKE",
                "severity": "CRITICAL",
                "event_count": stats["count"],
                "total_amount": round(stats["amount"], 2),
                "details": f"Rapid sequence: {stats['count']} transactions totaling ${stats['amount']:,.2f}",
                "detection_timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
            })

    # 3. Bot Scraper Burst Rule (>= 80 clicks in window)
    ip_clicks = {}
    for ev in window_events:
        if ev.get("action") in ["page_view", "product_click"]:
            ip = ev.get("ip_address")
            ip_clicks[ip] = ip_clicks.get(ip, 0) + 1

    for ip, count in ip_clicks.items():
        if count >= 80:
            detected_anomalies.append({
                "window_start": cutoff.strftime("%H:%M:%S"),
                "window_end": now.strftime("%H:%M:%S"),
                "user_id": "automated_client",
                "ip_address": ip,
                "anomaly_type": "BOT_SCRAPING_BURST",
                "severity": "MEDIUM",
                "event_count": count,
                "total_amount": 0.0,
                "details": f"Volumetric spike: {count} requests within window",
                "detection_timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
            })

    return detected_anomalies


# Check if live Kafka connection is available
kafka_conn = try_kafka_consumer()
is_kafka_mode = kafka_conn is not None

# Sidebar Controls
with st.sidebar:
    st.title("⚡ StreamWatch Engine")
    st.markdown("**Real-Time Stream Processing Telemetry**")
    st.divider()

    if is_kafka_mode:
        st.markdown('<span class="status-badge-kafka">● Kafka Broker Connected</span>', unsafe_allow_html=True)
        st.caption(f"Broker: `{KAFKA_BROKERS}` | Topic: `{ANOMALY_TOPIC}`")
    else:
        st.markdown('<span class="status-badge-cloud">● Cloud Simulation Mode</span>', unsafe_allow_html=True)
        st.caption("Running in-memory streaming engine directly on Streamlit Cloud.")

    st.divider()
    st.subheader("Stream Simulator Controls")

    col_btn1, col_btn2 = st.columns(2)
    with col_btn1:
        if st.button("▶ Start Stream" if not st.session_state.sim_active else "⏸ Pause"):
            st.session_state.sim_active = not st.session_state.sim_active
            st.rerun()

    with col_btn2:
        if st.button("🧹 Clear Logs"):
            st.session_state.anomaly_records.clear()
            st.session_state.raw_events_buffer.clear()
            st.session_state.total_events_processed = 0
            st.session_state.total_anomalies_detected = 0
            st.rerun()

    event_batch_rate = st.slider("Events Generated per Tick", 5, 50, 15)
    refresh_rate = st.slider("Refresh Frequency (sec)", 1, 5, 2)

    st.divider()
    st.subheader("⚡ Live Threat Injection")
    st.caption("Click to trigger instant security anomaly scenarios into the stream:")

    if st.button("🚨 Inject Credential Stuffing"):
        burst = st.session_state.generator.generate_credential_stuffing_burst(attempts=6)
        st.session_state.raw_events_buffer.extend(burst)
        st.session_state.total_events_processed += len(burst)
        st.success("Injected 6 failed login attempts!")

    if st.button("💳 Inject Transaction Spike"):
        burst = st.session_state.generator.generate_transaction_velocity_burst(burst_count=4)
        st.session_state.raw_events_buffer.extend(burst)
        st.session_state.total_events_processed += len(burst)
        st.success("Injected 4 rapid high-value transactions!")

    if st.button("🤖 Inject Bot Scraper Burst"):
        burst = st.session_state.generator.generate_bot_scraping_burst(count=85)
        st.session_state.raw_events_buffer.extend(burst)
        st.session_state.total_events_processed += len(burst)
        st.success("Injected 85 rapid page request events!")

    st.divider()
    st.markdown(
        """
        **Pipeline Architecture:**
        - **Pub/Sub**: Apache Kafka (`events.raw`)
        - **Processing**: PySpark Structured Streaming
        - **Windowing**: 5-min Sliding Window / 1-min Slide
        - **Late Data**: 10-min Watermark tolerance
        - **Lakehouse**: S3 / MinIO Parquet Sink
        """
    )


# Process Streaming Data
if is_kafka_mode and kafka_conn:
    try:
        records = kafka_conn.poll(timeout_ms=300)
        for _, rec_list in records.items():
            for rec in rec_list:
                st.session_state.anomaly_records.appendleft(rec.value)
                st.session_state.total_anomalies_detected += 1
    except Exception:
        pass
    finally:
        kafka_conn.close()
else:
    # Cloud In-Memory Simulation
    if st.session_state.sim_active:
        # Generate normal background clickstream traffic
        for _ in range(event_batch_rate):
            ev = st.session_state.generator.generate_single_event()
            st.session_state.raw_events_buffer.append(ev)
            st.session_state.total_events_processed += 1

        # Evaluate sliding window anomaly detection rules
        new_anomalies = run_sliding_window_detection(list(st.session_state.raw_events_buffer), window_minutes=5)
        for anom in new_anomalies:
            # Avoid duplicate logs for identical window & target
            exists = any(
                a.get("user_id") == anom["user_id"]
                and a.get("anomaly_type") == anom["anomaly_type"]
                and a.get("window_end") == anom["window_end"]
                for a in st.session_state.anomaly_records
            )
            if not exists:
                st.session_state.anomaly_records.appendleft(anom)
                st.session_state.total_anomalies_detected += 1


# Header
st.title("Real-Time Clickstream & Anomaly Detection Pipeline")
st.markdown("Live distributed stream processing monitoring for security threats, fraud, and volumetric anomalies.")

# KPI Metric Cards
col1, col2, col3, col4 = st.columns(4)

total_anom = len(st.session_state.anomaly_records)
crit_count = sum(1 for a in st.session_state.anomaly_records if a.get("severity") == "CRITICAL")
high_count = sum(1 for a in st.session_state.anomaly_records if a.get("severity") == "HIGH")
unique_ips = len(set(a.get("ip_address", "") for a in st.session_state.anomaly_records if a.get("ip_address")))

with col1:
    st.metric(label="Total Ingested Events", value=f"{st.session_state.total_events_processed:,}", delta="Live Ingestion")
with col2:
    st.metric(label="Total Anomalies Flagged", value=st.session_state.total_anomalies_detected, delta=f"{total_anom} in buffer")
with col3:
    st.metric(label="Critical Threats", value=crit_count, delta_color="inverse")
with col4:
    st.metric(label="Flagged Offender IPs", value=unique_ips)

st.divider()

# Charts Section
df_anom = pd.DataFrame(list(st.session_state.anomaly_records))

c1, c2 = st.columns([5, 5])

with c1:
    st.subheader("Anomaly Distribution by Category")
    if not df_anom.empty and "anomaly_type" in df_anom.columns:
        type_counts = df_anom["anomaly_type"].value_counts().reset_index()
        type_counts.columns = ["Anomaly Type", "Count"]

        color_map = {
            "TRANSACTION_VELOCITY_SPIKE": "#ff4b4b",
            "CREDENTIAL_STUFFING_BRUTE_FORCE": "#ffa421",
            "BOT_SCRAPING_BURST": "#2196f3",
        }

        fig_pie = px.pie(
            type_counts,
            values="Count",
            names="Anomaly Type",
            hole=0.45,
            color="Anomaly Type",
            color_discrete_map=color_map,
        )
        fig_pie.update_layout(margin=dict(t=20, b=20, l=20, r=20), height=320)
        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.info("Generating and analyzing clickstream stream... Use the sidebar buttons to inject attacks.")

with c2:
    st.subheader("Top Flagged IPs by Violation Count")
    if not df_anom.empty and "ip_address" in df_anom.columns:
        ip_counts = df_anom["ip_address"].value_counts().head(6).reset_index()
        ip_counts.columns = ["IP Address", "Violations"]

        fig_bar = px.bar(
            ip_counts,
            x="Violations",
            y="IP Address",
            orientation="h",
            color="Violations",
            color_continuous_scale="Reds",
        )
        fig_bar.update_layout(margin=dict(t=20, b=20, l=20, r=20), height=320)
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.info("No malicious IP activity flagged yet.")

st.divider()

# Live Table Feed
st.subheader("Live Flagged Threat Log")
if not df_anom.empty:
    cols = ["detection_timestamp", "anomaly_type", "severity", "user_id", "ip_address", "event_count", "total_amount", "details"]
    valid_cols = [c for c in cols if c in df_anom.columns]
    st.dataframe(
        df_anom[valid_cols].head(30),
        use_container_width=True,
        hide_index=True,
    )
else:
    st.write("Waiting for anomaly events. The 5-minute sliding window state store is aggregating background events.")

# Real-Time Auto Refresh
if st.session_state.sim_active:
    time.sleep(refresh_rate)
    st.rerun()
