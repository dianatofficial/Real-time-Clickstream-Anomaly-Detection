"""
Real-time Clickstream & Anomaly Monitoring Dashboard (Streamlit).
"""

import json
import os
import time
from collections import deque
from datetime import datetime, timezone
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# Set Page Config
st.set_page_config(
    page_title="StreamWatch | Real-Time Anomaly Detection",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for dark telemetry styling
st.markdown(
    """
    <style>
    .main {
        background-color: #0e1117;
    }
    .metric-card {
        background-color: #1e222d;
        border: 1px solid #2d3139;
        border-radius: 8px;
        padding: 16px;
        margin-bottom: 12px;
    }
    .badge-critical {
        background-color: #ff4b4b;
        color: white;
        padding: 4px 8px;
        border-radius: 4px;
        font-weight: bold;
    }
    .badge-high {
        background-color: #ffa421;
        color: white;
        padding: 4px 8px;
        border-radius: 4px;
        font-weight: bold;
    }
    .badge-medium {
        background-color: #2196f3;
        color: white;
        padding: 4px 8px;
        border-radius: 4px;
        font-weight: bold;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Application Parameters
KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ANOMALY_TOPIC = os.getenv("ANOMALY_TOPIC", "events.anomalies")
RAW_TOPIC = os.getenv("RAW_TOPIC", "events.raw")

# Session state buffers
if "anomaly_records" not in st.session_state:
    st.session_state.anomaly_records = deque(maxlen=200)

if "throughput_history" not in st.session_state:
    st.session_state.throughput_history = deque(maxlen=60)

if "total_events_processed" not in st.session_state:
    st.session_state.total_events_processed = 0

if "total_anomalies_detected" not in st.session_state:
    st.session_state.total_anomalies_detected = 0


def get_kafka_consumer(topic: str, group_id: str = "dashboard_consumer"):
    """Creates a non-blocking Kafka consumer connection."""
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BROKERS,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            consumer_timeout_ms=500,
        )
        return consumer
    except NoBrokersAvailable:
        return None
    except Exception:
        return None


# Sidebar Configuration & Controls
with st.sidebar:
    st.title("⚡ StreamWatch Engine")
    st.markdown("**Distributed Stream Processing Telemetry**")
    st.divider()

    st.markdown("### Broker Connection")
    st.text(f"Brokers: {KAFKA_BROKERS}")
    st.text(f"Raw Topic: {RAW_TOPIC}")
    st.text(f"Anomaly Topic: {ANOMALY_TOPIC}")

    st.divider()
    auto_refresh = st.checkbox("Auto Refresh Stream", value=True)
    refresh_rate = st.slider("Polling Frequency (seconds)", 1, 10, 2)

    if st.button("Clear Buffer Records"):
        st.session_state.anomaly_records.clear()
        st.session_state.throughput_history.clear()
        st.session_state.total_events_processed = 0
        st.session_state.total_anomalies_detected = 0
        st.rerun()

    st.divider()
    st.markdown(
        """
        **Pipeline Stack:**
        - Apache Kafka (Pub/Sub)
        - PySpark Structured Streaming
        - 5-Min Sliding Window / 1-Min Slide
        - 10-Min Watermark Handling
        - MinIO S3 Parquet Lakehouse
        """
    )

# Header Section
st.title("Real-Time Clickstream Anomaly Detection Dashboard")
st.markdown("Live monitoring of high-throughput interaction events, security threats, and fraud patterns.")

# Consume recent messages from Kafka
anomaly_consumer = get_kafka_consumer(ANOMALY_TOPIC, group_id="dashboard_anomaly_group")
if anomaly_consumer:
    try:
        messages = anomaly_consumer.poll(timeout_ms=400)
        for _, records in messages.items():
            for record in records:
                anomaly_data = record.value
                st.session_state.anomaly_records.appendleft(anomaly_data)
                st.session_state.total_anomalies_detected += 1
    except Exception:
        pass
    finally:
        anomaly_consumer.close()

# KPI Metrics Bar
col1, col2, col3, col4 = st.columns(4)

total_anomalies = len(st.session_state.anomaly_records)
critical_count = sum(1 for a in st.session_state.anomaly_records if a.get("severity") == "CRITICAL")
high_count = sum(1 for a in st.session_state.anomaly_records if a.get("severity") == "HIGH")

with col1:
    st.metric(label="Total Anomalies Flagged", value=st.session_state.total_anomalies_detected, delta=f"+{total_anomalies} in buffer")
with col2:
    st.metric(label="Critical Severity Threats", value=critical_count, delta_color="inverse")
with col3:
    st.metric(label="High Severity Threats", value=high_count, delta_color="inverse")
with col4:
    active_sources = len(set(a.get("ip_address", "") for a in st.session_state.anomaly_records))
    st.metric(label="Unique Offending IPs", value=active_sources)

st.divider()

# Visual Analytics Section
chart_col1, chart_col2 = st.columns([6, 4])

if st.session_state.anomaly_records:
    df_anomalies = pd.DataFrame(list(st.session_state.anomaly_records))
else:
    df_anomalies = pd.DataFrame(columns=[
        "detection_timestamp", "anomaly_type", "severity", "user_id", "ip_address", "event_count", "total_amount", "details"
    ])

with chart_col1:
    st.subheader("Anomaly Distribution by Type")
    if not df_anomalies.empty and "anomaly_type" in df_anomalies.columns:
        type_counts = df_anomalies["anomaly_type"].value_counts().reset_index()
        type_counts.columns = ["Anomaly Type", "Count"]

        fig_pie = px.pie(
            type_counts,
            values="Count",
            names="Anomaly Type",
            hole=0.45,
            color_discrete_sequence=["#ff4b4b", "#ffa421", "#2196f3", "#9c27b0"],
        )
        fig_pie.update_layout(margin=dict(t=20, b=20, l=20, r=20), height=320)
        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.info("Awaiting live anomaly telemetry from PySpark streaming job...")

with chart_col2:
    st.subheader("Top Offending Source IPs")
    if not df_anomalies.empty and "ip_address" in df_anomalies.columns:
        ip_counts = df_anomalies["ip_address"].value_counts().head(5).reset_index()
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
        st.info("No suspicious IPs detected yet.")

# Live Event Stream Table
st.subheader("Live Flagged Threats & Fraud Telemetry")
if not df_anomalies.empty:
    display_cols = ["detection_timestamp", "anomaly_type", "severity", "user_id", "ip_address", "event_count", "total_amount", "details"]
    available_cols = [c for c in display_cols if c in df_anomalies.columns]
    st.dataframe(
        df_anomalies[available_cols].head(25),
        use_container_width=True,
        hide_index=True,
    )
else:
    st.write("No anomaly alerts currently recorded. Producer events are either healthy or the stream processor is accumulating the initial sliding window.")

# Auto-rerun loop
if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()
