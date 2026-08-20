#!/usr/bin/env bash
set -e

echo "================================================================"
echo " Starting Clickstream Anomaly Detection Local Environment "
echo "================================================================"

# 1. Start Infrastructure Containers
echo "[1/4] Starting Docker services (Kafka, Zookeeper, MinIO, UI)..."
docker compose -f docker/docker-compose.yml up -d zookeeper kafka kafka-ui minio create-buckets

echo "[2/4] Waiting for Kafka broker to be healthy..."
docker compose -f docker/docker-compose.yml exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 || true

echo "[3/4] Starting Producer and Stream Processing..."
echo "To run components separately, execute:"
echo "  - Producer:   python src/producer/kafka_producer.py"
echo "  - PySpark:    python src/streaming/spark_stream_job.py"
echo "  - Dashboard:  streamlit run src/dashboard/app.py"

echo "[4/4] Starting full stack (Producer + Spark + Dashboard)..."
docker compose -f docker/docker-compose.yml up -d producer spark-streaming dashboard

echo "================================================================"
echo " Services are live:"
echo "   - Kafka UI:   http://localhost:8080"
echo "   - MinIO S3:   http://localhost:9001 (minioadmin / minioadmin)"
echo "   - Dashboard:  http://localhost:8501"
echo "================================================================"
