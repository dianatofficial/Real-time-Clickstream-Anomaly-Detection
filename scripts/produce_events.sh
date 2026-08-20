#!/usr/bin/env bash
set -e

echo "Starting synthetic clickstream event producer..."
export PYTHONPATH=$(pwd)
python src/producer/kafka_producer.py
