.PHONY: help up down restart logs build producer stream dashboard test lint format clean

help:
	@echo "Available commands:"
	@echo "  make up          Start all infrastructure services in background"
	@echo "  make down        Stop and remove containers and networks"
	@echo "  make restart     Restart infrastructure services"
	@echo "  make logs        Tail logs from all services"
	@echo "  make build       Build Docker images"
	@echo "  make producer    Start synthetic event producer"
	@echo "  make stream      Submit PySpark Structured Streaming job"
	@echo "  make dashboard   Launch Streamlit real-time monitoring dashboard"
	@echo "  make test        Run test suite with coverage report"
	@echo "  make lint        Run flake8 and code linters"
	@echo "  make format      Format code using black and isort"
	@echo "  make clean       Remove temporary files and caches"

up:
	docker compose -f docker/docker-compose.yml up -d

down:
	docker compose -f docker/docker-compose.yml down -v

restart: down up

logs:
	docker compose -f docker/docker-compose.yml logs -f

build:
	docker compose -f docker/docker-compose.yml build

producer:
	python src/producer/kafka_producer.py

stream:
	spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
		src/streaming/spark_stream_job.py

dashboard:
	streamlit run src/dashboard/app.py

test:
	pytest -v tests/

lint:
	flake8 src tests
	black --check src tests
	isort --check-only src tests

format:
	black src tests
	isort src tests

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".coverage" -delete
	find . -type d -name "htmlcov" -exec rm -rf {} +
