"""
YAML and Environment Variable configuration management module.
"""

import os
from pathlib import Path
from typing import Any, Dict
import yaml

from src.common.logging_utils import get_logger

logger = get_logger(__name__)


def load_yaml_config(file_path: str) -> Dict[str, Any]:
    """
    Loads a YAML configuration file from disk.

    Args:
        file_path: Relative or absolute path to the YAML file.

    Returns:
        Dictionary representation of configuration.
    """
    path = Path(file_path)
    if not path.exists():
        logger.warning("Config file %s does not exist. Returning empty configuration.", file_path)
        return {}

    with open(path, "r", encoding="utf-8") as f:
        try:
            config = yaml.safe_load(f) or {}
            return config
        except yaml.YAMLError as exc:
            logger.error("Failed to parse YAML config file %s: %s", file_path, exc)
            raise


def load_config(config_type: str = "spark") -> Dict[str, Any]:
    """
    Loads application configuration with environment variable overrides.

    Args:
        config_type: Identifier for configuration ("spark" or "producer").

    Returns:
        Dictionary with merged configuration settings.
    """
    base_dir = Path(__file__).resolve().parent.parent.parent
    config_file = base_dir / "config" / f"{config_type}_config.yaml"

    config = load_yaml_config(str(config_file))

    # Environment variable overrides
    if config_type == "spark":
        if "KAFKA_BOOTSTRAP_SERVERS" in os.environ:
            config.setdefault("kafka", {})["bootstrap_servers"] = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
        if "MINIO_ENDPOINT" in os.environ:
            config.setdefault("storage", {})["s3_endpoint"] = os.environ["MINIO_ENDPOINT"]
        if "MINIO_ACCESS_KEY" in os.environ:
            config.setdefault("storage", {})["s3_access_key"] = os.environ["MINIO_ACCESS_KEY"]
        if "MINIO_SECRET_KEY" in os.environ:
            config.setdefault("storage", {})["s3_secret_key"] = os.environ["MINIO_SECRET_KEY"]
        if "S3_BUCKET" in os.environ:
            config.setdefault("storage", {})["s3_bucket"] = os.environ["S3_BUCKET"]

    elif config_type == "producer":
        if "KAFKA_BOOTSTRAP_SERVERS" in os.environ:
            config.setdefault("kafka", {})["bootstrap_servers"] = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
        if "KAFKA_TOPIC" in os.environ:
            config.setdefault("kafka", {})["topic"] = os.environ["KAFKA_TOPIC"]
        if "EVENTS_PER_SECOND" in os.environ:
            config.setdefault("simulation", {})["events_per_second"] = int(os.environ["EVENTS_PER_SECOND"])

    return config
