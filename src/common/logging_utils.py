"""
Structured logging configuration utility for console and file log handlers.
"""

import logging
import os
import sys
from typing import Optional


def get_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    """
    Initializes and returns a structured logger with standardized formatting.

    Args:
        name: Name of the logger module.
        level: Explicit log level (DEBUG, INFO, WARN, ERROR). Defaults to LOG_LEVEL env var or INFO.

    Returns:
        Configured logging.Logger instance.
    """
    log_level_str = level or os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(log_level)

        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(log_level)

        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False

    return logger
