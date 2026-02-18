import os
import logging
import json


# ---------- logging ----------
LOG_DIR: str = os.getenv("LOG_DIR", "./logs")
LOG_LEVEL_NAME = os.getenv("LOG_LEVEL", "DEBUG").upper()
LOG_LEVEL: int = getattr(logging, LOG_LEVEL_NAME, logging.DEBUG)
LOG_TO_FILE: bool = os.getenv("LOG_TO_FILE", "false").lower() == "true"


# ---------- price source ----------
PRICE_SOURCE: str = os.getenv("PRICE_SOURCE", "yfinance")


# ---------- kafka ----------
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "prices")
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9094")
KAFKA_GROUP_ID: str = os.getenv("KAFKA_GROUP_ID", "ohlcv-group")

KAFKA_MAX_RETRIES: int = int(os.getenv("KAFKA_MAX_RETRIES", "15"))
KAFKA_RETRY_INTERVAL: float = float(os.getenv("KAFKA_RETRY_INTERVAL", "3"))

KAFKA_PRODUCER_CONFIG = {
    "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
    "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
    "request_timeout_ms": 5000,
    "retries": 5,
    "retry_backoff_ms": 1000,
    "api_version_auto_timeout_ms": 5000,
}
