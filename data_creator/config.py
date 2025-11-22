import os
import logging
import json
class Settings:

#app
    S3_CSV_PATH = os.getenv("S3_CSV_PATH", "./s3_tmp/raw")
    RAW_BUCKET = os.getenv("RAW_BUCKET", "raw-data")
    RAW_TOPIC = os.getenv("RAW_TOPIC", "raw-data")

#logs
    LOG_DIR: str = os.getenv("LOG_DIR", "./logs")
    LOG_LEVEL: int = getattr(logging, os.getenv("LOG_LEVEL", "DEBUG").upper(), logging.DEBUG)
    LOG_TO_FILE = os.getenv("LOG_TO_FILE", "false").lower() == "true"
#s3
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

#mlflow
    MLFLOW_S3_ENDPOINT_URL = os.getenv("MLFLOW_S3_ENDPOINT_URL", "http://minio:9000")
    MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT", "price_movement_classifier")

#kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "ohlcv-group")
    KAFKA_MAX_RETRIES = int(os.getenv("KAFKA_MAX_RETRIES", "15"))
    KAFKA_RETRY_INTERVAL = float(os.getenv("KAFKA_RETRY_INTERVAL", "3"))
    KAFKA_PRODUCER_CONFIG = {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
        "request_timeout_ms": 5000,
        "retries": 5,
        "retry_backoff_ms": 1000,
        "api_version_auto_timeout_ms": 5000,
    }



settings = Settings()