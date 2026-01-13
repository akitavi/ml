import os
import json


class Settings:
    # app
    S3_CSV_PATH = os.getenv("S3_CSV_PATH", "./s3_tmp/raw")
    RAW_BUCKET = os.getenv("RAW_BUCKET", "raw-data")
    RAW_TOPIC = os.getenv("RAW_TOPIC", "raw-data")
    CLEAN_TOPIC = os.getenv("CLEAN_TOPIC", "clean-data")
    CLEAN_BUCKET = os.getenv("CLEAN_BUCKET", "clean-data")

    # logs
    LOG_DIR: str = os.getenv("LOG_DIR", "./logs")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_TO_FILE = os.getenv("LOG_TO_FILE", "false").lower() == "true"

    # s3 / minio
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", MINIO_ACCESS_KEY)
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", MINIO_SECRET_KEY)

    # mlflow (kept)
    MLFLOW_S3_ENDPOINT_URL = os.getenv("MLFLOW_S3_ENDPOINT_URL", "http://minio:9000")
    MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT", "price_movement_classifier")

    # kafka
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

    # candidate generation
    ENABLE_CANDIDATE_GEN = os.getenv("ENABLE_CANDIDATE_GEN", "true").lower() == "true"
    CANDIDATES_TOPIC = os.getenv("CANDIDATES_TOPIC", "feature-candidates")

    SHORTLIST_BUCKET = os.getenv("SHORTLIST_BUCKET", CLEAN_BUCKET)
    SHORTLIST_PREFIX = os.getenv("SHORTLIST_PREFIX", "shortlists")

    # target + walk-forward
    TARGET_HORIZON = int(os.getenv("TARGET_HORIZON", "1"))
    WF_TRAIN_SIZE = int(os.getenv("WF_TRAIN_SIZE", "1500"))
    WF_TEST_SIZE = int(os.getenv("WF_TEST_SIZE", "250"))
    WF_STEP = int(os.getenv("WF_STEP", "250"))
    WF_MAX_SPLITS = int(os.getenv("WF_MAX_SPLITS", "20"))

    # pool + shortlist sizes
    POOL_TOP_N = int(os.getenv("POOL_TOP_N", "120"))
    SHORTLIST_MAX = int(os.getenv("SHORTLIST_MAX", "150"))

    # group generation controls
    MAX_GROUP_SIZE = int(os.getenv("MAX_GROUP_SIZE", "8"))
    BEAM_WIDTH = int(os.getenv("BEAM_WIDTH", "50"))

    # pair scan
    # PAIR_SCAN_TOP_N = int(os.getenv("PAIR_SCAN_TOP_N", "50"))
    # PAIR_SCAN_KEEP = int(os.getenv("PAIR_SCAN_KEEP", "40"))

    # constraints / diversity
    MAX_ABS_CORR_IN_GROUP = float(os.getenv("MAX_ABS_CORR_IN_GROUP", "0.95"))
    DIVERSITY_JACCARD_MAX = float(os.getenv("DIVERSITY_JACCARD_MAX", "0.7"))

    # ranking regularization
    RANK_STD_PENALTY = float(os.getenv("RANK_STD_PENALTY", "0.10"))
    RANK_CORR_PENALTY = float(os.getenv("RANK_CORR_PENALTY", "0.20"))


settings = Settings()
