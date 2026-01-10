from io import BytesIO

import uuid

import pandas as pd
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from fastapi import FastAPI
from fastapi.responses import Response

from config import settings
from kafka_client import start_consumer, wait_for_kafka
from s3_client import from_s3_to_mem, upload_to_s3
from service import engineer_features
from logger import get_logger


from metrics.middleware import MetricsMiddleware
from metrics.decorators import track_step, track_feature_metrics
from metrics.registry import KAFKA_MESSAGES_TOTAL, FEATURES_ROWS, FEATURES_COLUMNS
from candidate_service import generate_and_publish_candidates

logger = get_logger(__name__)

app = FastAPI(title="OHLCV features service")

# HTTP metrics (requests/latency) are collected by middleware below.
app.add_middleware(MetricsMiddleware)



# ВХОД: читаем сообщения из CLEAN_TOPIC и данные из CLEAN_BUCKET
INPUT_TOPIC = settings.CLEAN_TOPIC        # env CLEAN_TOPIC=clean-data
INPUT_BUCKET = settings.CLEAN_BUCKET      # env CLEAN_BUCKET=clean-data

# ВЫХОД: складываем фичи в тот же бакет (можно вынести в отдельный FEATURES_BUCKET при желании)
OUTPUT_BUCKET = settings.CLEAN_BUCKET


def _infer_ticker(message: dict, s3_key: str) -> str:
    if isinstance(message, dict) and message.get("ticker"):
        return str(message["ticker"])
    if isinstance(s3_key, str) and "/" in s3_key:
        return s3_key.split("/", 1)[0]
    return "UNKNOWN"


@track_step("load_s3")
def load_s3_data(s3_key_parquet):
    """Load the parquet data from S3."""
    raw_buf = from_s3_to_mem(s3_key_parquet, bucket=INPUT_BUCKET)
    df = pd.read_parquet(raw_buf)
    return df

@track_step("feature_engineering")
@track_feature_metrics
def feature_engineering(df):
    """Apply feature engineering to the data."""
    return engineer_features(df)

@track_step("upload_s3")
def upload_to_s3_files(df_features, s3_key_parquet):
    """Upload feature data to S3."""
    logger.debug(f"Uploading to S3, df_features type: {type(df_features)}")
    if not isinstance(df_features, pd.DataFrame):
        logger.error("df_features is not a pandas DataFrame!")
        return

    features_key_parquet = s3_key_parquet.replace(".parquet", "_features.parquet")
    features_key_csv = s3_key_parquet.replace(".parquet", "_features.csv")

    parquet_buf = BytesIO()
    df_features.to_parquet(parquet_buf, index=False)
    parquet_buf.seek(0)
    upload_to_s3(parquet_buf, features_key_parquet, bucket=OUTPUT_BUCKET)

    csv_buf = BytesIO()
    df_features.to_csv(csv_buf, index=False)
    csv_buf.seek(0)
    upload_to_s3(csv_buf, features_key_csv, bucket=OUTPUT_BUCKET)

    logger.info(
        "Features saved to S3:\n"
        f"  s3://output-bucket/{features_key_parquet}\n"
        f"  s3://output-bucket/{features_key_csv}"
    )

@track_step("candidate_generation")
def generate_candidates(message, df_features, s3_key_parquet):
    """Generate and publish candidates."""
    run_id = str(message.get("run_id") or uuid.uuid4())
    dataset_version = str(message.get("dataset_version") or "v1")
    ticker = _infer_ticker(message, s3_key_parquet)

    try:
        shortlist_key = generate_and_publish_candidates(
            df_features=df_features,
            ticker=ticker,
            dataset_key=s3_key_parquet,
            dataset_version=dataset_version,
            run_id=run_id,
        )
        logger.info(
            f"Shortlist generated: s3://{settings.SHORTLIST_BUCKET}/{shortlist_key}"
        )
    except Exception as e:
        logger.exception(
            "Candidate generation/publish failed "
            "(features already saved): %s", e
        )

@track_step("process_message")
def process_message(message: dict):
    """Main function to process messages."""
    logger.info(f"Processing message from topic 'input-topic': {message}")

    try:
        s3_key_parquet = message["s3_key_parquet"]
    except KeyError:
        logger.error("Message does not contain required field 's3_key_parquet'")
        KAFKA_MESSAGES_TOTAL.labels(result="skipped").inc()
        return

    try:
        # Load parquet from S3
        df = load_s3_data(s3_key_parquet)

        # Feature engineering
        df_features = feature_engineering(df)

        # Save features to S3
        upload_to_s3_files(df_features, s3_key_parquet)

        # Candidate generation
        if settings.ENABLE_CANDIDATE_GEN:
            generate_candidates(message, df_features, s3_key_parquet)

        KAFKA_MESSAGES_TOTAL.labels(result="success").inc()

    except Exception as e:
        logger.exception(f"Failed to process message: {e}")
        KAFKA_MESSAGES_TOTAL.labels(result="error").inc()


@app.on_event("startup")
def on_startup():
    """
    При старте приложения:
    - ждём доступности Kafka
    - запускаем consumer INPUT_TOPIC в отдельном потоке
    """
    logger.info("Application startup: waiting for Kafka...")
    wait_for_kafka()

    logger.info(f"Starting Kafka consumer for topic '{INPUT_TOPIC}'")
    start_consumer(
        topic=INPUT_TOPIC,
        on_message=process_message,
        auto_offset_reset="latest",  # для отладки можно временно поставить 'earliest'
        run_in_thread=True,
    )
    logger.info("Kafka consumer started")


@app.get("/health")
def healthcheck():
    return {"status": "ok"}


@app.get("/metrics")
def metrics():
    return Response(
        generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )
