from io import BytesIO

import uuid

import pandas as pd
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from prometheus_client import Histogram, Counter, Gauge
from fastapi import FastAPI
from fastapi.responses import Response

from config import settings
from kafka_client import start_consumer, wait_for_kafka
from s3_client import from_s3_to_mem, upload_to_s3
from service import engineer_features
from logger import get_logger


from metrics.middleware import MetricsMiddleware

from candidate_service import generate_and_publish_candidates

logger = get_logger(__name__)

app = FastAPI(title="OHLCV features service")

# HTTP metrics (requests/latency) are collected by middleware below.
app.add_middleware(MetricsMiddleware)

PROCESS_MESSAGE_TIME = Histogram(
    "process_message_step_seconds",
    "Time spent in process_message steps",
    ["step"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30)
)

KAFKA_MESSAGES_TOTAL = Counter(
    "kafka_messages_total",
    "Total Kafka messages handled by this service",
    ["result"],
)

FEATURES_ROWS = Gauge(
    "features_rows_last",
    "Number of feature rows produced by the last successfully processed message",
)

FEATURES_COLUMNS = Gauge(
    "features_columns_last",
    "Number of feature columns produced by the last successfully processed message",
)


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


def process_message(message: dict):
    logger.info(f"Processing message from topic '{INPUT_TOPIC}': {message}")

    try:
        s3_key_parquet = message["s3_key_parquet"]
    except KeyError:
        logger.error("Message does not contain required field 's3_key_parquet'")
        KAFKA_MESSAGES_TOTAL.labels(result="skipped").inc()
        return

    try:
        # 1) Load parquet from S3
        with PROCESS_MESSAGE_TIME.labels(step="load_s3").time():
            raw_buf = from_s3_to_mem(s3_key_parquet, bucket=INPUT_BUCKET)
            df = pd.read_parquet(raw_buf)

        logger.info(
            f"Loaded dataframe from s3://{INPUT_BUCKET}/{s3_key_parquet} "
            f"with shape {df.shape}"
        )

        # 2) Feature engineering
        with PROCESS_MESSAGE_TIME.labels(step="feature_engineering").time():
            df_features = engineer_features(df)

        logger.info(f"Features dataframe shape: {df_features.shape}")
        FEATURES_ROWS.set(int(df_features.shape[0]))
        FEATURES_COLUMNS.set(int(df_features.shape[1]))

        # 3) Save features to S3
        with PROCESS_MESSAGE_TIME.labels(step="upload_s3").time():
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
            f"  s3://{OUTPUT_BUCKET}/{features_key_parquet}\n"
            f"  s3://{OUTPUT_BUCKET}/{features_key_csv}"
        )

        # 4) Candidate generation
        if settings.ENABLE_CANDIDATE_GEN:
            with PROCESS_MESSAGE_TIME.labels(step="candidate_generation").time():
                run_id = str(message.get("run_id") or uuid.uuid4())
                dataset_version = str(message.get("dataset_version") or "v1")
                ticker = _infer_ticker(message, s3_key_parquet)

                try:
                    shortlist_key = generate_and_publish_candidates(
                        df_features=df_features,
                        ticker=ticker,
                        dataset_key=features_key_parquet,
                        dataset_version=dataset_version,
                        run_id=run_id,
                    )
                    logger.info(
                        f"Shortlist generated: "
                        f"s3://{settings.SHORTLIST_BUCKET}/{shortlist_key}"
                    )
                except Exception as e:
                    logger.exception(
                        "Candidate generation/publish failed "
                        "(features already saved): %s", e
                    )

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
