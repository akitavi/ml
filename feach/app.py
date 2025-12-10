import logging
from io import BytesIO

import pandas as pd
from fastapi import FastAPI

from config import settings
from kafka_client import start_consumer, wait_for_kafka
from s3_client import from_s3_to_mem, upload_to_s3
from service import engineer_features
from logger import get_logger

logger = get_logger(__name__)

app = FastAPI(title="OHLCV features service")

# ВХОД: читаем сообщения из CLEAN_TOPIC и данные из CLEAN_BUCKET
INPUT_TOPIC = settings.CLEAN_TOPIC        # env CLEAN_TOPIC=clean-data
INPUT_BUCKET = settings.CLEAN_BUCKET      # env CLEAN_BUCKET=generate-clean

# ВЫХОД: складываем фичи в тот же бакет (можно вынести в отдельный FEATURES_BUCKET при желании)
OUTPUT_BUCKET = settings.CLEAN_BUCKET


def process_message(message: dict):
    """
    Обрабатывает одно сообщение из Kafka:

    1) берёт parquet по s3_key_parquet из INPUT_BUCKET
    2) применяет engineer_features
    3) кладёт результат в OUTPUT_BUCKET в parquet и csv (с суффиксом _features)
    """

    logger.info(f"Processing message from topic '{INPUT_TOPIC}': {message}")

    try:
        s3_key_parquet = message["s3_key_parquet"]
    except KeyError:
        logger.error("Message does not contain required field 's3_key_parquet'")
        return

    try:
        # 1. Забираем исходный parquet из INPUT_BUCKET
        buffer = from_s3_to_mem(s3_key_parquet, bucket=INPUT_BUCKET)
        df = pd.read_parquet(buffer)
        logger.info(
            f"Loaded dataframe from s3://{INPUT_BUCKET}/{s3_key_parquet} "
            f"with shape {df.shape}"
        )

        # 2. Обрабатываем данные (feature engineering)
        df_features = engineer_features(df)
        logger.info(f"Features dataframe shape: {df_features.shape}")

        # 3. Формируем ключи для фичей
        # Пример: TSLA/.../file_clean.parquet -> TSLA/.../file_clean_features.parquet/csv
        features_key_parquet = s3_key_parquet.replace(
            ".parquet", "_features.parquet"
        )
        features_key_csv = s3_key_parquet.replace(
            ".parquet", "_features.csv"
        )

        # 3.1 Сохраняем parquet в OUTPUT_BUCKET
        parquet_buf = BytesIO()
        df_features.to_parquet(parquet_buf, index=False)
        parquet_buf.seek(0)
        upload_to_s3(parquet_buf, features_key_parquet, bucket=OUTPUT_BUCKET)

        # 3.2 Сохраняем csv в OUTPUT_BUCKET
        csv_buf = BytesIO()
        df_features.to_csv(csv_buf, index=False)
        csv_buf.seek(0)
        upload_to_s3(csv_buf, features_key_csv, bucket=OUTPUT_BUCKET)

        logger.info(
            "Features saved to S3:\n"
            f"  s3://{OUTPUT_BUCKET}/{features_key_parquet}\n"
            f"  s3://{OUTPUT_BUCKET}/{features_key_csv}"
        )

        # Здесь **не** отправляем новое сообщение в Kafka,
        # т.к. этот сервис выступает как "Kafka → S3" слушатель.

    except Exception as e:
        logger.exception(f"Failed to process message: {e}")


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
