import logging
from io import BytesIO

import pandas as pd
from fastapi import FastAPI

from config import settings
from kafka_client import start_consumer, send_to_kafka, wait_for_kafka
from s3_client import from_s3_to_mem, upload_to_s3
from service import engineer_features
from logger import get_logger

logger = get_logger(__name__)

app = FastAPI(title="OHLCV processing service")

RAW_TOPIC = settings.RAW_TOPIC
CLEAN_TOPIC = settings.CLEAN_TOPIC
RAW_BUCKET = settings.RAW_BUCKET
CLEAN_BUCKET = settings.CLEAN_BUCKET


def process_message(message: dict):
    """
    Обрабатывает одно сообщение из Kafka:
    1) берёт parquet по s3_key_parquet из RAW_BUCKET
    2) применяет engineer_features
    3) кладёт результат в CLEAN_BUCKET в parquet и csv
    4) посылает новое сообщение в CLEAN_TOPIC
    """

    logger.info(f"Processing message from topic '{RAW_TOPIC}': {message}")

    try:
        s3_key_parquet = message["s3_key_parquet"]
    except KeyError:
        logger.error("Message does not contain required field 's3_key_parquet'")
        return

    try:
        # 1. Забираем исходный parquet из RAW_BUCKET
        buffer = from_s3_to_mem(s3_key_parquet, bucket=RAW_BUCKET)
        df = pd.read_parquet(buffer)
        logger.info(f"Loaded dataframe from s3://{RAW_BUCKET}/{s3_key_parquet} "
                    f"with shape {df.shape}")

        # 2. Обрабатываем данные (feature engineering / очистка и т.п.)
        df_processed = engineer_features(df)
        logger.info(f"Processed dataframe shape: {df_processed.shape}")

        # 3. Формируем ключи для очищенных данных
        # Пример: TSLA/.../file.parquet -> TSLA/.../file_clean.parquet/csv
        clean_key_parquet = s3_key_parquet.replace(".parquet", "_clean.parquet")
        clean_key_csv = s3_key_parquet.replace(".parquet", "_clean.csv")

        # 3.1 Сохраняем parquet в CLEAN_BUCKET
        parquet_buf = BytesIO()
        df_processed.to_parquet(parquet_buf, index=False)
        parquet_buf.seek(0)
        upload_to_s3(parquet_buf, clean_key_parquet, bucket=CLEAN_BUCKET)

        # 3.2 Сохраняем csv в CLEAN_BUCKET
        csv_buf = BytesIO()
        df_processed.to_csv(csv_buf, index=False)
        csv_buf.seek(0)
        upload_to_s3(csv_buf, clean_key_csv, bucket=CLEAN_BUCKET)

        # 4. Готовим и отправляем сообщение в CLEAN_TOPIC
        out_message = {
            **message,
            "s3_key_parquet": clean_key_parquet,
            "s3_key_csv": clean_key_csv,
            "s3_uri_parquet": f"s3://{CLEAN_BUCKET}/{clean_key_parquet}",
            "s3_uri_csv": f"s3://{CLEAN_BUCKET}/{clean_key_csv}",
            "type": "clean",
        }

        send_to_kafka(CLEAN_TOPIC, out_message)
        logger.info(f"Sent processed message to topic '{CLEAN_TOPIC}': {out_message}")

    except Exception as e:
        logger.exception(f"Failed to process message: {e}")


@app.on_event("startup")
def on_startup():
    """
    При старте приложения:
    - ждём доступности Kafka
    - запускаем consumer RAW_TOPIC в отдельном потоке
    """
    logger.info("Application startup: waiting for Kafka...")
    wait_for_kafka()

    logger.info(f"Starting Kafka consumer for topic '{RAW_TOPIC}'")
    start_consumer(
        topic=RAW_TOPIC,
        on_message=process_message,
        auto_offset_reset="latest",
        run_in_thread=True,
    )
    logger.info("Kafka consumer started")


@app.get("/health")
def healthcheck():
    return {"status": "ok"}
