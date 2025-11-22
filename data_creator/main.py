from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from datetime import datetime
from pathlib import Path
import yfinance as yf
import pandas as pd
from io import BytesIO
from config import settings
from s3_client import upload_to_s3
import kafka_client
from kafka_client import init_kafka_producer, send_to_kafka

from logger import get_logger
logger = get_logger(__name__)


app = FastAPI()


class DownloadRequest(BaseModel):
    ticker: str = "AMD"
    start: str = "2024-01-01"  # yyyy-mm-dd
    end: str = "2025-07-01"
    interval: str = "1d"


def download_yf_data(ticker: str, start: str, end: str, interval: str) -> str:
    logger.debug(f"Downloading data from YahooFinance: {ticker} from {start} to {end} with interval={interval}")
    df = yf.download(ticker, start=start, end=end, interval=interval)

    if not isinstance(df, pd.DataFrame) or df.empty:
        logger.warning(f"No data for {ticker} {start}→{end} ({interval})")
        raise ValueError("No data returned or invalid format")

    # Сохраняем parquet в памяти и отправляем в S3
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=True)
    parquet_buffer.seek(0)
    parquet_name = f"{ticker}_{start}_{end}_{interval}.parquet"
    s3_key = f"{ticker}/{interval}/{parquet_name}"
    upload_to_s3(parquet_buffer, s3_key)

    return s3_key



@app.on_event("startup")
def startup_event():
    kafka_client.init_kafka_producer()


@app.post("/generate_raw")
def generate(req: DownloadRequest, request: Request):
    try:
        client_ip = request.client.host if request.client else "unknown"
        logger.debug(f"Request from {client_ip}, data: {req.dict()}")

        s3_key = download_yf_data(
            ticker=req.ticker,
            start=req.start,
            end=req.end,
            interval=req.interval
        )

        if kafka_client.kafka_enabled:
            kafka_msg = {
                "ticker": req.ticker,
                "interval": req.interval,
                "s3_uri": f"s3://{settings.RAW_BUCKET}/{s3_key}",
                "s3_key": s3_key,
                "start": req.start,
                "end": req.end,
                "created_at": datetime.utcnow().isoformat(),
                "type": "raw"
            }
            send_to_kafka(settings.RAW_TOPIC, kafka_msg)
        else:
            logger.error("Kafka disabled, skipping publish")

        return {"status": "ok", "s3_key": s3_key}

    except Exception as e:
        logger.error(f"Unhandled error: {e}")
        raise HTTPException(status_code=500, detail=str(e))