# service.py
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from datetime import datetime
from io import BytesIO

import yfinance as yf
import pandas as pd
import numpy as np

from config import settings
from s3_client import upload_to_s3
import kafka_client
from kafka_client import init_kafka_producer, send_to_kafka
from logger import get_logger

logger = get_logger(__name__)
app = FastAPI()


# =========================
# Pydantic model
# =========================
class DownloadRequest(BaseModel):
    ticker: str = "AMD"
    start: str = "2025-01-01"  # yyyy-mm-dd
    end: str = "2025-07-01"
    interval: str = "1d"


# =========================
# Download -> clean helpers
# =========================
def download_yf_dataframe(ticker: str, start: str, end: str, interval: str) -> pd.DataFrame:
    """Download data from YahooFinance and return DataFrame (raw)."""
    logger.debug(
        f"Downloading data from YahooFinance: {ticker} from {start} to {end} with interval={interval}"
    )
    df = yf.download(ticker, start=start, end=end, interval=interval)

    if not isinstance(df, pd.DataFrame) or df.empty:
        logger.warning(f"No data for {ticker} {start}→{end} ({interval})")
        raise ValueError("No data returned or invalid format")

    return df


def clean_dataframe(df: pd.DataFrame, ticker: str | None = None) -> pd.DataFrame:
    """Your cleaning logic, unchanged."""
    if isinstance(df.index, pd.MultiIndex) or df.index.name is not None:
        df = df.reset_index()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df.columns.name = None
    df.columns = [str(col).strip().lower() for col in df.columns]

    if ticker and "ticker" not in df.columns:
        df["ticker"] = ticker

    df.dropna(how="all", inplace=True)
    df.drop_duplicates(inplace=True)
    df.replace(["", " ", "NA", "N/A", "NaN", "nan", "null", "NULL"], np.nan, inplace=True)
    threshold = int(df.shape[1] / 2)
    df.dropna(thresh=threshold, inplace=True)

    if "datetime" in df.columns:
        try:
            df["datetime"] = pd.to_datetime(df["datetime"])
            df.rename(columns={"datetime": "timestamp"}, inplace=True)
        except Exception as err:
            logger.warning(f"Failed to convert 'datetime': {err}")
    else:
        for alt in ["timestamp", "time", "date"]:
            if alt in df.columns:
                try:
                    df[alt] = pd.to_datetime(df[alt])
                    if alt != "timestamp":
                        df.rename(columns={alt: "timestamp"}, inplace=True)
                    break
                except Exception as err:
                    logger.warning(f"Failed to convert '{alt}': {err}")

    for col in df.columns:
        if df[col].dtype == object:
            try:
                df[col] = pd.to_numeric(df[col])
            except Exception:
                pass

    if "timestamp" not in df.columns:
        raise KeyError("'timestamp' column is required")

    df["unixtime"] = df["timestamp"].astype(np.int64) // 10**9
    return df


def upload_clean_artifacts(df_clean: pd.DataFrame, ticker: str, start: str, end: str, interval: str):
    """
    Upload clean parquet + clean csv to CLEAN_BUCKET.
    Returns (clean_parquet_key, clean_csv_key).
    """
    base_name = f"{ticker}_{start}_{end}_{interval}_clean"

    clean_parquet_key = f"{ticker}/{interval}/{base_name}.parquet"
    clean_csv_key = f"{ticker}/{interval}/{base_name}.csv"

    logger.debug(
        f"[S3] Preparing to upload cleaned artifacts. "
        f"Bucket={settings.CLEAN_BUCKET}, ParquetKey={clean_parquet_key}, CsvKey={clean_csv_key}"
    )

    # parquet
    try:
        logger.debug(f"[S3] Serializing DataFrame to parquet buffer ({len(df_clean)} rows)")
        parquet_buffer = BytesIO()
        df_clean.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        logger.debug(f"[S3] Uploading parquet → s3://{settings.CLEAN_BUCKET}/{clean_parquet_key}")
        upload_to_s3(parquet_buffer, clean_parquet_key, bucket=settings.CLEAN_BUCKET)
        logger.info(
            f"[S3] Uploaded cleaned parquet to s3://{settings.CLEAN_BUCKET}/{clean_parquet_key}"
        )

    except Exception as err:
        logger.error(f"[S3] Failed uploading parquet: {err}")
        raise

    # csv
    try:
        logger.debug(f"[S3] Serializing DataFrame to CSV buffer ({len(df_clean)} rows)")
        csv_buffer = BytesIO()
        df_clean.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        logger.debug(f"[S3] Uploading CSV → s3://{settings.CLEAN_BUCKET}/{clean_csv_key}")
        upload_to_s3(csv_buffer, clean_csv_key, bucket=settings.CLEAN_BUCKET)
        logger.info(
            f"[S3] Uploaded cleaned csv to s3://{settings.CLEAN_BUCKET}/{clean_csv_key}"
        )

    except Exception as err:
        logger.error(f"[S3] Failed uploading CSV: {err}")
        raise

    return clean_parquet_key, clean_csv_key


# =========================
# FastAPI
# =========================
@app.on_event("startup")
def startup_event():
    init_kafka_producer()


@app.post("/generate_clean") 
def generate(req: DownloadRequest, request: Request):
    try:
        client_ip = request.client.host if request.client else "unknown"
        logger.debug(f"Request from {client_ip}, data: {req.dict()}")

        # 1) download raw into memory
        df_raw = download_yf_dataframe(
            ticker=req.ticker,
            start=req.start,
            end=req.end,
            interval=req.interval
        )

        # 2) clean in memory
        df_clean = clean_dataframe(df_raw, req.ticker)

        # 3) upload ONLY clean artifacts
        clean_parquet_key, clean_csv_key = upload_clean_artifacts(
            df_clean, req.ticker, req.start, req.end, req.interval
        )

        # 4) kafka notify about clean
        if kafka_client.kafka_enabled:
            kafka_msg = {
                "ticker": req.ticker,
                "interval": req.interval,
                "s3_key_parquet": clean_parquet_key,
                "s3_key_csv": clean_csv_key,
                "s3_uri_parquet": f"s3://{settings.CLEAN_BUCKET}/{clean_parquet_key}",
                "s3_uri_csv": f"s3://{settings.CLEAN_BUCKET}/{clean_csv_key}",
                "start": req.start,
                "end": req.end,
                "created_at": datetime.utcnow().isoformat(),
                "type": "clean"
            }
            send_to_kafka(settings.CLEAN_TOPIC, kafka_msg)
        else:
            logger.warning("Kafka disabled, skipping publish")

        return {
            "status": "ok",
            "clean_parquet_key": clean_parquet_key,
            "clean_csv_key": clean_csv_key
        }

    except Exception as e:
        logger.error(f"Unhandled error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# Optional CLI entrypoint
# =========================
def main():
    init_kafka_producer()
    logger.info("Service module loaded. Run with uvicorn to serve API.")


if __name__ == "__main__":
    main()
