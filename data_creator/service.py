# service_core.py
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
    """Clean downloaded dataframe."""
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

    # handle timestamp
    timestamp_col_candidates = ["datetime", "timestamp", "time", "date"]
    for col in timestamp_col_candidates:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col])
                if col != "timestamp":
                    df.rename(columns={col: "timestamp"}, inplace=True)
                break
            except Exception as err:
                logger.warning(f"Failed to convert '{col}': {err}")

    if "timestamp" not in df.columns:
        raise KeyError("'timestamp' column is required")

    df["unixtime"] = df["timestamp"].astype(np.int64) // 10**9

    # convert object columns to numeric where possible
    for col in df.columns:
        if df[col].dtype == object:
            try:
                df[col] = pd.to_numeric(df[col])
            except Exception:
                pass

    return df


def upload_clean_artifacts(df_clean: pd.DataFrame, ticker: str, start: str, end: str, interval: str):
    """Upload clean parquet + CSV to S3 and return their keys."""
    base_name = f"{ticker}_{start}_{end}_{interval}_clean"
    clean_parquet_key = f"{ticker}/{interval}/{base_name}.parquet"
    clean_csv_key = f"{ticker}/{interval}/{base_name}.csv"

    # parquet
    parquet_buffer = BytesIO()
    df_clean.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    upload_to_s3(parquet_buffer, clean_parquet_key, bucket=settings.CLEAN_BUCKET)
    logger.info(f"[S3] Uploaded cleaned parquet to s3://{settings.CLEAN_BUCKET}/{clean_parquet_key}")

    # CSV
    csv_buffer = BytesIO()
    df_clean.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    upload_to_s3(csv_buffer, clean_csv_key, bucket=settings.CLEAN_BUCKET)
    logger.info(f"[S3] Uploaded cleaned CSV to s3://{settings.CLEAN_BUCKET}/{clean_csv_key}")

    return clean_parquet_key, clean_csv_key


def generate_clean(ticker: str, start: str, end: str, interval: str):
    """Full pipeline: download → clean → upload → kafka notify."""
    # 1) download
    df_raw = download_yf_dataframe(ticker, start, end, interval)
    # 2) clean
    df_clean = clean_dataframe(df_raw, ticker)
    # 3) upload
    parquet_key, csv_key = upload_clean_artifacts(df_clean, ticker, start, end, interval)
    # 4) kafka notify
    if kafka_client.kafka_enabled:
        kafka_msg = {
            "ticker": ticker,
            "interval": interval,
            "s3_key_parquet": parquet_key,
            "s3_key_csv": csv_key,
            "s3_uri_parquet": f"s3://{settings.CLEAN_BUCKET}/{parquet_key}",
            "s3_uri_csv": f"s3://{settings.CLEAN_BUCKET}/{csv_key}",
            "start": start,
            "end": end,
            "created_at": datetime.utcnow().isoformat(),
            "type": "clean"
        }
        send_to_kafka(settings.CLEAN_TOPIC, kafka_msg)
    return parquet_key, csv_key

