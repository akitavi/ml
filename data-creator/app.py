from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from logger import get_logger
from kafka_client import init_kafka_producer
from service import generate_clean
from metrics.middleware import MetricsMiddleware

logger = get_logger(__name__)
app = FastAPI()

app.add_middleware(MetricsMiddleware)


class DownloadRequest(BaseModel):
    ticker: str = "AMD"
    start: str = "2025-01-01"
    end: str = "2025-07-01"
    interval: str = "1d"


@app.on_event("startup")
def startup_event():
    init_kafka_producer()


@app.get("/metrics")
def metrics():
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )


@app.post("/generate_clean")
async def generate(req: DownloadRequest, request: Request):
    parquet_key, csv_key = generate_clean(
        ticker=req.ticker,
        start=req.start,
        end=req.end,
        interval=req.interval
    )

    return {
        "status": "ok",
        "clean_parquet_key": parquet_key,
        "clean_csv_key": csv_key
    }