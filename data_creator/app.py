# service_api.py
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from logger import get_logger
from kafka_client import init_kafka_producer
from service import generate_clean

from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response
import time

logger = get_logger(__name__)
app = FastAPI()


# =========================
# Prometheus / VictoriaMetrics
# =========================
REQUEST_COUNT = Counter(
    "api_requests_total",
    "Total number of requests",
    ["endpoint", "method", "status"]
)

REQUEST_LATENCY = Histogram(
    "api_request_duration_seconds",
    "Request duration in seconds",
    ["endpoint", "method"]
)

# =========================
# Pydantic model
# =========================
class DownloadRequest(BaseModel):
    ticker: str = "AMD"
    start: str = "2025-01-01"
    end: str = "2025-07-01"
    interval: str = "1d"


# =========================
# Startup
# =========================
@app.on_event("startup")
def startup_event():
    init_kafka_producer()


# =========================
# Metrics endpoint
# =========================
@app.get("/metrics")
def metrics():
    """Expose Prometheus metrics for VictoriaMetrics to scrape."""
    data = generate_latest()
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)


# =========================
# Decorator for metrics
# =========================
def track_metrics(endpoint_name: str):
    """Async decorator for measuring request count and latency"""
    def decorator(func):
        # Передаем все параметры явно, чтобы FastAPI их видел
        async def wrapper(req: DownloadRequest, request: Request):
            start_time = time.time()
            try:
                result = await func(req, request)
                status = "200"
                return result
            except HTTPException as e:
                status = str(e.status_code)
                raise
            except Exception:
                status = "500"
                raise
            finally:
                duration = time.time() - start_time
                REQUEST_LATENCY.labels(endpoint=endpoint_name, method="POST").observe(duration)
                REQUEST_COUNT.labels(endpoint=endpoint_name, method="POST", status=status).inc()
        return wrapper
    return decorator


# =========================
# API Endpoint
# =========================
@app.post("/generate_clean")
@track_metrics("generate_clean")
async def generate(req: DownloadRequest, request: Request):
    try:
        client_ip = request.client.host if request.client else "unknown"
        logger.debug(f"Request from {client_ip}, data: {req.dict()}")

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

    except Exception as e:
        logger.error(f"Unhandled error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
