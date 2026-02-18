import time
import logging
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, TimeoutError

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from config import PRICE_SOURCE
from sources.registry import get_source


router = APIRouter()
logger = logging.getLogger("price-checker.api")

# Один пул потоков на процесс
executor = ThreadPoolExecutor(max_workers=4)

# Таймаут в секундах
PRICE_TIMEOUT_SECONDS = 5


@router.get("/healthz")
def healthz():
    return {"status": "ok"}


@router.get("/price/{ticker}")
def get_price(
    ticker: str,
    source: str = Query(default=PRICE_SOURCE, description="price source"),
):
    started = time.perf_counter()

    try:
        price_source = get_source(source)
    except ValueError as e:
        return JSONResponse(
            {"error": str(e), "available_sources": ["yahoo", "yfinance"]},
            status_code=400,
        )

    try:
        future = executor.submit(price_source.get_price, ticker)
        price: Optional[float] = future.result(timeout=PRICE_TIMEOUT_SECONDS)

        elapsed_ms = int((time.perf_counter() - started) * 1000)

        logger.info(
            "price ok ticker=%s price=%s source=%s elapsed_ms=%s",
            ticker.upper(),
            price,
            source,
            elapsed_ms,
        )

        return {
            "ticker": ticker.upper(),
            "price": price,
            "source": source,
        }

    except TimeoutError:
        elapsed_ms = int((time.perf_counter() - started) * 1000)

        logger.warning(
            "price timeout ticker=%s source=%s elapsed_ms=%s",
            ticker.upper(),
            source,
            elapsed_ms,
        )

        return JSONResponse(
            {
                "ticker": ticker.upper(),
                "error": "price_timeout",
            },
            status_code=504,
        )

    except Exception as e:
        elapsed_ms = int((time.perf_counter() - started) * 1000)

        logger.warning(
            "price failed ticker=%s source=%s err=%s elapsed_ms=%s",
            ticker.upper(),
            source,
            str(e),
            elapsed_ms,
            exc_info=True,
        )

        return JSONResponse(
            {
                "ticker": ticker.upper(),
                "error": "price_fetch_failed",
                "details": str(e),
            },
            status_code=502,
        )


def register_routes(app):
    app.include_router(router)
