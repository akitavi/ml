import logging
import time

from fastapi import FastAPI, Request
from config import LOG_LEVEL
from api import register_routes


logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger("price-checker")


def create_app() -> FastAPI:
    app = FastAPI(title="Price Checker", version="1.0.0")

    @app.middleware("http")
    async def log_requests(request: Request, call_next):
        started = time.perf_counter()
        response = await call_next(request)
        elapsed_ms = int((time.perf_counter() - started) * 1000)

        logger.info(
            "request method=%s path=%s status=%s elapsed_ms=%s",
            request.method,
            request.url.path,
            response.status_code,
            elapsed_ms,
        )

        return response

    register_routes(app)
    logger.info("Price Checker started")
    return app


app = create_app()

