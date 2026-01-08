import time
from starlette.middleware.base import BaseHTTPMiddleware
from metrics.registry import HTTP_REQUESTS_TOTAL, HTTP_REQUEST_LATENCY

class MetricsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        start = time.monotonic()
        status = "500"
        try:
            response = await call_next(request)
            status = str(response.status_code)
            return response
        finally:
            duration = time.monotonic() - start
            path = request.url.path
            method = request.method

            HTTP_REQUEST_LATENCY.labels(
                method=method,
                path=path
            ).observe(duration)

            HTTP_REQUESTS_TOTAL.labels(
                method=method,
                path=path,
                status=status
            ).inc()

