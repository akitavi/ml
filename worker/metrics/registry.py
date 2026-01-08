from prometheus_client import Counter, Histogram

# === HTTP / FastAPI ===
HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "path", "status"]
)

HTTP_REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency",
    ["method", "path"]
)

# === Service / pipeline ===
SERVICE_LATENCY = Histogram(
    "service_step_duration_seconds",
    "Time spent in service steps",
    ["step"]
)

SERVICE_ERRORS = Counter(
    "service_step_errors_total",
    "Errors in service steps",
    ["step"]
)
