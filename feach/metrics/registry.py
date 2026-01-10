from prometheus_client import Counter, Histogram, Gauge

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
PROCESS_MESSAGE_TIME = Histogram(
    "process_message_step_seconds",
    "Time spent in process_message steps",
    ["step"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30)
)

# Counter to count Kafka messages processed
KAFKA_MESSAGES_TOTAL = Counter(
    "kafka_messages_total",
    "Total Kafka messages handled by this service",
    ["result"],
)

# Gauge to track the number of feature rows and columns processed
FEATURES_ROWS = Gauge(
    "features_rows_last",
    "Number of feature rows produced by the last successfully processed message",
)
FEATURES_COLUMNS = Gauge(
    "features_columns_last",
    "Number of feature columns produced by the last successfully processed message",
)