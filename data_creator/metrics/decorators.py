import time
from functools import wraps
from metrics.registry import SERVICE_LATENCY, SERVICE_ERRORS

def track_step(step_name: str):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.monotonic()
            try:
                return func(*args, **kwargs)
            except Exception:
                SERVICE_ERRORS.labels(step=step_name).inc()
                raise
            finally:
                SERVICE_LATENCY.labels(step=step_name).observe(time.monotonic() - start)
        return wrapper
    return decorator
