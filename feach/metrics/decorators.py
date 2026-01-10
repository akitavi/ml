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

def track_feature_metrics(df_features):
    """Decorator to track feature row and column metrics."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            FEATURES_ROWS.set(int(df_features.shape[0]))
            FEATURES_COLUMNS.set(int(df_features.shape[1]))
            return result
        return wrapper
    return decorator