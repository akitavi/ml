from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from service import (
    start_service,
    stop_service,
    get_status,
    state,
)

app = FastAPI(
    title="Model Trainer Worker",
    version="1.0.0",
)


@app.on_event("startup")
def on_startup():
    """
    Автоматически запускаем воркер при старте FastAPI.
    """
    start_service()


@app.on_event("shutdown")
def on_shutdown():
    """
    Корректно останавливаем воркер.
    """
    stop_service()


@app.get("/health", tags=["system"])
def health():
    """
    Kubernetes / Docker healthcheck.
    """
    return {"status": "ok"}


@app.get("/status", tags=["worker"])
def status():
    """
    Текущий статус воркера и счётчики.
    """
    return get_status()
