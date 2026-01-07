from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

import service


app = FastAPI(title="Model Trainer Worker", version="1.0.0")


@app.on_event("startup")
def on_startup():
    service.start_service()


@app.on_event("shutdown")
def on_shutdown():
    service.stop_service()


@app.get("/health")
def health():
    status = service.get_status()
    return {"status": "ok", **status}


class TrainRequest(BaseModel):
    message: dict


@app.post("/train_once")
def train_once(req: TrainRequest):
    """
    Ручной запуск тренировки по payload (для отладки).
    Kafka offsets тут не трогаем.
    """
    try:
        return service_app.train_and_save(req.message)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
