from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

import service


app = FastAPI(title="Model Trainer Worker", version="1.0.0")


@app.on_event("startup")
def on_startup():
    worker.start_worker()


@app.on_event("shutdown")
def on_shutdown():
    worker.stop_worker()


@app.get("/health")
def health():
    status = worker.get_status()
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
        return worker_app.train_and_save(req.message)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
