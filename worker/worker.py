import os
import json
import time
import threading
from io import BytesIO
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from logger import get_logger
from s3_client import from_s3_to_mem, upload_to_s3

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
import joblib


logger = get_logger("model_worker")

# --- ENV ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "model-trainer-group")

INPUT_TOPIC = os.getenv("INPUT_TOPIC", "feature-candidates")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "trained-models")

DATASET_BUCKET = os.getenv("DATASET_BUCKET", os.getenv("CLEAN_BUCKET", "clean-data"))
MODELS_BUCKET = os.getenv("MODELS_BUCKET", "models")

AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "earliest")
POLL_TIMEOUT_MS = int(os.getenv("POLL_TIMEOUT_MS", "2000"))

DLQ_TOPIC = os.getenv("DLQ_TOPIC", "feature-candidates-dlq")
SEND_TO_DLQ_ON_ERROR = os.getenv("SEND_TO_DLQ_ON_ERROR", "true").lower() == "true"

MAX_POLL_RECORDS = int(os.getenv("MAX_POLL_RECORDS", "1"))  # 1 = надёжнее


# ------------------ helpers ------------------

def _safe_json_loads(s: str):
    try:
        return json.loads(s)
    except Exception:
        return None


def build_xy(df: pd.DataFrame, feature_cols: list[str]) -> tuple[pd.DataFrame, pd.Series]:
    missing = [c for c in feature_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Dataset missing feature columns: {missing}")
    if "close" not in df.columns:
        raise ValueError("Dataset must contain 'close' column")

    # таргет-baseline: next_close > close
    x = df[feature_cols].copy()
    y = (df["close"].shift(-1) > df["close"]).astype(int)

    x = x.iloc[:-1].reset_index(drop=True)
    y = y.iloc[:-1].reset_index(drop=True)
    return x, y


def eval_walkforward_auc(model, x: pd.DataFrame, y: pd.Series, splits_meta: list[dict]) -> dict:
    aucs: list[float] = []

    for i, sp in enumerate(splits_meta, start=1):
        tr0, tr1 = int(sp["train_start"]), int(sp["train_end"])
        te0, te1 = int(sp["test_start"]), int(sp["test_end"])

        x_tr, y_tr = x.iloc[tr0:tr1], y.iloc[tr0:tr1]
        x_te, y_te = x.iloc[te0:te1], y.iloc[te0:te1]

        if len(x_tr) < 50 or len(x_te) < 20:
            raise ValueError(f"Split #{i} too small: train={len(x_tr)} test={len(x_te)}")

        model.fit(x_tr, y_tr)
        p = model.predict_proba(x_te)[:, 1]
        aucs.append(float(roc_auc_score(y_te, p)))

    return {
        "n_splits": len(aucs),
        "auc_mean": float(sum(aucs) / len(aucs)) if aucs else None,
        "auc_min": float(min(aucs)) if aucs else None,
        "auc_max": float(max(aucs)) if aucs else None,
        "aucs": aucs,
    }


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        request_timeout_ms=5000,
        retries=3,
        retry_backoff_ms=500,
    )


def train_and_save(message: dict) -> dict:
    run_id = message["run_id"]
    candidate_id = message["candidate_id"]
    ticker = message["ticker"]
    dataset_key = message["dataset_key"]
    feature_cols = message["features"]

    splits_meta_raw = message.get("splits_meta")
    splits_meta = None
    if isinstance(splits_meta_raw, str):
        splits_meta = _safe_json_loads(splits_meta_raw)
    elif isinstance(splits_meta_raw, list):
        splits_meta = splits_meta_raw

    # 1) dataset from S3
    buf = from_s3_to_mem(dataset_key, bucket=DATASET_BUCKET)
    df = pd.read_parquet(buf)

    # 2) X/y
    x, y = build_xy(df, feature_cols)

    # 3) baseline model
    model = Pipeline(
        steps=[
            ("scaler", StandardScaler(with_mean=True, with_std=True)),
            ("clf", LogisticRegression(max_iter=2000, n_jobs=1)),
        ]
    )

    metrics = {}
    if splits_meta:
        metrics["walkforward"] = eval_walkforward_auc(model, x, y, splits_meta)

    # final fit on all data
    model.fit(x, y)

    # 4) Save artifacts
    model_key = f"{ticker}/{run_id}/{candidate_id}/model.joblib"
    meta_key = f"{ticker}/{run_id}/{candidate_id}/model_meta.json"

    model_buf = BytesIO()
    joblib.dump(model, model_buf)
    model_buf.seek(0)
    upload_to_s3(model_buf, model_key, bucket=MODELS_BUCKET)

    meta = {
        "run_id": run_id,
        "candidate_id": candidate_id,
        "ticker": ticker,
        "dataset_bucket": DATASET_BUCKET,
        "dataset_key": dataset_key,
        "features": feature_cols,
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "model_bucket": MODELS_BUCKET,
        "model_key": model_key,
        "meta_key": meta_key,
        "metrics": metrics,
        "source_message_created_at": message.get("created_at"),
        "quick_metric": message.get("quick_metric"),
        "quick_mean": message.get("quick_mean"),
        "quick_std": message.get("quick_std"),
        "novelty_score": message.get("novelty_score"),
        "corr_penalty": message.get("corr_penalty"),
        "strategy": message.get("strategy"),
        "generation": message.get("generation"),
        "parent_id": message.get("parent_id"),
    }

    meta_buf = BytesIO(json.dumps(meta, ensure_ascii=False, indent=2).encode("utf-8"))
    upload_to_s3(meta_buf, meta_key, bucket=MODELS_BUCKET)

    return {
        "status": "ok",
        "run_id": run_id,
        "candidate_id": candidate_id,
        "ticker": ticker,
        "model_bucket": MODELS_BUCKET,
        "model_key": model_key,
        "meta_key": meta_key,
        "metrics": metrics,
    }


# ------------------ worker state + loop ------------------

class WorkerState:
    def __init__(self):
        self.thread: Optional[threading.Thread] = None
        self.stop_event = threading.Event()
        self.running = False
        self.last_error: Optional[str] = None
        self.processed_ok = 0
        self.processed_fail = 0


state = WorkerState()


def worker_loop():
    state.running = True
    state.last_error = None

    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset=AUTO_OFFSET_RESET,
        enable_auto_commit=False,   # <-- commit только после успеха
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        max_poll_records=MAX_POLL_RECORDS,
    )

    producer = make_producer()

    logger.info(
        f"Worker loop started. topic={INPUT_TOPIC} group={KAFKA_GROUP_ID} "
        f"bootstrap={KAFKA_BOOTSTRAP_SERVERS} dataset_bucket={DATASET_BUCKET} models_bucket={MODELS_BUCKET}"
    )

    try:
        while not state.stop_event.is_set():
            try:
                polled = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)
                if not polled:
                    continue

                for tp, records in polled.items():
                    for record in records:
                        msg = record.value
                        logger.info(f"Got message p={record.partition} offset={record.offset}")

                        try:
                            result = train_and_save(msg)
                            producer.send(OUTPUT_TOPIC, result)
                            producer.flush(10)

                            consumer.commit()
                            state.processed_ok += 1
                            logger.info(f"Committed offset={record.offset} (success).")

                        except Exception as e:
                            state.processed_fail += 1
                            state.last_error = str(e)
                            logger.exception(f"Training failed offset={record.offset}: {e}")

                            if SEND_TO_DLQ_ON_ERROR:
                                dlq_payload = {
                                    "error": str(e),
                                    "failed_at": datetime.now(timezone.utc).isoformat(),
                                    "original_message": msg,
                                }
                                try:
                                    producer.send(DLQ_TOPIC, dlq_payload)
                                    producer.flush(10)
                                    consumer.commit()  # чтобы не зациклиться
                                    logger.error(f"Sent to DLQ={DLQ_TOPIC} and committed offset={record.offset}.")
                                except Exception as dlq_err:
                                    state.last_error = f"DLQ failed: {dlq_err}"
                                    logger.exception(f"DLQ send failed, will NOT commit offset: {dlq_err}")

            except KafkaError as ke:
                state.last_error = str(ke)
                logger.exception(f"Kafka error: {ke}")
                time.sleep(2)
            except Exception as e:
                state.last_error = str(e)
                logger.exception(f"Unexpected worker loop error: {e}")
                time.sleep(2)

    finally:
        try:
            producer.close()
        except Exception:
            pass
        try:
            consumer.close()
        except Exception:
            pass
        state.running = False
        logger.info("Worker loop stopped.")


def start_worker():
    if state.thread and state.thread.is_alive():
        return
    state.stop_event.clear()
    state.thread = threading.Thread(target=worker_loop, name="kafka-worker", daemon=True)
    state.thread.start()
    logger.info("Background worker thread started.")


def stop_worker():
    state.stop_event.set()
    logger.info("Stop requested.")


def get_status() -> dict:
    return {
        "worker_running": state.running,
        "processed_ok": state.processed_ok,
        "processed_fail": state.processed_fail,
        "last_error": state.last_error,
        "kafka": {
            "bootstrap": KAFKA_BOOTSTRAP_SERVERS,
            "group_id": KAFKA_GROUP_ID,
            "input_topic": INPUT_TOPIC,
            "output_topic": OUTPUT_TOPIC,
            "dlq_topic": DLQ_TOPIC,
        },
        "s3": {
            "dataset_bucket": DATASET_BUCKET,
            "models_bucket": MODELS_BUCKET,
        },
    }

