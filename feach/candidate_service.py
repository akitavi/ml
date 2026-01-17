import hashlib
from io import BytesIO
from typing import Optional

import pandas as pd

from candidate_generator import build_shortlist
from config import settings
from kafka_client import send_to_kafka
from logger import get_logger
from s3_client import upload_to_s3

logger = get_logger(__name__)


def _seed_from_run_id(run_id: str) -> int:
    h = hashlib.sha1((run_id or "").encode("utf-8")).hexdigest()
    return int(h[:8], 16)


def _shortlist_s3_key(ticker: str, run_id: str) -> str:
    t = ticker or "UNKNOWN"
    return f"{settings.SHORTLIST_PREFIX}/{t}/{run_id}/shortlist.parquet"


def generate_and_publish_candidates(
    df_features: pd.DataFrame,
    *,
    ticker: str,
    dataset_key: str,
    dataset_version: str,
    run_id: str,
    kafka_topic: Optional[str] = None,
) -> str:
    topic = kafka_topic or settings.CANDIDATES_TOPIC

    shortlist = build_shortlist(
        df_features=df_features,
        ticker=ticker,
        dataset_key=dataset_key,
        dataset_version=dataset_version,
        target_horizon=settings.TARGET_HORIZON,
        wf_train_size=settings.WF_TRAIN_SIZE,
        wf_test_size=settings.WF_TEST_SIZE,
        wf_step=settings.WF_STEP,
        wf_max_splits=settings.WF_MAX_SPLITS,
        pool_top_n=settings.POOL_TOP_N,
        shortlist_max=settings.SHORTLIST_MAX,
        max_group_size=settings.MAX_GROUP_SIZE,
        beam_width=settings.BEAM_WIDTH,
        # pair_scan_top_n=settings.PAIR_SCAN_TOP_N,
        # pair_scan_keep=settings.PAIR_SCAN_KEEP,
        max_abs_corr_in_group=settings.MAX_ABS_CORR_IN_GROUP,
        diversity_jaccard_max=settings.DIVERSITY_JACCARD_MAX,
        rank_std_penalty=settings.RANK_STD_PENALTY,
        rank_corr_penalty=settings.RANK_CORR_PENALTY,
        rng_seed=_seed_from_run_id(run_id),
    )

    key = _shortlist_s3_key(ticker, run_id)
    buf = BytesIO()
    shortlist.to_parquet(buf, index=False)
    buf.seek(0)
    upload_to_s3(buf, key, bucket=settings.SHORTLIST_BUCKET)
    logger.info(f"Shortlist saved: s3://{settings.SHORTLIST_BUCKET}/{key} (rows={len(shortlist)})")

    for _, row in shortlist.iterrows():
        msg = {
            "run_id": run_id,
            "candidate_id": row["candidate_id"],
            "ticker": row["ticker"],
            "dataset_key": row["dataset_key"],
            "dataset_version": row["dataset_version"],
            "features": row["features"],
            "group_size": int(row["group_size"]),
            "strategy": row["strategy"],
            "generation": int(row["generation"]),
            "parent_id": row["parent_id"],
            "quick_metric": row["quick_metric"],
            "quick_mean": float(row["quick_mean"]),
            "quick_std": float(row["quick_std"]),
            "splits_meta": row["splits_meta"],
            "novelty_score": float(row["novelty_score"]),
            "corr_penalty": float(row["corr_penalty"])
        }
        send_to_kafka(topic, msg)

    logger.info(f"Published {len(shortlist)} candidates to Kafka topic '{topic}'")
    return key
