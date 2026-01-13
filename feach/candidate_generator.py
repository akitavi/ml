import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_candidate_id(
    ticker: str,
    dataset_key: str,
    dataset_version: str,
    features: Sequence[str],
) -> str:
    canon = "\n".join(
        [
            ticker or "",
            dataset_key or "",
            dataset_version or "",
            "\n".join(sorted(features)),
        ]
    )
    return hashlib.sha1(canon.encode("utf-8")).hexdigest()


def jaccard(a: Sequence[str], b: Sequence[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa and not sb:
        return 1.0
    inter = len(sa & sb)
    union = len(sa | sb)
    return inter / union if union else 0.0


def auc_fast(y_true: np.ndarray, y_score: np.ndarray) -> float:
    """ROC-AUC without sklearn. y_true should be 0/1."""
    y_true = np.asarray(y_true).astype(np.int8)
    y_score = np.asarray(y_score).astype(np.float64)

    n_pos = int((y_true == 1).sum())
    n_neg = int((y_true == 0).sum())
    if n_pos == 0 or n_neg == 0:
        return float("nan")

    order = np.argsort(y_score, kind="mergesort")
    ranks = np.empty_like(order, dtype=np.float64)
    ranks[order] = np.arange(1, len(y_score) + 1, dtype=np.float64)

    # average ranks for ties
    sorted_scores = y_score[order]
    i = 0
    while i < len(sorted_scores):
        j = i + 1
        while j < len(sorted_scores) and sorted_scores[j] == sorted_scores[i]:
            j += 1
        if j - i > 1:
            avg = (i + 1 + j) / 2.0
            ranks[order[i:j]] = avg
        i = j

    sum_ranks_pos = float(ranks[y_true == 1].sum())
    auc = (sum_ranks_pos - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg)
    return float(auc)


def make_direction_target(close: pd.Series, horizon: int) -> pd.Series:
    future = close.shift(-horizon)
    return (future > close).astype("float")


def make_walk_forward_splits(
    n: int,
    train_size: int,
    test_size: int,
    step: int,
    max_splits: int,
) -> List[Dict[str, int]]:
    splits: List[Dict[str, int]] = []
    start = 0
    while True:
        train_start = start
        train_end = train_start + train_size
        test_end = train_end + test_size
        if test_end > n:
            break
        splits.append(
            {
                "train_start": int(train_start),
                "train_end": int(train_end),
                "test_start": int(train_end),
                "test_end": int(test_end),
            }
        )
        if len(splits) >= max_splits:
            break
        start += step
    return splits


def _zscore(x: np.ndarray) -> np.ndarray:
    x = np.asarray(x, dtype=np.float64)
    mu = np.nanmean(x)
    sd = np.nanstd(x)
    if not np.isfinite(sd) or sd < 1e-12:
        return np.zeros_like(x, dtype=np.float64)
    z = (x - mu) / sd
    return np.nan_to_num(z, nan=0.0, posinf=0.0, neginf=0.0)


def corr_penalty(df: pd.DataFrame, cols: Sequence[str], idx: np.ndarray) -> float:
    if len(cols) <= 1:
        return 0.0
    x = df.loc[idx, list(cols)].to_numpy(dtype=np.float64)
    x = np.nan_to_num(x, nan=0.0, posinf=0.0, neginf=0.0)
    c = np.corrcoef(x, rowvar=False)
    if c.ndim != 2:
        return 0.0
    n = c.shape[0]
    vals = []
    for i in range(n):
        for j in range(i + 1, n):
            v = abs(float(c[i, j]))
            if np.isfinite(v):
                vals.append(v)
    return float(np.mean(vals)) if vals else 0.0


def max_abs_corr_within(df: pd.DataFrame, cols: Sequence[str], idx: np.ndarray) -> float:
    if len(cols) <= 1:
        return 0.0
    x = df.loc[idx, list(cols)].to_numpy(dtype=np.float64)
    x = np.nan_to_num(x, nan=0.0, posinf=0.0, neginf=0.0)
    c = np.corrcoef(x, rowvar=False)
    if c.ndim != 2:
        return 0.0
    n = c.shape[0]
    m = 0.0
    for i in range(n):
        for j in range(i + 1, n):
            v = abs(float(c[i, j]))
            if np.isfinite(v):
                m = max(m, v)
    return float(m)


@dataclass(frozen=True)
class SingleFeatureScore:
    feature: str
    mean: float
    std: float


def build_candidate_pool(
    df: pd.DataFrame,
    feature_cols: Sequence[str],
    y: pd.Series,
    splits: List[Dict[str, int]],
    top_n: int,
) -> Tuple[List[SingleFeatureScore], Dict[str, Dict[str, float]]]:
    y_np = y.to_numpy()
    stats: Dict[str, Dict[str, float]] = {}
    scored: List[SingleFeatureScore] = []

    for col in feature_cols:
        x = df[col].to_numpy(dtype=np.float64)

        miss = float(np.mean(~np.isfinite(x)))
        if miss > 0.30:
            continue
        if float(np.nanstd(x)) < 1e-10:
            continue

        aucs = []
        for sp in splits:
            te_idx = np.arange(sp["test_start"], sp["test_end"])
            y_te = y_np[te_idx]
            if not np.isfinite(y_te).all():
                continue
            z_te = _zscore(x[te_idx])
            auc = auc_fast(y_te.astype(np.int8), z_te)
            if np.isfinite(auc):
                aucs.append(float(auc))

        if len(aucs) < max(2, min(3, len(splits))):
            continue

        mean = float(np.mean(aucs))
        std = float(np.std(aucs))
        stats[col] = {"mean": mean, "std": std}
        scored.append(SingleFeatureScore(col, mean, std))

    scored.sort(key=lambda s: (s.mean - 0.10 * s.std), reverse=True)
    return scored[:top_n], stats


def _seed_groups(pool: List[SingleFeatureScore], feature_cols: Sequence[str]) -> List[List[str]]:
    top = [s.feature for s in pool[: min(len(pool), 30)]]
    seeds: List[List[str]] = [[f] for f in top]

    def has(cols: Sequence[str]) -> bool:
        return all(c in feature_cols for c in cols)

    hand = []
    if has(["log_ret_lag1", "rsi14", "macd_hist"]):
        hand.append(["log_ret_lag1", "rsi14", "macd_hist"])
    if has(["vol_10", "atr14", "bb_width"]):
        hand.append(["vol_10", "atr14", "bb_width"])
    if has(["ema_10", "ema_20", "ratio_sma_20"]):
        hand.append(["ema_10", "ema_20", "ratio_sma_20"])
    if has(["vol_ratio5", "vol_mean5"]):
        hand.append(["vol_ratio5", "vol_mean5"])
    if has(["dow_sin", "dow_cos", "mth_sin", "mth_cos"]):
        hand.append(["dow_sin", "dow_cos", "mth_sin", "mth_cos"])

    seeds.extend(hand)

    # few 2-feature seeds from top
    for i in range(min(10, len(top))):
        for j in range(i + 1, min(10, len(top))):
            seeds.append([top[i], top[j]])

    seen = set()
    uniq = []
    for g in seeds:
        key = tuple(sorted(g))
        if key in seen:
            continue
        seen.add(key)
        uniq.append(g)
    return uniq


def quick_eval_group_auc(
    df: pd.DataFrame,
    cols: Sequence[str],
    y: pd.Series,
    splits: List[Dict[str, int]],
    single_stats: Dict[str, Dict[str, float]],
) -> Tuple[float, float]:
    """Fast group scoring: weighted linear combo of per-feature z-scores."""
    y_np = y.to_numpy(dtype=np.int8)

    ws = []
    for c in cols:
        m = single_stats.get(c, {}).get("mean", 0.5)
        ws.append(float(m - 0.5))
    w = np.asarray(ws, dtype=np.float64)
    if float(np.sum(np.abs(w))) < 1e-12:
        w = np.ones(len(cols), dtype=np.float64)

    aucs = []
    for sp in splits:
        te = np.arange(sp["test_start"], sp["test_end"])
        y_te = y_np[te]
        if int(y_te.sum()) == 0 or int((1 - y_te).sum()) == 0:
            continue

        X = df.loc[te, list(cols)].to_numpy(dtype=np.float64)
        X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
        Z = np.vstack([_zscore(X[:, i]) for i in range(X.shape[1])]).T
        score = Z @ w
        auc = auc_fast(y_te, score)
        if np.isfinite(auc):
            aucs.append(float(auc))

    if not aucs:
        return float("nan"), float("nan")
    return float(np.mean(aucs)), float(np.std(aucs))


def _rank_value(mean: float, std: float, corr_p: float, std_pen: float, corr_pen: float) -> float:
    if not np.isfinite(mean):
        return -1e9
    std = 0.0 if not np.isfinite(std) else float(std)
    corr_p = 0.0 if not np.isfinite(corr_p) else float(corr_p)
    return float(mean - std_pen * std - corr_pen * corr_p)


def build_shortlist(
    df_features: pd.DataFrame,
    ticker: str,
    dataset_key: str,
    dataset_version: str,
    *,
    target_horizon: int,
    wf_train_size: int,
    wf_test_size: int,
    wf_step: int,
    wf_max_splits: int,
    pool_top_n: int,
    shortlist_max: int,
    max_group_size: int,
    beam_width: int,
    # pair_scan_top_n: int,
    # pair_scan_keep: int,
    max_abs_corr_in_group: float,
    diversity_jaccard_max: float,
    rank_std_penalty: float,
    rank_corr_penalty: float,
    rng_seed: int,
) -> pd.DataFrame:
    df = df_features.copy()
    if "close" not in df.columns:
        raise ValueError("df_features must contain 'close' column to build target")

    feature_cols = [c for c in df.columns if c != "close"]
    y = make_direction_target(df["close"], horizon=target_horizon)
    mask = y.notna()
    df = df.loc[mask].reset_index(drop=True)
    y = y.loc[mask].reset_index(drop=True)

    splits = make_walk_forward_splits(
        n=len(df),
        train_size=wf_train_size,
        test_size=wf_test_size,
        step=wf_step,
        max_splits=wf_max_splits,
    )
    if len(splits) < 2:
        raise ValueError(f"Not enough data for walk-forward splits (n={len(df)})")

    splits_meta = json.dumps(splits, separators=(",", ":"))
    rng = np.random.default_rng(rng_seed)

    pool, single_stats = build_candidate_pool(df, feature_cols, y, splits, top_n=pool_top_n)
    pool_feats = [s.feature for s in pool]

    base_train = np.arange(splits[0]["train_start"], splits[0]["train_end"])

    candidates: List[Dict] = []
    seen_ids = set()

    def add_candidate(feats: List[str], strategy: str, generation: int, parent_id: Optional[str]) -> None:
        feats = list(dict.fromkeys(feats))
        if not feats or len(feats) > max_group_size:
            return
        cid = stable_candidate_id(ticker, dataset_key, dataset_version, feats)
        if cid in seen_ids:
            return
        if max_abs_corr_within(df, feats, base_train) > max_abs_corr_in_group:
            return

        mean, std = quick_eval_group_auc(df, feats, y, splits, single_stats)
        if not np.isfinite(mean):
            return

        cp = corr_penalty(df, feats, base_train)

        candidates.append(
            {
                "candidate_id": cid,
                "ticker": ticker,
                "dataset_key": dataset_key,
                "dataset_version": dataset_version,
                "features": feats,
                "group_size": int(len(feats)),
                "strategy": strategy,
                "generation": int(generation),
                "parent_id": parent_id,
                "quick_metric": "roc_auc",
                "quick_mean": float(mean),
                "quick_std": float(std),
                "splits_meta": splits_meta,
                "novelty_score": 0.0,
                "corr_penalty": float(cp),
                "created_at": _utc_now_iso(),
            }
        )
        seen_ids.add(cid)

    # seeds
    for g in _seed_groups(pool, feature_cols):
        add_candidate(g, strategy="seed", generation=0, parent_id=None)

    # pair_scan
    # top_for_pairs = pool_feats[: min(len(pool_feats), pair_scan_top_n)]
    # pair_scores: List[Tuple[float, List[str]]] = []
    # for i in range(len(top_for_pairs)):
    #     for j in range(i + 1, len(top_for_pairs)):
    #         g = [top_for_pairs[i], top_for_pairs[j]]
    #         if max_abs_corr_within(df, g, base_train) > max_abs_corr_in_group:
    #             continue
    #         mean, std = quick_eval_group_auc(df, g, y, splits, single_stats)
    #         if not np.isfinite(mean):
    #             continue
    #         cp = corr_penalty(df, g, base_train)
    #         pair_scores.append((_rank_value(mean, std, cp, rank_std_penalty, rank_corr_penalty), g))
    # pair_scores.sort(key=lambda t: t[0], reverse=True)
    # for _, g in pair_scores[:pair_scan_keep]:
    #     add_candidate(g, strategy="pair_scan", generation=1, parent_id=None)

    # def rank_rows(rows: List[Dict]) -> List[Tuple[float, Dict]]:
    #     out = []
    #     for r in rows:
    #         out.append((_rank_value(r["quick_mean"], r["quick_std"], r["corr_penalty"], rank_std_penalty, rank_corr_penalty), r))
    #     out.sort(key=lambda t: t[0], reverse=True)
    #     return out

    # beam_search
    ranked = rank_rows(candidates)
    beam = [r for _, r in ranked[: min(len(ranked), beam_width)]]

    generation = 1
    while generation <= (max_group_size - 1) and len(candidates) < shortlist_max * 3:
        expansions: List[Tuple[float, List[str], str]] = []
        for parent in beam[: min(len(beam), beam_width)]:
            parent_feats = parent["features"]
            addon = pool_feats[: min(len(pool_feats), 60)].copy()
            rng.shuffle(addon)
            for add_f in addon[:30]:
                if add_f in parent_feats:
                    continue
                new_feats = parent_feats + [add_f]
                if max_abs_corr_within(df, new_feats, base_train) > max_abs_corr_in_group:
                    continue
                mean, std = quick_eval_group_auc(df, new_feats, y, splits, single_stats)
                if not np.isfinite(mean):
                    continue
                cp = corr_penalty(df, new_feats, base_train)
                expansions.append((_rank_value(mean, std, cp, rank_std_penalty, rank_corr_penalty), new_feats, parent["candidate_id"]))
        if not expansions:
            break
        expansions.sort(key=lambda t: t[0], reverse=True)
        for _, feats, pid in expansions[:beam_width]:
            add_candidate(feats, strategy="beam_search", generation=generation, parent_id=pid)

        ranked = rank_rows(candidates)
        beam = [r for _, r in ranked[: min(len(ranked), beam_width)]]
        generation += 1

    # mutation
    ranked = rank_rows(candidates)
    top_for_mut = [r for _, r in ranked[: min(len(ranked), 30)]]
    for parent in top_for_mut:
        feats = list(parent["features"])
        if len(feats) < 2:
            continue
        drop_idx = int(rng.integers(0, len(feats)))
        base_feats = feats[:drop_idx] + feats[drop_idx + 1 :]
        choices = pool_feats[: min(len(pool_feats), 80)]
        if len(choices) < 5:
            continue
        for add_f in rng.choice(choices, size=min(10, len(choices)), replace=False):
            add_f = str(add_f)
            if add_f in base_feats:
                continue
            add_candidate(base_feats + [add_f], strategy="mutation", generation=parent["generation"] + 1, parent_id=parent["candidate_id"])

    # diversity filter + novelty
    ranked = rank_rows(candidates)
    selected: List[Dict] = []
    for _, r in ranked:
        feats = r["features"]
        if not selected:
            selected.append(r)
            continue
        max_j = max(jaccard(feats, s["features"]) for s in selected)
        if max_j >= diversity_jaccard_max:
            continue
        selected.append(r)
        if len(selected) >= shortlist_max:
            break

    for r in selected:
        if len(selected) == 1:
            r["novelty_score"] = 1.0
            continue
        others = [o for o in selected if o["candidate_id"] != r["candidate_id"]]
        mj = max(jaccard(r["features"], o["features"]) for o in others) if others else 0.0
        r["novelty_score"] = float(1.0 - mj)

    out = pd.DataFrame(selected)
    cols = [
        "candidate_id",
        "ticker",
        "dataset_key",
        "dataset_version",
        "features",
        "group_size",
        "strategy",
        "generation",
        "parent_id",
        "quick_metric",
        "quick_mean",
        "quick_std",
        "splits_meta",
        "novelty_score",
        "corr_penalty",
        "created_at",
    ]
    for c in cols:
        if c not in out.columns:
            out[c] = None
    return out[cols]
