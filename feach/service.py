import logging
from typing import Iterable, List

import numpy as np
import pandas as pd

from metrics.decorators import track_step

logger = logging.getLogger(__name__)

DEFAULT_RSI_PERIOD = 14
DEFAULT_ATR_PERIOD = 14
RET_VOL_WINDOW = 10
VOL_MEAN_WINDOW = 5
SMA_EMA_WINDOWS = (5, 10, 20)
LAG_COLUMNS = ("open", "high", "low", "close", "volume")


@track_step("rsi")
def _rsi(series: pd.Series, period: int = DEFAULT_RSI_PERIOD) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi


@track_step("atr")
def _atr(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    period: int = DEFAULT_ATR_PERIOD,
) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    atr = tr.rolling(period).mean()
    return atr


@track_step("add_lag_features")
def _add_lag_features(
    df: pd.DataFrame,
    columns: Iterable[str],
    n_lags: int,
    feature_cols: List[str],
) -> None:
    for col in columns:
        for lag in range(1, n_lags + 1):
            new_col = f"{col}_lag_{lag}"
            df[new_col] = df[col].shift(lag)
            feature_cols.append(new_col)


@track_step("engineer_features")
def engineer_features(
    df: pd.DataFrame,
    n_lags: int = 5,
    timestamp_col: str = "timestamp",
) -> pd.DataFrame:
    df = df.sort_values(timestamp_col).reset_index(drop=True).copy()
    df[timestamp_col] = pd.to_datetime(df[timestamp_col])

    feature_cols: List[str] = []

    _add_lag_features(df, LAG_COLUMNS, n_lags, feature_cols)

    df["log_ret"] = np.log(df["close"]).diff()
    df["log_ret_lag1"] = df["log_ret"].shift(1)
    df["vol_10"] = df["log_ret"].rolling(RET_VOL_WINDOW).std()
    feature_cols += ["log_ret_lag1", "vol_10"]

    for window in SMA_EMA_WINDOWS:
        sma_col = f"sma_{window}"
        ema_col = f"ema_{window}"
        ratio_col = f"ratio_sma_{window}"

        df[sma_col] = df["close"].rolling(window).mean()
        df[ema_col] = df["close"].ewm(span=window, adjust=False).mean()
        df[ratio_col] = df["close"] / df[sma_col] - 1

        feature_cols += [sma_col, ema_col, ratio_col]

    ema_12 = df["close"].ewm(span=12, adjust=False).mean()
    ema_26 = df["close"].ewm(span=26, adjust=False).mean()
    df["macd"] = ema_12 - ema_26
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
    df["macd_hist"] = df["macd"] - df["macd_signal"]
    feature_cols += ["macd", "macd_signal", "macd_hist"]

    df["rsi14"] = _rsi(df["close"], DEFAULT_RSI_PERIOD)
    feature_cols.append("rsi14")

    m20 = df["close"].rolling(20).mean()
    s20 = df["close"].rolling(20).std()

    df["bb_upper"] = m20 + 2 * s20
    df["bb_lower"] = m20 - 2 * s20
    df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / m20

    feature_cols += ["bb_upper", "bb_lower", "bb_width"]

    df["atr14"] = _atr(df["high"], df["low"], df["close"], DEFAULT_ATR_PERIOD)
    feature_cols.append("atr14")

    df["vol_mean5"] = df["volume"].rolling(VOL_MEAN_WINDOW).mean()
    df["vol_ratio5"] = df["volume"] / df["vol_mean5"] - 1
    feature_cols += ["vol_ratio5"]

    df["dow"] = df[timestamp_col].dt.weekday
    df["month"] = df[timestamp_col].dt.month

    df["dow_sin"] = np.sin(2 * np.pi * df["dow"] / 7)
    df["dow_cos"] = np.cos(2 * np.pi * df["dow"] / 7)
    df["mth_sin"] = np.sin(2 * np.pi * df["month"] / 12)
    df["mth_cos"] = np.cos(2 * np.pi * df["month"] / 12)

    feature_cols += ["dow_sin", "dow_cos", "mth_sin", "mth_cos"]
    df = df.dropna().reset_index(drop=True)

    df_features = df[feature_cols + ["close"]].copy()

    if not isinstance(df_features, pd.DataFrame):
        logger.error(f"Expected df_features to be a pandas DataFrame, got {type(df_features)}")
        raise ValueError("The df_features must be a pandas DataFrame")
    return df_features
