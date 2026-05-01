"""
StockIQ – Indicator Engine
Computes RSI, MACD, SMA, EMA, ATR, Bollinger Bands, slope/theta
on OHLCV DataFrames fetched from TimescaleDB.

Design:
  - All calculations are vectorised (NumPy/pandas).
  - Results are cached in Redis with a configurable TTL.
  - Called by the screener before returning stock quotes.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
from typing import Dict, Optional

import numpy as np
import pandas as pd
import pandas_ta as ta  # pip install pandas-ta

from core.config import settings
from core.redis_client import redis_pool
from core.db import db_pool

logger = logging.getLogger(__name__)

INDICATOR_CACHE_TTL = 60  # seconds


# ─────────────────────────────────────────────────────────
#  OHLCV loader from TimescaleDB
# ─────────────────────────────────────────────────────────
async def load_ohlcv(
    symbol: str,
    exchange: str,
    period: str = "1d",
    interval: str = "5m",
    limit: int = 300,
) -> pd.DataFrame:
    """
    Load OHLCV bars from TimescaleDB continuous aggregates.
    period: "1d" | "1w"
    interval: "1m" | "5m" | "15m" | "1h" | "1d"
    """
    interval_map = {
        "1m": "ohlcv_1m",
        "5m": "ohlcv_5m",
        "15m": "ohlcv_15m",
        "1h": "ohlcv_1h",
        "1d": "ohlcv_1d",
    }
    table = interval_map.get(interval, "ohlcv_5m")

    sql = f"""
        SELECT time_bucket AS ts, open, high, low, close, volume
        FROM {table}
        WHERE symbol = $1 AND exchange = $2
        ORDER BY time_bucket DESC
        LIMIT $3
    """
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(sql, symbol, exchange, limit)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    df["ts"] = pd.to_datetime(df["ts"])
    df = df.sort_values("ts").reset_index(drop=True)
    df = df.astype(
        {"open": float, "high": float, "low": float, "close": float, "volume": float}
    )
    return df


# ─────────────────────────────────────────────────────────
#  Core indicator calculations
# ─────────────────────────────────────────────────────────
def compute_rsi(df: pd.DataFrame, period: int = 14) -> Optional[float]:
    if len(df) < period + 1:
        return None
    rsi_series = ta.rsi(df["close"], length=period)
    val = rsi_series.dropna()
    return round(float(val.iloc[-1]), 2) if not val.empty else None


def compute_macd(
    df: pd.DataFrame,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> Dict[str, Optional[float]]:
    if len(df) < slow + signal:
        return {"macd": None, "signal": None, "hist": None}
    result = ta.macd(df["close"], fast=fast, slow=slow, signal=signal)
    if result is None or result.empty:
        return {"macd": None, "signal": None, "hist": None}
    last = result.iloc[-1]
    return {
        "macd": round(float(last[f"MACD_{fast}_{slow}_{signal}"]), 4),
        "signal": round(float(last[f"MACDs_{fast}_{slow}_{signal}"]), 4),
        "hist": round(float(last[f"MACDh_{fast}_{slow}_{signal}"]), 4),
    }


def compute_sma(df: pd.DataFrame, period: int = 20) -> Optional[float]:
    if len(df) < period:
        return None
    sma = ta.sma(df["close"], length=period)
    val = sma.dropna()
    return round(float(val.iloc[-1]), 4) if not val.empty else None


def compute_ema(df: pd.DataFrame, period: int = 20) -> Optional[float]:
    if len(df) < period:
        return None
    ema = ta.ema(df["close"], length=period)
    val = ema.dropna()
    return round(float(val.iloc[-1]), 4) if not val.empty else None


def compute_atr(df: pd.DataFrame, period: int = 14) -> Optional[float]:
    if len(df) < period + 1:
        return None
    atr = ta.atr(df["high"], df["low"], df["close"], length=period)
    val = atr.dropna()
    return round(float(val.iloc[-1]), 4) if not val.empty else None


def compute_bollinger(
    df: pd.DataFrame, period: int = 20, std: float = 2.0
) -> Dict[str, Optional[float]]:
    if len(df) < period:
        return {"upper": None, "lower": None}
    bb = ta.bbands(df["close"], length=period, std=std)
    if bb is None or bb.empty:
        return {"upper": None, "lower": None}
    last = bb.iloc[-1]
    return {
        "upper": round(float(last[f"BBU_{period}_{std}"]), 4),
        "lower": round(float(last[f"BBL_{period}_{std}"]), 4),
    }


def compute_rolling_volatility(df: pd.DataFrame, period: int = 20) -> Optional[float]:
    """Annualised rolling std-dev of log returns."""
    if len(df) < period + 1:
        return None
    log_returns = np.log(df["close"] / df["close"].shift(1)).dropna()
    if len(log_returns) < period:
        return None
    vol = float(log_returns.rolling(period).std().iloc[-1])
    # annualise assuming 252 trading days, ~78 5-min bars/day
    return round(vol * math.sqrt(252 * 78), 4)


def compute_relative_volume(df: pd.DataFrame, period: int = 20) -> Optional[float]:
    """Current bar volume / mean of last N bars."""
    if len(df) < period + 1:
        return None
    avg_vol = float(df["volume"].iloc[-(period + 1): -1].mean())
    cur_vol = float(df["volume"].iloc[-1])
    if avg_vol == 0:
        return None
    return round(cur_vol / avg_vol, 4)


def compute_slope_theta(df: pd.DataFrame, lookback: int = 20) -> Optional[float]:
    """
    Linear regression slope of closing prices over `lookback` bars,
    expressed as angle in degrees (theta).
    """
    if len(df) < lookback:
        return None
    prices = df["close"].iloc[-lookback:].values
    x = np.arange(lookback, dtype=float)
    # normalise x to [0,1] so slope is in price units / bar
    slope = float(np.polyfit(x, prices, 1)[0])
    # convert slope to angle
    theta = math.degrees(math.atan(slope / (prices.mean() + 1e-9)))
    return round(theta, 4)


# ─────────────────────────────────────────────────────────
#  Full indicator bundle for one symbol
# ─────────────────────────────────────────────────────────
async def compute_indicators(symbol: str, exchange: str) -> Dict:
    """
    Compute all indicators for a symbol, with Redis caching.
    Returns a flat dict to be merged into StockQuote.
    """
    cache_key = f"indicators:{exchange}:{symbol}"
    redis = redis_pool.client

    # Cache hit
    cached = await redis.get(cache_key)
    if cached:
        return json.loads(cached)

    # Load OHLCV data (two resolutions)
    df_5m = await load_ohlcv(symbol, exchange, interval="5m", limit=300)
    df_1d = await load_ohlcv(symbol, exchange, interval="1d", limit=60)

    if df_5m.empty:
        return {}

    macd_vals = compute_macd(df_5m)
    bb_vals = compute_bollinger(df_5m)

    result = {
        "rsi_14": compute_rsi(df_5m),
        "macd": macd_vals["macd"],
        "macd_signal": macd_vals["signal"],
        "macd_hist": macd_vals["hist"],
        "sma_20": compute_sma(df_5m, 20),
        "sma_50": compute_sma(df_5m, 50),
        "ema_20": compute_ema(df_5m, 20),
        "atr_14": compute_atr(df_5m),
        "bb_upper": bb_vals["upper"],
        "bb_lower": bb_vals["lower"],
        "vol_stddev": compute_rolling_volatility(df_5m),
        "rel_volume": compute_relative_volume(df_5m),
        "slope_1d": compute_slope_theta(df_5m, lookback=78),    # ~1 day of 5m bars
        "slope_1w": compute_slope_theta(df_1d, lookback=5),     # 5 daily bars
    }

    # Cache for 60 seconds
    await redis.setex(cache_key, INDICATOR_CACHE_TTL, json.dumps(result))
    return result


# ─────────────────────────────────────────────────────────
#  Batch computation for screener
# ─────────────────────────────────────────────────────────
async def compute_indicators_batch(
    symbols: list[tuple[str, str]]  # [(symbol, exchange), ...]
) -> Dict[str, Dict]:
    """
    Compute indicators for multiple symbols concurrently.
    Returns dict keyed by "EXCHANGE:SYMBOL".
    """
    tasks = {
        f"{exchange}:{symbol}": compute_indicators(symbol, exchange)
        for symbol, exchange in symbols
    }
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)
    return {
        key: (result if not isinstance(result, Exception) else {})
        for key, result in zip(tasks.keys(), results)
    }