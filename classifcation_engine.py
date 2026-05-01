"""
StockIQ – Classification Engine
Applies rule-based (and optionally ML-based) classification tags
to enriched stock quotes.

Tag taxonomy:
  bullish_momentum     – RSI > 60, MACD hist > 0, slope_1d > 5°
  bearish_momentum     – RSI < 40, MACD hist < 0, slope_1d < -5°
  breakout_candidate   – price near/above BB upper, rel_volume > 1.5
  high_volatility      – vol_stddev > threshold or ATR/price > 2%
  low_liquidity        – volume < absolute threshold
  trend_continuation   – slope_1d and slope_1w same sign, RSI 45-65
  mean_reversion       – RSI extreme (< 30 or > 70), price at BB band
"""

from __future__ import annotations

import logging
from typing import List

from models.schemas import ClassificationTag, StockQuote

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────
#  Thresholds (tune per market conditions)
# ─────────────────────────────────────────────────────────
RSI_BULLISH_MIN = 55.0
RSI_BEARISH_MAX = 45.0
RSI_OVERBOUGHT = 70.0
RSI_OVERSOLD = 30.0

MACD_HIST_BULLISH = 0.0
MACD_HIST_BEARISH = 0.0

SLOPE_BULLISH_DEG = 3.0
SLOPE_BEARISH_DEG = -3.0

REL_VOLUME_BREAKOUT = 1.5
PRICE_BB_BREAKOUT_FACTOR = 0.98      # within 2% of BB upper

VOL_STDDEV_HIGH = 0.35               # annualised vol > 35%

LIQUIDITY_VOLUME_MIN = 50_000        # absolute minimum volume
ATR_TO_PRICE_HIGH = 0.025            # ATR/price > 2.5%


# ─────────────────────────────────────────────────────────
#  Tag evaluators
# ─────────────────────────────────────────────────────────
def _is_bullish_momentum(q: StockQuote) -> bool:
    rsi_ok = q.rsi_14 is not None and q.rsi_14 > RSI_BULLISH_MIN
    macd_ok = q.macd_hist is not None and q.macd_hist > MACD_HIST_BULLISH
    slope_ok = q.slope_1d is not None and q.slope_1d > SLOPE_BULLISH_DEG
    return sum([rsi_ok, macd_ok, slope_ok]) >= 2   # at least 2 of 3


def _is_bearish_momentum(q: StockQuote) -> bool:
    rsi_ok = q.rsi_14 is not None and q.rsi_14 < RSI_BEARISH_MAX
    macd_ok = q.macd_hist is not None and q.macd_hist < MACD_HIST_BEARISH
    slope_ok = q.slope_1d is not None and q.slope_1d < SLOPE_BEARISH_DEG
    return sum([rsi_ok, macd_ok, slope_ok]) >= 2


def _is_breakout_candidate(q: StockQuote) -> bool:
    vol_ok = q.rel_volume is not None and q.rel_volume > REL_VOLUME_BREAKOUT
    price_ok = (
        q.bb_upper is not None
        and q.ltp >= q.bb_upper * PRICE_BB_BREAKOUT_FACTOR
    )
    return vol_ok and price_ok


def _is_high_volatility(q: StockQuote) -> bool:
    if q.vol_stddev is not None and q.vol_stddev > VOL_STDDEV_HIGH:
        return True
    if q.atr_14 is not None and q.ltp > 0:
        return (q.atr_14 / q.ltp) > ATR_TO_PRICE_HIGH
    return False


def _is_low_liquidity(q: StockQuote) -> bool:
    return q.volume < LIQUIDITY_VOLUME_MIN


def _is_trend_continuation(q: StockQuote) -> bool:
    slopes_agree = (
        q.slope_1d is not None
        and q.slope_1w is not None
        and (
            (q.slope_1d > 0 and q.slope_1w > 0)
            or (q.slope_1d < 0 and q.slope_1w < 0)
        )
    )
    rsi_neutral = q.rsi_14 is not None and 45 <= q.rsi_14 <= 65
    return slopes_agree and rsi_neutral


def _is_mean_reversion(q: StockQuote) -> bool:
    rsi_extreme = q.rsi_14 is not None and (
        q.rsi_14 < RSI_OVERSOLD or q.rsi_14 > RSI_OVERBOUGHT
    )
    at_bb_band = False
    if q.bb_lower is not None and q.bb_upper is not None and q.ltp > 0:
        band_width = q.bb_upper - q.bb_lower
        if band_width > 0:
            dist_lower = abs(q.ltp - q.bb_lower) / band_width
            dist_upper = abs(q.ltp - q.bb_upper) / band_width
            at_bb_band = min(dist_lower, dist_upper) < 0.1
    return rsi_extreme or at_bb_band


# ─────────────────────────────────────────────────────────
#  Public API
# ─────────────────────────────────────────────────────────
def classify_stock(quote: StockQuote) -> List[ClassificationTag]:
    """
    Apply all classification rules to a StockQuote.
    Returns list of matching ClassificationTag enums.
    Bullish and bearish momentum are mutually exclusive (first match wins).
    """
    tags: List[ClassificationTag] = []

    if _is_bullish_momentum(quote):
        tags.append(ClassificationTag.BULLISH_MOMENTUM)
    elif _is_bearish_momentum(quote):
        tags.append(ClassificationTag.BEARISH_MOMENTUM)

    if _is_breakout_candidate(quote):
        tags.append(ClassificationTag.BREAKOUT_CANDIDATE)

    if _is_high_volatility(quote):
        tags.append(ClassificationTag.HIGH_VOLATILITY)

    if _is_low_liquidity(quote):
        tags.append(ClassificationTag.LOW_LIQUIDITY)

    if _is_trend_continuation(quote):
        tags.append(ClassificationTag.TREND_CONTINUATION)

    if _is_mean_reversion(quote):
        tags.append(ClassificationTag.MEAN_REVERSION)

    if not tags:
        tags.append(ClassificationTag.NEUTRAL)

    return tags


def classify_batch(quotes: List[StockQuote]) -> List[StockQuote]:
    """Classify a list of quotes in place and return them."""
    result = []
    for q in quotes:
        tags = classify_stock(q)
        result.append(q.model_copy(update={"tags": tags}))
    return result