"""
StockIQ – Screener API Router
POST /api/v1/screener/screen  → returns filtered, enriched, classified stocks
GET  /api/v1/screener/top-gainers
GET  /api/v1/screener/top-losers
GET  /api/v1/screener/high-volume
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from core.config import settings
from core.redis_client import redis_pool
from core.db import db_pool
from models.schemas import (
    ClassificationTag,
    Exchange,
    PriceRange,
    ScreenerFilter,
    ScreenerResult,
    StockQuote,
)
from services.indicator_engine import compute_indicators_batch
from services.classification_engine import classify_batch

logger = logging.getLogger(__name__)
router = APIRouter()

# Price range presets (min_price, max_price)
PRICE_RANGE_MAP = {
    PriceRange.MICRO:  (0.0,    20.0),
    PriceRange.SMALL:  (20.0,   100.0),
    PriceRange.MID:    (100.0,  500.0),
    PriceRange.LARGE:  (500.0,  1000.0),
}


# ─────────────────────────────────────────────────────────
#  SQL builder
# ─────────────────────────────────────────────────────────
def _build_query(f: ScreenerFilter) -> tuple[str, list]:
    """
    Build parameterised SQL from ScreenerFilter.
    Uses the `latest_quotes` materialised view (refreshed every 5s by bg job).
    """
    conditions: List[str] = []
    params: List = []
    idx = 1

    if f.exchange:
        conditions.append(f"exchange = ${idx}")
        params.append(f.exchange.value)
        idx += 1

    # ── Price ─────────────────────────────────────────────
    p_min, p_max = f.price_min, f.price_max
    if f.price_range and f.price_range != PriceRange.CUSTOM:
        p_min, p_max = PRICE_RANGE_MAP[f.price_range]

    if p_min is not None:
        conditions.append(f"ltp >= ${idx}")
        params.append(p_min)
        idx += 1
    if p_max is not None:
        conditions.append(f"ltp <= ${idx}")
        params.append(p_max)
        idx += 1

    # ── Momentum / change % ────────────────────────────────
    change_col = {
        "1m":  "change_pct_1m",
        "5m":  "change_pct_5m",
        "15m": "change_pct_15m",
        "day": "change_pct_day",
    }.get(f.window, "change_pct_day")

    if f.change_pct_min is not None:
        conditions.append(f"{change_col} >= ${idx}")
        params.append(f.change_pct_min)
        idx += 1
    if f.change_pct_max is not None:
        conditions.append(f"{change_col} <= ${idx}")
        params.append(f.change_pct_max)
        idx += 1

    # ── Volume ────────────────────────────────────────────
    if f.volume_min is not None:
        conditions.append(f"volume >= ${idx}")
        params.append(f.volume_min)
        idx += 1

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    # ── Sort ──────────────────────────────────────────────
    safe_sort = {
        "change_pct_day", "change_pct_5m", "change_pct_15m",
        "ltp", "volume", "rsi_14", "atr_14", "rel_volume",
    }
    sort_col = f.sort_by if f.sort_by in safe_sort else "change_pct_day"
    sort_dir = "DESC" if f.sort_desc else "ASC"

    sql = f"""
        SELECT
            symbol, exchange, name, sector,
            ltp, open, high, low, close, prev_close, volume,
            change_pct_day, change_pct_1m, change_pct_5m, change_pct_15m,
            rsi_14, macd, macd_signal, macd_hist,
            sma_20, sma_50, ema_20, atr_14,
            bb_upper, bb_lower, vol_stddev, rel_volume,
            slope_1d, slope_1w,
            updated_at
        FROM latest_quotes
        {where}
        ORDER BY {sort_col} {sort_dir} NULLS LAST
        LIMIT ${idx} OFFSET ${idx+1}
    """
    params.extend([f.limit, f.offset])

    count_sql = f"SELECT COUNT(*) FROM latest_quotes {where}"
    return sql, count_sql, params


# ─────────────────────────────────────────────────────────
#  Post-SQL filters (indicators not in materialized view)
# ─────────────────────────────────────────────────────────
def _apply_post_filters(quotes: List[StockQuote], f: ScreenerFilter) -> List[StockQuote]:
    result = []
    for q in quotes:
        if f.rsi_min is not None and (q.rsi_14 is None or q.rsi_14 < f.rsi_min):
            continue
        if f.rsi_max is not None and (q.rsi_14 is None or q.rsi_14 > f.rsi_max):
            continue
        if f.macd_hist_min is not None and (q.macd_hist is None or q.macd_hist < f.macd_hist_min):
            continue
        if f.macd_hist_max is not None and (q.macd_hist is None or q.macd_hist > f.macd_hist_max):
            continue
        if f.atr_min is not None and (q.atr_14 is None or q.atr_14 < f.atr_min):
            continue
        if f.atr_max is not None and (q.atr_14 is None or q.atr_14 > f.atr_max):
            continue
        if f.rel_volume_min is not None and (q.rel_volume is None or q.rel_volume < f.rel_volume_min):
            continue
        if f.tags:
            if not any(tag in q.tags for tag in f.tags):
                continue
        result.append(q)
    return result


# ─────────────────────────────────────────────────────────
#  Endpoints
# ─────────────────────────────────────────────────────────
@router.post("/screen", response_model=ScreenerResult)
async def screen_stocks(filters: ScreenerFilter):
    """
    Main screener endpoint.
    Accepts a ScreenerFilter body and returns enriched, classified stocks.
    """
    sql, count_sql, params = _build_query(filters)

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)
        count_row = await conn.fetchrow(count_sql, *params[:-2])

    total = count_row["count"] if count_row else 0
    quotes = [StockQuote(**dict(r), timestamp=r["updated_at"]) for r in rows]

    # Classify
    quotes = classify_batch(quotes)

    # Post-filter on tag / indicator fields
    quotes = _apply_post_filters(quotes, filters)

    return ScreenerResult(
        total=total,
        stocks=quotes,
        generated_at=datetime.now(timezone.utc),
    )


@router.get("/top-gainers", response_model=ScreenerResult)
async def top_gainers(
    exchange: Optional[Exchange] = None,
    window: str = Query("day", pattern="^(1m|5m|15m|day)$"),
    limit: int = Query(20, ge=1, le=100),
):
    """Top N gainers by % change in the specified window."""
    filters = ScreenerFilter(
        exchange=exchange,
        change_pct_min=0.0,
        window=window,
        sort_by=f"change_pct_{window}" if window != "day" else "change_pct_day",
        sort_desc=True,
        limit=limit,
    )
    return await screen_stocks(filters)


@router.get("/top-losers", response_model=ScreenerResult)
async def top_losers(
    exchange: Optional[Exchange] = None,
    window: str = Query("day", pattern="^(1m|5m|15m|day)$"),
    limit: int = Query(20, ge=1, le=100),
):
    """Top N losers by % change."""
    filters = ScreenerFilter(
        exchange=exchange,
        change_pct_max=0.0,
        window=window,
        sort_by=f"change_pct_{window}" if window != "day" else "change_pct_day",
        sort_desc=False,
        limit=limit,
    )
    return await screen_stocks(filters)


@router.get("/high-volume", response_model=ScreenerResult)
async def high_volume(
    exchange: Optional[Exchange] = None,
    rel_volume_min: float = Query(2.0, ge=1.0),
    limit: int = Query(20, ge=1, le=100),
):
    """Stocks with unusual volume (relative to 20-day average)."""
    filters = ScreenerFilter(
        exchange=exchange,
        rel_volume_min=rel_volume_min,
        sort_by="rel_volume",
        sort_desc=True,
        limit=limit,
    )
    return await screen_stocks(filters)