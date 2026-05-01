"""
StockIQ – Market Data REST API
GET /api/v1/market/quote/{exchange}/{symbol}   → single live quote
GET /api/v1/market/quotes                      → bulk quotes from cache
GET /api/v1/market/ohlcv/{exchange}/{symbol}   → historical OHLCV bars
GET /api/v1/market/instruments                 → searchable instrument list
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query

from core.config import settings
from core.redis_client import redis_pool
from core.db import db_pool
from models.schemas import Exchange, StockQuote, StockTick
from services.indicator_engine import compute_indicators
from services.classification_engine import classify_stock

logger = logging.getLogger(__name__)
router = APIRouter()


# ─────────────────────────────────────────────────────────
#  Single quote
# ─────────────────────────────────────────────────────────
@router.get("/quote/{exchange}/{symbol}", response_model=StockQuote)
async def get_quote(exchange: Exchange, symbol: str):
    """
    Returns an enriched quote for a single symbol.
    Reads latest price from Redis cache; computes indicators on demand.
    """
    symbol = symbol.upper()
    cache_key = f"quote:{exchange.value}:{symbol}"
    redis = redis_pool.client

    raw = await redis.get(cache_key)
    if not raw:
        raise HTTPException(status_code=404, detail=f"{exchange}:{symbol} not found in cache")

    tick_data = json.loads(raw)
    indicators = await compute_indicators(symbol, exchange.value)

    # Merge tick + indicators into quote
    quote_data = {**tick_data, **indicators, "name": "", "sector": ""}
    quote = StockQuote(**quote_data)
    tags = classify_stock(quote)
    return quote.model_copy(update={"tags": tags})


# ─────────────────────────────────────────────────────────
#  Bulk quotes
# ─────────────────────────────────────────────────────────
@router.get("/quotes", response_model=List[StockQuote])
async def get_bulk_quotes(
    symbols: str = Query(..., description="Comma-separated list e.g. NSE:RELIANCE,NSE:INFY"),
):
    """Returns quotes for multiple symbols from Redis cache."""
    pairs = [s.strip().upper() for s in symbols.split(",") if ":" in s]
    if not pairs:
        raise HTTPException(status_code=400, detail="Provide symbols as EXCHANGE:SYMBOL")
    if len(pairs) > 100:
        raise HTTPException(status_code=400, detail="Max 100 symbols per request")

    redis = redis_pool.client
    keys = [f"quote:{p.replace(':', ':', 1)}" for p in pairs]
    values = await redis.mget(keys)

    quotes: List[StockQuote] = []
    for raw in values:
        if raw:
            data = json.loads(raw)
            q = StockQuote(**{**data, "name": "", "sector": ""})
            quotes.append(q)

    return quotes


# ─────────────────────────────────────────────────────────
#  OHLCV history
# ─────────────────────────────────────────────────────────
@router.get("/ohlcv/{exchange}/{symbol}")
async def get_ohlcv(
    exchange: Exchange,
    symbol: str,
    interval: str = Query("5m", pattern="^(1m|5m|15m|1h|1d)$"),
    limit: int = Query(200, ge=1, le=1000),
):
    """
    Returns OHLCV bars from TimescaleDB continuous aggregates.
    Suitable for feeding charting libraries (TradingView, Recharts).
    """
    symbol = symbol.upper()
    interval_map = {
        "1m": "ohlcv_1m", "5m": "ohlcv_5m",
        "15m": "ohlcv_15m", "1h": "ohlcv_1h", "1d": "ohlcv_1d",
    }
    table = interval_map[interval]
    sql = f"""
        SELECT
            EXTRACT(EPOCH FROM time_bucket) * 1000 AS t,
            open, high, low, close, volume
        FROM {table}
        WHERE symbol = $1 AND exchange = $2
        ORDER BY time_bucket DESC
        LIMIT $3
    """
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(sql, symbol, exchange.value, limit)

    if not rows:
        raise HTTPException(status_code=404, detail="No OHLCV data found")

    # Return as list of lists [t, o, h, l, c, v] – compatible with most chart libs
    bars = [[r["t"], r["open"], r["high"], r["low"], r["close"], r["volume"]] for r in rows]
    bars.reverse()  # chronological order
    return {"symbol": symbol, "exchange": exchange.value, "interval": interval, "bars": bars}


# ─────────────────────────────────────────────────────────
#  Instrument search
# ─────────────────────────────────────────────────────────
@router.get("/instruments")
async def search_instruments(
    q: str = Query(..., min_length=1, max_length=50),
    exchange: Optional[Exchange] = None,
    limit: int = Query(20, ge=1, le=50),
):
    """Full-text search over instrument names and tickers."""
    sql = """
        SELECT symbol, exchange, name, sector, industry
        FROM instruments
        WHERE
            (symbol ILIKE $1 OR name ILIKE $1)
            {exchange_clause}
        ORDER BY
            CASE WHEN symbol ILIKE $2 THEN 0 ELSE 1 END,
            name
        LIMIT $3
    """
    pattern = f"%{q}%"
    exact_pattern = f"{q}%"
    exchange_clause = ""
    params = [pattern, exact_pattern, limit]

    if exchange:
        exchange_clause = "AND exchange = $4"
        params.append(exchange.value)

    sql = sql.format(exchange_clause=exchange_clause)
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)

    return [dict(r) for r in rows]