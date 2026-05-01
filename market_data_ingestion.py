"""
StockIQ – Market Data Ingestion Service
Priority: Zerodha Kite Connect → Upstox → Yahoo Finance (fallback)

Architecture:
  KiteTicker (WS) ─► normalize() ─► Redis Pub/Sub ─► WS broadcast gateway
                                   ─► TimescaleDB   (tick persistence)
                                   ─► Redis Cache   (latest quote per symbol)
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import aiohttp
import pandas as pd
import yfinance as yf
from kiteconnect import KiteTicker

from core.config import settings
from core.redis_client import redis_pool
from core.db import db_pool
from models.schemas import Exchange, StockTick
from services.indicator_engine import IndicatorEngine

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────
#  Instrument registry  (symbol → instrument_token)
# ─────────────────────────────────────────────────────────
_NSE_PREFIX = "NSE:"
_BSE_PREFIX = "BSE:"


class InstrumentRegistry:
    """Loads all NSE/BSE instrument tokens from Kite and maintains mapping."""

    def __init__(self):
        self._token_to_symbol: Dict[int, str] = {}
        self._symbol_to_token: Dict[str, int] = {}
        self._loaded = False

    async def load(self, kite) -> None:
        """Fetch full instrument list from Kite REST API."""
        loop = asyncio.get_event_loop()
        instruments = await loop.run_in_executor(
            None, lambda: kite.instruments("NSE") + kite.instruments("BSE")
        )
        for inst in instruments:
            tok = inst["instrument_token"]
            sym = f"{inst['exchange']}:{inst['tradingsymbol']}"
            self._token_to_symbol[tok] = sym
            self._symbol_to_token[sym] = tok
        self._loaded = True
        logger.info(
            "Instrument registry loaded: %d instruments", len(self._token_to_symbol)
        )

    def get_symbol(self, token: int) -> Optional[str]:
        return self._token_to_symbol.get(token)

    def get_token(self, symbol: str) -> Optional[int]:
        return self._symbol_to_token.get(symbol)

    def get_all_tokens(self) -> List[int]:
        return list(self._token_to_symbol.keys())


# ─────────────────────────────────────────────────────────
#  Kite WebSocket adapter
# ─────────────────────────────────────────────────────────
class KiteIngestionAdapter:
    """
    Wraps KiteTicker in an asyncio-compatible interface.
    Publishes normalised ticks to Redis Pub/Sub channel.
    """

    def __init__(self, registry: InstrumentRegistry):
        self.registry = registry
        self._ticker: Optional[KiteTicker] = None
        self._running = False

    def _build_ticker(self) -> KiteTicker:
        kt = KiteTicker(settings.KITE_API_KEY, settings.KITE_ACCESS_TOKEN)
        kt.on_ticks = self._on_ticks
        kt.on_connect = self._on_connect
        kt.on_error = self._on_error
        kt.on_close = self._on_close
        return kt

    def _on_connect(self, ws, response):
        tokens = self.registry.get_all_tokens()
        # Kite allows max 3000 tokens per connection
        batch = tokens[:3000]
        ws.subscribe(batch)
        ws.set_mode(ws.MODE_FULL, batch)
        logger.info("Kite WS connected; subscribed to %d instruments", len(batch))

    def _on_ticks(self, ws, ticks: List[dict]):
        """Called by KiteTicker in its own thread; bridge to asyncio."""
        loop = asyncio.get_event_loop()
        asyncio.run_coroutine_threadsafe(
            self._process_ticks(ticks), loop
        )

    async def _process_ticks(self, ticks: List[dict]):
        pipeline = []
        for raw in ticks:
            symbol_full = self.registry.get_symbol(raw["instrument_token"])
            if not symbol_full:
                continue
            exchange, symbol = symbol_full.split(":", 1)
            tick = StockTick(
                symbol=symbol,
                exchange=Exchange(exchange),
                timestamp=raw.get("timestamp") or datetime.now(timezone.utc),
                ltp=float(raw.get("last_price", 0)),
                open=float(raw.get("ohlc", {}).get("open", 0)),
                high=float(raw.get("ohlc", {}).get("high", 0)),
                low=float(raw.get("ohlc", {}).get("low", 0)),
                close=float(raw.get("ohlc", {}).get("close", 0)),
                prev_close=float(raw.get("ohlc", {}).get("close", 0)),
                volume=int(raw.get("volume", 0)),
                avg_price=float(raw.get("average_price", 0)),
            )
            tick_with_change = _compute_pct_changes(tick)
            pipeline.append(tick_with_change)

        if pipeline:
            await _publish_ticks(pipeline)

    def _on_error(self, ws, code, reason):
        logger.error("Kite WS error %s: %s", code, reason)

    def _on_close(self, ws, code, reason):
        logger.warning("Kite WS closed %s: %s – will reconnect", code, reason)
        if self._running:
            asyncio.get_event_loop().call_later(5, self._ticker.connect)

    def start(self):
        self._running = True
        self._ticker = self._build_ticker()
        self._ticker.connect(threaded=True)

    def stop(self):
        self._running = False
        if self._ticker:
            self._ticker.close()


# ─────────────────────────────────────────────────────────
#  Yahoo Finance fallback (async polling)
# ─────────────────────────────────────────────────────────
class YahooFallbackPoller:
    """Polls Yahoo Finance for delayed data when Kite is unavailable."""

    SYMBOLS_NSE = ["RELIANCE.NS", "INFY.NS", "TCS.NS", "HDFC.NS", "ICICIBANK.NS"]

    def __init__(self):
        self._running = False

    async def start(self):
        self._running = True
        while self._running:
            try:
                await self._poll()
            except Exception as exc:
                logger.warning("Yahoo fallback poll error: %s", exc)
            await asyncio.sleep(settings.YAHOO_REFRESH_SECONDS)

    async def _poll(self):
        loop = asyncio.get_event_loop()
        tickers_str = " ".join(self.SYMBOLS_NSE)
        data = await loop.run_in_executor(
            None, lambda: yf.download(tickers_str, period="1d", interval="1m", progress=False)
        )
        if data.empty:
            return
        # Process and publish; simplified for brevity
        ticks = _df_to_ticks(data)
        await _publish_ticks(ticks)

    def stop(self):
        self._running = False


# ─────────────────────────────────────────────────────────
#  Normalizer helpers
# ─────────────────────────────────────────────────────────
def _compute_pct_changes(tick: StockTick) -> StockTick:
    """
    Compute intraday change %.
    Rolling 1m/5m/15m changes are computed from Redis time-series snapshots.
    """
    if tick.prev_close and tick.prev_close > 0:
        day_chg = round((tick.ltp - tick.prev_close) / tick.prev_close * 100, 4)
    else:
        day_chg = 0.0

    return tick.model_copy(update={"change_pct_day": day_chg})


async def _compute_rolling_pct(symbol: str, exchange: str, ltp: float) -> Dict:
    """
    Compute 1m/5m/15m % change from cached price snapshots in Redis.
    Key pattern: snapshot:{exchange}:{symbol}:{N}m
    """
    redis = redis_pool.client
    results = {}
    for minutes in [1, 5, 15]:
        key = f"snapshot:{exchange}:{symbol}:{minutes}m"
        old_price_raw = await redis.get(key)
        if old_price_raw:
            old_price = float(old_price_raw)
            if old_price > 0:
                results[f"change_pct_{minutes}m"] = round(
                    (ltp - old_price) / old_price * 100, 4
                )
        # Store current price as new snapshot every N minutes
        # (managed separately by SnapshotScheduler)
    return results


def _df_to_ticks(data: pd.DataFrame) -> List[StockTick]:
    """Convert yfinance DataFrame into StockTick list."""
    ticks = []
    # simplified; real impl handles multi-index yf output
    return ticks


# ─────────────────────────────────────────────────────────
#  Redis publish
# ─────────────────────────────────────────────────────────
async def _publish_ticks(ticks: List[StockTick]):
    redis = redis_pool.client
    pipe = redis.pipeline()
    channel = settings.REDIS_MARKET_CHANNEL

    for tick in ticks:
        payload = tick.model_dump_json()
        # Pub/Sub for WS broadcast
        pipe.publish(channel, payload)
        # Latest quote cache (TTL = 10s to survive brief reconnects)
        cache_key = f"quote:{tick.exchange}:{tick.symbol}"
        pipe.setex(cache_key, 10, payload)

    await pipe.execute()

    # Persist to TimescaleDB asynchronously
    asyncio.create_task(_persist_ticks(ticks))


async def _persist_ticks(ticks: List[StockTick]):
    """Bulk-insert ticks into TimescaleDB ticks hypertable."""
    if not ticks:
        return
    rows = [
        (
            t.timestamp, t.symbol, t.exchange.value,
            t.ltp, t.open, t.high, t.low, t.close,
            t.volume, t.change_pct_day,
        )
        for t in ticks
    ]
    sql = """
        INSERT INTO ticks (time, symbol, exchange, ltp, open, high, low, close, volume, change_pct_day)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        ON CONFLICT DO NOTHING
    """
    async with db_pool.acquire() as conn:
        await conn.executemany(sql, rows)


# ─────────────────────────────────────────────────────────
#  Top-level service class
# ─────────────────────────────────────────────────────────
class MarketDataIngestionService:
    """Orchestrates all ingestion adapters with fallback logic."""

    def __init__(self):
        self.registry = InstrumentRegistry()
        self.kite_adapter: Optional[KiteIngestionAdapter] = None
        self.yahoo_poller = YahooFallbackPoller()
        self._running = False

    async def start(self):
        self._running = True
        kite_ok = False

        if settings.KITE_API_KEY and settings.KITE_ACCESS_TOKEN:
            try:
                from kiteconnect import KiteConnect
                kite = KiteConnect(api_key=settings.KITE_API_KEY)
                kite.set_access_token(settings.KITE_ACCESS_TOKEN)
                await self.registry.load(kite)
                self.kite_adapter = KiteIngestionAdapter(self.registry)
                self.kite_adapter.start()
                kite_ok = True
                logger.info("Kite Connect ingestion active")
            except Exception as exc:
                logger.warning("Kite Connect unavailable: %s – switching to fallback", exc)

        if not kite_ok and settings.YAHOO_FALLBACK_ENABLED:
            logger.info("Using Yahoo Finance fallback poller")
            asyncio.create_task(self.yahoo_poller.start())

    async def stop(self):
        self._running = False
        if self.kite_adapter:
            self.kite_adapter.stop()
        self.yahoo_poller.stop()