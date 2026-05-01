"""
StockIQ – Production FastAPI Application Entry Point
Python 3.11+ | FastAPI | Async Redis | WebSockets | TimescaleDB
"""

import logging
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from core.config import settings
from core.redis_client import redis_pool
from core.db import db_pool
from api import market, screener, news, ws, analytics
from services.market_data_ingestion import MarketDataIngestionService
from services.news_ingestion import NewsIngestionService

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────
#  Lifespan: startup + shutdown
# ─────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── startup ──────────────────────────────────────────
    logger.info("Starting StockIQ backend…")

    # 1. DB connection pool
    await db_pool.connect()
    logger.info("PostgreSQL/TimescaleDB pool connected")

    # 2. Redis connection pool
    await redis_pool.connect()
    logger.info("Redis pool connected")

    # 3. Market data ingestion (Kite → Redis pub/sub → WS broadcast)
    app.state.market_ingestion = MarketDataIngestionService()
    asyncio.create_task(app.state.market_ingestion.start())
    logger.info("Market data ingestion service started")

    # 4. News ingestion (periodic fetch + sentiment tagging)
    app.state.news_ingestion = NewsIngestionService()
    asyncio.create_task(app.state.news_ingestion.start())
    logger.info("News ingestion service started")

    yield

    # ── shutdown ─────────────────────────────────────────
    logger.info("Shutting down StockIQ backend…")
    await app.state.market_ingestion.stop()
    await app.state.news_ingestion.stop()
    await db_pool.disconnect()
    await redis_pool.disconnect()
    logger.info("Clean shutdown complete")


# ─────────────────────────────────────────────────────────
#  App factory
# ─────────────────────────────────────────────────────────
def create_app() -> FastAPI:
    app = FastAPI(
        title="StockIQ – Indian Market Intelligence Platform",
        description="Real-time NSE/BSE screener, analytics & news",
        version="1.0.0",
        lifespan=lifespan,
        docs_url="/api/docs",
        redoc_url="/api/redoc",
    )

    # ── Middleware ────────────────────────────────────────
    app.add_middleware(GZipMiddleware, minimum_size=1000)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.ALLOWED_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ── Routers ───────────────────────────────────────────
    app.include_router(market.router,    prefix="/api/v1/market",    tags=["Market Data"])
    app.include_router(screener.router,  prefix="/api/v1/screener",  tags=["Screener"])
    app.include_router(news.router,      prefix="/api/v1/news",      tags=["News"])
    app.include_router(analytics.router, prefix="/api/v1/analytics", tags=["Analytics"])
    app.include_router(ws.router,        prefix="/ws",               tags=["WebSocket"])

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        workers=1,           # single worker; use gunicorn+uvicorn for prod
        log_level="info",
    )