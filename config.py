"""
StockIQ – Application Configuration
All settings are read from environment variables or .env file.
"""

from functools import lru_cache
from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # ── App ───────────────────────────────────────────────
    APP_ENV: str = "development"
    SECRET_KEY: str = "change-me-in-production"
    ALLOWED_ORIGINS: List[str] = ["http://localhost:3000", "https://stockiq.app"]

    # ── PostgreSQL / TimescaleDB ──────────────────────────
    DATABASE_URL: str = (
        "postgresql+asyncpg://stockiq:stockiq@localhost:5432/stockiq"
    )
    DB_MIN_POOL: int = 5
    DB_MAX_POOL: int = 20

    # ── Redis ─────────────────────────────────────────────
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_MARKET_CHANNEL: str = "market:ticks"
    REDIS_NEWS_CHANNEL: str = "news:feed"
    CACHE_TTL_SECONDS: int = 5

    # ── Kafka (optional; falls back to Redis Streams) ─────
    USE_KAFKA: bool = False
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC_TICKS: str = "stock.ticks"
    KAFKA_TOPIC_NEWS: str = "market.news"

    # ── Zerodha Kite Connect ──────────────────────────────
    KITE_API_KEY: str = ""
    KITE_API_SECRET: str = ""
    KITE_ACCESS_TOKEN: str = ""          # refresh via OAuth daily

    # ── Upstox ───────────────────────────────────────────
    UPSTOX_API_KEY: str = ""
    UPSTOX_API_SECRET: str = ""
    UPSTOX_ACCESS_TOKEN: str = ""

    # ── Yahoo Finance fallback ────────────────────────────
    YAHOO_FALLBACK_ENABLED: bool = True
    YAHOO_REFRESH_SECONDS: int = 15      # delayed / backup

    # ── News sources ──────────────────────────────────────
    NEWS_REFRESH_SECONDS: int = 120
    NEWS_MAX_ITEMS: int = 200
    GNEWS_API_KEY: str = ""              # GNews or NewsAPI
    NEWSAPI_KEY: str = ""

    # ── R analytics microservice ──────────────────────────
    R_SERVICE_URL: str = "http://localhost:8001"
    R_SERVICE_TIMEOUT: float = 10.0

    # ── Julia compute microservice ────────────────────────
    JULIA_SERVICE_URL: str = "http://localhost:8002"
    JULIA_SERVICE_TIMEOUT: float = 30.0

    # ── TimescaleDB-specific ──────────────────────────────
    TS_RETENTION_DAYS: int = 365         # hypertable retention policy
    TS_CHUNK_INTERVAL: str = "1 day"


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()