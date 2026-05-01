"""
StockIQ – Typed Data Schemas (Pydantic v2)
All public-facing data contracts live here.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional, List

from pydantic import BaseModel, Field, ConfigDict


# ─────────────────────────────────────────────────────────
#  Enumerations
# ─────────────────────────────────────────────────────────
class Exchange(str, Enum):
    NSE = "NSE"
    BSE = "BSE"


class Sentiment(str, Enum):
    POSITIVE = "positive"
    NEGATIVE = "negative"
    NEUTRAL = "neutral"


class NewsSource(str, Enum):
    DOMESTIC = "domestic"
    INTERNATIONAL = "international"


class ClassificationTag(str, Enum):
    BULLISH_MOMENTUM = "bullish_momentum"
    BEARISH_MOMENTUM = "bearish_momentum"
    BREAKOUT_CANDIDATE = "breakout_candidate"
    HIGH_VOLATILITY = "high_volatility"
    LOW_LIQUIDITY = "low_liquidity"
    TREND_CONTINUATION = "trend_continuation"
    MEAN_REVERSION = "mean_reversion_candidate"
    NEUTRAL = "neutral"


class PriceRange(str, Enum):
    MICRO = "0-20"
    SMALL = "20-100"
    MID = "100-500"
    LARGE = "500-1000"
    CUSTOM = "custom"


# ─────────────────────────────────────────────────────────
#  Live Tick / Quote
# ─────────────────────────────────────────────────────────
class StockTick(BaseModel):
    """Single real-time tick from the feed"""
    model_config = ConfigDict(frozen=True)

    symbol: str
    exchange: Exchange
    timestamp: datetime
    ltp: float                          # last traded price
    open: float
    high: float
    low: float
    close: float
    prev_close: float
    volume: int
    avg_price: float = 0.0

    # pre-computed change fields (filled by normalizer)
    change_pct_day: float = 0.0
    change_pct_1m: float = 0.0
    change_pct_5m: float = 0.0
    change_pct_15m: float = 0.0


class StockQuote(BaseModel):
    """Enriched quote with indicators and classification"""
    model_config = ConfigDict(from_attributes=True)

    symbol: str
    exchange: Exchange
    name: str = ""
    sector: str = ""
    timestamp: datetime

    ltp: float
    open: float
    high: float
    low: float
    close: float
    prev_close: float
    volume: int

    change_pct_day: float
    change_pct_1m: float = 0.0
    change_pct_5m: float = 0.0
    change_pct_15m: float = 0.0

    # Technical indicators
    rsi_14: Optional[float] = None
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_hist: Optional[float] = None
    sma_20: Optional[float] = None
    sma_50: Optional[float] = None
    ema_20: Optional[float] = None
    atr_14: Optional[float] = None
    bb_upper: Optional[float] = None
    bb_lower: Optional[float] = None
    vol_stddev: Optional[float] = None  # rolling volatility
    rel_volume: Optional[float] = None  # vol / avg_vol_20d
    slope_1d: Optional[float] = None    # price slope theta, 1-day
    slope_1w: Optional[float] = None    # price slope theta, 1-week

    # Classification
    tags: List[ClassificationTag] = []


# ─────────────────────────────────────────────────────────
#  Screener Filter Request
# ─────────────────────────────────────────────────────────
class ScreenerFilter(BaseModel):
    exchange: Optional[Exchange] = None

    # Price range
    price_min: Optional[float] = Field(None, ge=0)
    price_max: Optional[float] = Field(None, ge=0)
    price_range: Optional[PriceRange] = None

    # Momentum
    change_pct_min: Optional[float] = None  # e.g. +2 for gainers
    change_pct_max: Optional[float] = None
    window: str = "day"                      # "1m", "5m", "15m", "day"

    # Volume / liquidity
    volume_min: Optional[int] = None
    rel_volume_min: Optional[float] = None

    # RSI
    rsi_min: Optional[float] = Field(None, ge=0, le=100)
    rsi_max: Optional[float] = Field(None, ge=0, le=100)

    # MACD (positive = bullish, negative = bearish)
    macd_hist_min: Optional[float] = None
    macd_hist_max: Optional[float] = None

    # ATR / volatility
    atr_min: Optional[float] = None
    atr_max: Optional[float] = None

    # Classification tags (OR logic within list)
    tags: Optional[List[ClassificationTag]] = None

    # Pagination
    limit: int = Field(100, ge=1, le=500)
    offset: int = Field(0, ge=0)
    sort_by: str = "change_pct_day"
    sort_desc: bool = True


class ScreenerResult(BaseModel):
    total: int
    stocks: List[StockQuote]
    generated_at: datetime


# ─────────────────────────────────────────────────────────
#  News
# ─────────────────────────────────────────────────────────
class NewsArticle(BaseModel):
    id: str
    headline: str
    source: str
    source_type: NewsSource
    url: str
    published_at: datetime
    sentiment: Sentiment = Sentiment.NEUTRAL
    symbols: List[str] = []             # related tickers if detected


class NewsResponse(BaseModel):
    articles: List[NewsArticle]
    fetched_at: datetime


# ─────────────────────────────────────────────────────────
#  WebSocket messages
# ─────────────────────────────────────────────────────────
class WSSubscribeMsg(BaseModel):
    action: str                         # "subscribe" | "unsubscribe"
    symbols: List[str]


class WSTickMsg(BaseModel):
    type: str = "tick"
    data: StockTick


class WSErrorMsg(BaseModel):
    type: str = "error"
    message: str


# ─────────────────────────────────────────────────────────
#  Analytics (R / Julia service responses)
# ─────────────────────────────────────────────────────────
class VolatilityCluster(BaseModel):
    symbol: str
    regime: str                         # "low" | "medium" | "high"
    garch_sigma: float
    ewm_vol: float


class QuantileStats(BaseModel):
    symbol: str
    p5: float
    p25: float
    median: float
    p75: float
    p95: float
    skewness: float
    kurtosis: float


class SimulationResult(BaseModel):
    symbol: str
    paths: int
    horizon_days: int
    expected_return: float
    var_95: float
    cvar_95: float
    prob_profit: float