"""
StockIQ – News Ingestion Service
Fetches headlines from domestic (ET, Moneycontrol, Mint) and
international (Bloomberg RSS, Reuters, CNBC) sources.
Applies lightweight lexicon-based sentiment tagging.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from typing import List, Optional

import aiohttp
import feedparser

from core.config import settings
from core.redis_client import redis_pool
from models.schemas import NewsArticle, NewsSource, Sentiment

logger = logging.getLogger(__name__)

NEWS_CACHE_KEY = "news:articles"
NEWS_CACHE_TTL = settings.NEWS_REFRESH_SECONDS * 2


# ─────────────────────────────────────────────────────────
#  RSS feed registry
# ─────────────────────────────────────────────────────────
RSS_FEEDS = [
    # Domestic
    {
        "name": "Economic Times – Markets",
        "url": "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
        "source_type": NewsSource.DOMESTIC,
    },
    {
        "name": "Moneycontrol",
        "url": "https://www.moneycontrol.com/rss/marketreports.xml",
        "source_type": NewsSource.DOMESTIC,
    },
    {
        "name": "Mint – Markets",
        "url": "https://www.livemint.com/rss/markets",
        "source_type": NewsSource.DOMESTIC,
    },
    # International
    {
        "name": "Reuters – Business",
        "url": "https://feeds.reuters.com/reuters/businessNews",
        "source_type": NewsSource.INTERNATIONAL,
    },
    {
        "name": "CNBC",
        "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664",
        "source_type": NewsSource.INTERNATIONAL,
    },
]


# ─────────────────────────────────────────────────────────
#  Sentiment lexicon (simplified; replace with transformers)
# ─────────────────────────────────────────────────────────
_POSITIVE_WORDS = {
    "surge", "rally", "gain", "rise", "jump", "record", "high", "beat",
    "profit", "growth", "strong", "bullish", "upgrade", "outperform",
    "recover", "soar", "boost", "positive", "upbeat", "expand",
}
_NEGATIVE_WORDS = {
    "fall", "drop", "crash", "decline", "loss", "low", "miss", "cut",
    "bearish", "downgrade", "underperform", "weak", "debt", "default",
    "plunge", "concern", "risk", "sell-off", "negative", "contract",
}


def _score_sentiment(headline: str) -> Sentiment:
    words = set(re.findall(r"\b\w+\b", headline.lower()))
    pos = len(words & _POSITIVE_WORDS)
    neg = len(words & _NEGATIVE_WORDS)
    if pos > neg:
        return Sentiment.POSITIVE
    if neg > pos:
        return Sentiment.NEGATIVE
    return Sentiment.NEUTRAL


def _extract_symbols(headline: str) -> List[str]:
    """
    Crude regex for Indian stock tickers mentioned in headlines.
    Real implementation: NER model or symbol dictionary lookup.
    """
    # Match uppercase words of 2-10 chars that could be tickers
    candidates = re.findall(r"\b([A-Z]{2,10})\b", headline)
    return candidates[:5]  # cap at 5 to avoid noise


def _parse_feed_entry(entry: dict, source_name: str, source_type: NewsSource) -> Optional[NewsArticle]:
    headline = entry.get("title", "").strip()
    url = entry.get("link", "").strip()
    if not headline or not url:
        return None

    # Parse date
    pub_struct = entry.get("published_parsed")
    if pub_struct:
        import calendar
        pub_dt = datetime.fromtimestamp(calendar.timegm(pub_struct), tz=timezone.utc)
    else:
        pub_dt = datetime.now(timezone.utc)

    art_id = hashlib.md5(url.encode()).hexdigest()

    return NewsArticle(
        id=art_id,
        headline=headline,
        source=source_name,
        source_type=source_type,
        url=url,
        published_at=pub_dt,
        sentiment=_score_sentiment(headline),
        symbols=_extract_symbols(headline),
    )


# ─────────────────────────────────────────────────────────
#  Async RSS fetcher
# ─────────────────────────────────────────────────────────
async def _fetch_feed(session: aiohttp.ClientSession, feed_conf: dict) -> List[NewsArticle]:
    url = feed_conf["url"]
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            if resp.status != 200:
                return []
            text = await resp.text()
    except Exception as exc:
        logger.warning("Feed fetch failed [%s]: %s", url, exc)
        return []

    parsed = feedparser.parse(text)
    articles = []
    for entry in parsed.entries[: settings.NEWS_MAX_ITEMS // len(RSS_FEEDS)]:
        article = _parse_feed_entry(entry, feed_conf["name"], feed_conf["source_type"])
        if article:
            articles.append(article)
    return articles


async def fetch_all_news() -> List[NewsArticle]:
    connector = aiohttp.TCPConnector(limit=20)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [_fetch_feed(session, feed) for feed in RSS_FEEDS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    all_articles: List[NewsArticle] = []
    seen_ids: set = set()
    for result in results:
        if isinstance(result, list):
            for art in result:
                if art.id not in seen_ids:
                    seen_ids.add(art.id)
                    all_articles.append(art)

    all_articles.sort(key=lambda a: a.published_at, reverse=True)
    return all_articles[: settings.NEWS_MAX_ITEMS]


# ─────────────────────────────────────────────────────────
#  Redis cache helpers
# ─────────────────────────────────────────────────────────
async def cache_articles(articles: List[NewsArticle]):
    redis = redis_pool.client
    payload = json.dumps([a.model_dump(mode="json") for a in articles])
    await redis.setex(NEWS_CACHE_KEY, NEWS_CACHE_TTL, payload)
    # Pub/Sub so news panel updates without polling
    await redis.publish(settings.REDIS_NEWS_CHANNEL, payload)


async def get_cached_articles() -> Optional[List[NewsArticle]]:
    redis = redis_pool.client
    raw = await redis.get(NEWS_CACHE_KEY)
    if not raw:
        return None
    data = json.loads(raw)
    return [NewsArticle(**item) for item in data]


# ─────────────────────────────────────────────────────────
#  Background service
# ─────────────────────────────────────────────────────────
class NewsIngestionService:
    def __init__(self):
        self._running = False

    async def start(self):
        self._running = True
        logger.info("News ingestion service started (interval=%ds)", settings.NEWS_REFRESH_SECONDS)
        while self._running:
            try:
                articles = await fetch_all_news()
                await cache_articles(articles)
                logger.info("News refresh: %d articles cached", len(articles))
            except Exception as exc:
                logger.error("News ingestion error: %s", exc)
            await asyncio.sleep(settings.NEWS_REFRESH_SECONDS)

    async def stop(self):
        self._running = False