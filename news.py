"""
StockIQ – News API Router
GET /api/v1/news/           → paginated news with optional source filter
GET /api/v1/news/latest     → latest N headlines
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Query

from models.schemas import NewsArticle, NewsResponse, NewsSource, Sentiment
from services.news_ingestion import fetch_all_news, get_cached_articles

router = APIRouter()


@router.get("/", response_model=NewsResponse)
async def get_news(
    source_type: Optional[NewsSource] = None,
    sentiment: Optional[Sentiment] = None,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """
    Returns paginated news articles.
    Optional filters: source_type (domestic/international), sentiment.
    Falls back to live fetch if cache is cold.
    """
    articles = await get_cached_articles()
    if articles is None:
        articles = await fetch_all_news()

    if source_type:
        articles = [a for a in articles if a.source_type == source_type]
    if sentiment:
        articles = [a for a in articles if a.sentiment == sentiment]

    return NewsResponse(
        articles=articles[offset : offset + limit],
        fetched_at=datetime.now(timezone.utc),
    )


@router.get("/latest", response_model=List[NewsArticle])
async def latest_news(limit: int = Query(10, ge=1, le=50)):
    articles = await get_cached_articles() or []
    return articles[:limit]