"""Read-only stats for the spider-hub dashboard: how many posts have been
collected per platform, and a daily trend. Pure GROUP BY queries against
D1's `posts` table (see app/services/d1.py) - no writes, no Kafka."""

from __future__ import annotations

from fastapi import APIRouter, Query

from app.schemas.stats import PlatformStat, Post, PostPage, TimeseriesPoint
from app.services.d1 import get_post_counts_by_platform, get_post_timeseries, list_posts

router = APIRouter(prefix="/stats", tags=["stats"])


@router.get("/platforms", response_model=list[PlatformStat])
async def platform_stats() -> list[PlatformStat]:
    rows = await get_post_counts_by_platform()
    return [PlatformStat(**row) for row in rows]


@router.get("/timeseries", response_model=list[TimeseriesPoint])
async def timeseries_stats(days: int = Query(default=14, ge=1, le=90)) -> list[TimeseriesPoint]:
    rows = await get_post_timeseries(days)
    return [TimeseriesPoint(**row) for row in rows]


@router.get("/posts", response_model=PostPage)
async def posts(
    platform: str | None = None,
    keyword_id: str | None = None,
    movie_id: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> PostPage:
    rows, total = await list_posts(platform=platform, keyword_id=keyword_id, movie_id=movie_id, limit=limit, offset=offset)
    return PostPage(items=[Post(**row) for row in rows], total=total, limit=limit, offset=offset)
