"""Read-only stats for the spider-hub dashboard: how many posts and comments
have been collected per platform, and a daily trend. Pure GROUP BY queries
against D1's `posts` / `comments` tables. Posts/comments themselves go
through their own repositories (app/repositories/d1/{posts,comments}.py);
everything else here is a stats_summary.py passthrough (see
app/services/d1.py)."""

from __future__ import annotations

from fastapi import APIRouter, Query

from app.repositories.d1 import comment_repo, post_repo
from app.schemas.stats import Comment, CommentPage, CommentWithPost, KeywordVolume, PlatformStat, Post, PostPage, TimeseriesPoint
from app.services.d1 import (
    get_comment_counts_by_platform,
    get_comment_timeseries,
    get_keyword_volume,
    get_post_counts_by_platform,
    get_post_timeseries,
)

router = APIRouter(prefix="/stats", tags=["stats"])


@router.get("/platforms", response_model=list[PlatformStat])
async def platform_stats() -> list[PlatformStat]:
    rows = await get_post_counts_by_platform()
    return [PlatformStat(**row) for row in rows]


@router.get("/timeseries", response_model=list[TimeseriesPoint])
async def timeseries_stats(days: int = Query(default=14, ge=1, le=90)) -> list[TimeseriesPoint]:
    rows = await get_post_timeseries(days)
    return [TimeseriesPoint(**row) for row in rows]


@router.get("/comment-counts", response_model=list[PlatformStat])
async def comment_count_stats() -> list[PlatformStat]:
    rows = await get_comment_counts_by_platform()
    return [PlatformStat(**row) for row in rows]


@router.get("/comment-timeseries", response_model=list[TimeseriesPoint])
async def comment_timeseries_stats(days: int = Query(default=14, ge=1, le=90)) -> list[TimeseriesPoint]:
    rows = await get_comment_timeseries(days)
    return [TimeseriesPoint(**row) for row in rows]


@router.get("/keywords", response_model=list[KeywordVolume])
async def keyword_volume_stats(platform: str | None = Query(default=None)) -> list[KeywordVolume]:
    rows = await get_keyword_volume(platform)
    return [KeywordVolume(**row) for row in rows]


@router.get("/posts", response_model=PostPage)
async def posts(
    platform: str | None = None,
    keyword_id: str | None = None,
    movie_id: str | None = None,
    sort: str = Query(default="recent"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> PostPage:
    order = "engagement" if sort == "engagement" else "recent"
    rows, total = await post_repo.list_posts(
        platform=platform, keyword_id=keyword_id, movie_id=movie_id, sort=order, limit=limit, offset=offset
    )
    return PostPage(items=[Post(**row) for row in rows], total=total, limit=limit, offset=offset)


@router.get("/posts/{post_id}/comments", response_model=list[Comment])
async def post_comments(post_id: str) -> list[Comment]:
    rows = await comment_repo.list_comments(post_id)
    return [Comment(**row) for row in rows]


@router.get("/comments", response_model=CommentPage)
async def comments(
    platform: str | None = None,
    movie_id: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> CommentPage:
    rows, total = await comment_repo.list_all_comments(platform=platform, movie_id=movie_id, limit=limit, offset=offset)
    return CommentPage(items=[CommentWithPost(**row) for row in rows], total=total, limit=limit, offset=offset)
