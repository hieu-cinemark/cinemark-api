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
    keyword_match: bool | None = Query(default=None),
    sort: str = Query(default="recent"),
    cursor: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> PostPage:
    # sort="engagement" (top-N by keyword/movie) has no numbered pager on
    # the frontend and isn't keyset-eligible (see list_posts_cursor's own
    # docstring) - keeps using list_posts' offset path, always offset=0
    # in practice (useTopPostsByKeyword/useTopPostsByMovie fetch one
    # fixed-size batch, never paginate further).
    if sort == "engagement":
        rows, _total = await post_repo.list_posts(
            platform=platform,
            keyword_id=keyword_id,
            movie_id=movie_id,
            keyword_match=keyword_match,
            sort="engagement",
            limit=limit,
            offset=offset,
        )
        return PostPage(items=[Post(**row) for row in rows])

    rows, next_cursor = await post_repo.list_posts_cursor(
        platform=platform,
        keyword_id=keyword_id,
        movie_id=movie_id,
        keyword_match=keyword_match,
        sort="recent",
        cursor=cursor,
        limit=limit,
    )
    return PostPage(items=[Post(**row) for row in rows], nextCursor=next_cursor)


@router.get("/posts/{post_id}/comments", response_model=list[Comment])
async def post_comments(post_id: str) -> list[Comment]:
    rows = await comment_repo.list_comments(post_id)
    return [Comment(**row) for row in rows]


@router.get("/comments", response_model=CommentPage)
async def comments(
    platform: str | None = None,
    movie_id: str | None = None,
    keyword_id: str | None = None,
    sentiment: str | None = Query(default=None),
    cursor: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
) -> CommentPage:
    label = sentiment if sentiment in {"positive", "negative", "neutral"} else None
    rows, next_cursor = await comment_repo.list_all_comments_cursor(
        platform=platform, movie_id=movie_id, keyword_id=keyword_id, sentiment=label, cursor=cursor, limit=limit
    )
    return CommentPage(items=[CommentWithPost(**row) for row in rows], nextCursor=next_cursor)
