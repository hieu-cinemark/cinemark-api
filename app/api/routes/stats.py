"""Thống kê chỉ đọc cho dashboard spider-hub: số bài và comment đã thu thập theo nền
tảng, và xu hướng theo ngày. Bản thân bài/comment đi qua repository riêng
(app/repositories/d1/{posts,comments}.py); mọi thứ khác ở đây chỉ chuyển tiếp sang
stats_summary.py (xem app/repositories/d1/store.py), vốn đọc các bảng tổng hợp theo ngày thay
vì GROUP BY trên toàn bảng `posts` / `comments` của D1."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import APIRouter, Query

from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.logging import get_logger
from app.repositories.d1 import comment_repo, post_repo
from app.repositories.d1.store import (
    get_comment_counts_by_platform,
    get_comment_timeseries,
    get_keyword_volume,
    get_post_counts_by_platform,
    get_post_timeseries,
)
from app.schemas.stats import (
    Comment,
    CommentPage,
    CommentWithPost,
    HourlyPoint,
    IngestFunnel,
    KeywordVolume,
    PlatformStat,
    Post,
    PostPage,
    TimeseriesPoint,
)
from app.services.stats.stats_summary import get_hourly_counts, get_ingest_funnel

router = APIRouter(prefix="/stats", tags=["stats"])
logger = get_logger(__name__)

# Số liệu tổng hợp cho trang Tổng quan (28/10/2026): D1 quá tải ("7429 overloaded") làm truy vấn mất 15-20 giây kèm thử
# lại, dashboard hết chờ và hiện 0. Giờ: kết quả còn mới (< _FRESH_SECONDS) trả thẳng từ Redis - nhiều tab/người cùng
# mở không nhân số truy vấn D1 lên; truy vấn D1 quá _QUERY_TIMEOUT_SECONDS hoặc lỗi/rỗng thì trả số gần nhất đã có
# (giữ tới _STALE_SECONDS) thay vì 0.
_FRESH_SECONDS = 60
_STALE_SECONDS = 2 * 86400
_QUERY_TIMEOUT_SECONDS = 8.0
_CACHE_KEY = f"{REDIS_KEY_PREFIX}stats_cache:{{name}}"


async def _cached_rows(name: str, compute: Callable[[], Awaitable[list[dict[str, Any]]]]) -> list[dict[str, Any]]:
    redis = get_redis_client()
    key = _CACHE_KEY.format(name=name)
    cached: dict[str, Any] | None = None
    try:
        raw = await redis.get(key)
        cached = json.loads(raw) if raw else None
    except Exception as exc:  # noqa: BLE001 - Redis trục trặc: vẫn đọc D1 như cũ
        logger.warning("stats_cache_read_failed", name=name, error=str(exc)[:200])
    if cached and time.time() - float(cached.get("at") or 0) < _FRESH_SECONDS:
        return cached["rows"]
    try:
        rows = await asyncio.wait_for(compute(), timeout=_QUERY_TIMEOUT_SECONDS)
    except Exception as exc:  # noqa: BLE001 - timeout / D1 lỗi: rơi về số gần nhất
        logger.warning("stats_query_failed", name=name, error=str(exc)[:200] or type(exc).__name__)
        rows = None
    if rows:
        try:
            await redis.set(key, json.dumps({"at": time.time(), "rows": rows}, default=str), ex=_STALE_SECONDS)
        except Exception:  # noqa: BLE001 - không lưu được cache thì vẫn trả kết quả vừa đọc
            pass
        return rows
    if cached:
        logger.info("stats_served_stale", name=name, age_seconds=round(time.time() - float(cached.get("at") or 0)))
        return cached["rows"]
    return rows or []


@router.get("/platforms", response_model=list[PlatformStat])
async def platform_stats() -> list[PlatformStat]:
    rows = await _cached_rows("platforms", get_post_counts_by_platform)
    return [PlatformStat(**row) for row in rows]


@router.get("/timeseries", response_model=list[TimeseriesPoint])
async def timeseries_stats(days: int = Query(default=14, ge=1, le=90)) -> list[TimeseriesPoint]:
    rows = await _cached_rows(f"timeseries:{days}", lambda: get_post_timeseries(days))
    return [TimeseriesPoint(**row) for row in rows]


@router.get("/hourly", response_model=list[HourlyPoint])
async def hourly_stats(hours: int = Query(default=24, ge=1, le=72)) -> list[HourlyPoint]:
    # Bộ đếm Redis (xem stats_summary.get_hourly_counts), không đụng tới D1.
    rows = await get_hourly_counts(hours)
    return [HourlyPoint(**row) for row in rows]


@router.get("/ingest-funnel", response_model=list[IngestFunnel])
async def ingest_funnel(hours: int = Query(default=24, ge=1, le=72)) -> list[IngestFunnel]:
    # Nhận về -> mới / cập nhật / bị loại theo lý do (Redis, xem stats_summary).
    return [IngestFunnel(**row) for row in await get_ingest_funnel(hours)]


@router.get("/comment-counts", response_model=list[PlatformStat])
async def comment_count_stats() -> list[PlatformStat]:
    rows = await _cached_rows("comment_counts", get_comment_counts_by_platform)
    return [PlatformStat(**row) for row in rows]


@router.get("/comment-timeseries", response_model=list[TimeseriesPoint])
async def comment_timeseries_stats(days: int = Query(default=14, ge=1, le=90)) -> list[TimeseriesPoint]:
    rows = await _cached_rows(f"comment_timeseries:{days}", lambda: get_comment_timeseries(days))
    return [TimeseriesPoint(**row) for row in rows]


@router.get("/keywords", response_model=list[KeywordVolume])
async def keyword_volume_stats(platform: str | None = Query(default=None)) -> list[KeywordVolume]:
    rows = await _cached_rows(f"keywords:{platform or 'all'}", lambda: get_keyword_volume(platform))
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
    # sort="engagement" (top-N theo từ khoá/phim) không có thanh phân trang đánh số ở
    # frontend và không dùng được keyset (xem docstring của list_posts_cursor) - vẫn dùng
    # đường offset của list_posts, thực tế luôn là offset=0 (useTopPostsByKeyword/
    # useTopPostsByMovie lấy một lô cố định, không bao giờ phân trang tiếp).
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
