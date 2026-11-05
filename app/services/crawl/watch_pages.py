"""Quét lại page Facebook của phim đang chiếu (2026-11-05), để mục "Đang nổi" của Social Topic có tốc độ tăng tương tác.

Đọc page không đăng nhập hay bị chặn theo IP, nên lượt này đi qua tài khoản: mỗi settings.facebook_watch_pages_interval_hours
giờ xếp job page_feed cho page của phim đang chiếu, rồi tới page chung (CGV, Moveek...) hay có bài. Mỗi page chỉ đọc trang
bảng tin mới nhất và gửi lại cả bài đã thấy (dedupe=false): một request cập nhật số của nhiều bài, ingest ghi
post_engagement_snapshots khi số đổi, cinemark đọc bảng đó để tính tốc độ (api/lib/trendingPosts.ts).

Phim đang chiếu: từ ngày chiếu sớm (movie_measure_settings.premiere_date, không có thì ngày công chiếu) tới
SHOWING_DAYS_AFTER_RELEASE ngày sau công chiếu. Không có tài khoản Facebook sẵn sàng thì không xếp gì.
"""

from __future__ import annotations

from datetime import datetime, time
from typing import Any
from zoneinfo import ZoneInfo

from app.core.config import settings
from app.core.logging import get_logger
from app.repositories.d1.store import d1_query
from app.services.accounts import platform_config_db
from app.services.crawl.source_pages import MAX_PENDING_PAGE_FEED, queue_source
from app.services.crawl.task_queue import list_pending

logger = get_logger(__name__)

TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")
SHOWING_DAYS_AFTER_RELEASE = 30
# Giờ yên lặng của Meta (spider-hub META_QUIET_HOURS): job xếp lúc này chỉ nằm chờ.
QUIET_START = time(23, 30)
QUIET_END = time(5, 30)


def in_quiet_hours(now: datetime) -> bool:
    current = now.astimezone(TIMEZONE).time()
    return current >= QUIET_START or current < QUIET_END


def pick_pages(rows: list[dict[str, Any]], showing_ids: set[str], limit: int) -> list[dict[str, Any]]:
    """Page của phim đang chiếu trước, rồi page chung, cùng nhóm thì page hay có bài mới trước. Page của phim khác bị bỏ."""
    eligible = [row for row in rows if not row.get("movie_id") or row["movie_id"] in showing_ids]
    eligible.sort(key=lambda row: (0 if row.get("movie_id") else 1, int(row.get("quiet_runs") or 0)))
    return eligible[:limit]


async def showing_movie_ids(now: datetime) -> set[str]:
    today = now.astimezone(TIMEZONE).date().isoformat()
    rows = await d1_query(
        "SELECT m.id FROM movies m LEFT JOIN movie_measure_settings s ON s.movie_id = m.id "
        "WHERE m.enabled = 1 AND m.released_at IS NOT NULL "
        "AND date(COALESCE(NULLIF(s.premiere_date, ''), m.released_at)) <= date(?) "
        f"AND date(m.released_at, '+{SHOWING_DAYS_AFTER_RELEASE} day') >= date(?)",
        [today, today],
        quiet=True,
    )
    return {row["id"] for row in rows or []}


async def queue_round(now: datetime) -> int:
    if in_quiet_hours(now) or not await platform_config_db.count_crawl_ready_accounts("facebook"):
        return 0
    pending = [item for item in await list_pending("facebook") if item.get("type") == "page_feed"]
    if len(pending) >= MAX_PENDING_PAGE_FEED:
        logger.info("watch_pages_skipped_backlog", pending_page_feed=len(pending))
        return 0
    showing = await showing_movie_ids(now)
    if not showing:
        return 0
    rows = await d1_query("SELECT * FROM source_pages WHERE enabled = 1 AND platform = 'facebook'", quiet=True)
    picked = pick_pages(rows or [], showing, settings.facebook_watch_pages_per_round)
    options = {
        "max_pages": settings.facebook_watch_pages_max_pages,
        "since_days": settings.facebook_watch_pages_since_days,
        "dedupe": "false",
        "reels": "false",
    }
    queued = 0
    for row in picked:
        if await queue_source(row, overrides=options):
            queued += 1
    logger.info("watch_pages_queued", queued=queued, picked=len(picked), showing=len(showing))
    return queued
