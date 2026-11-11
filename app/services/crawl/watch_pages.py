"""Quét lại page Facebook của phim đang chiếu, để mục "Đang nổi" của Social Topic có tốc độ tăng tương tác.

Mỗi settings.facebook_watch_pages_interval_hours giờ xếp job đọc page cho page của phim đang chiếu, rồi tới page chung (CGV,
Moveek...) hay có bài. Mỗi page chỉ đọc trang bảng tin mới nhất và gửi lại cả bài đã thấy (dedupe=false): một request cập
nhật số của nhiều bài, ingest ghi post_engagement_snapshots khi số đổi, cinemark đọc bảng đó để tính tốc độ
(api/lib/trendingPosts.ts).

chuyển sang luồng KHÔNG đăng nhập (job public_page của spider-hub, proxy riêng không gán tài khoản - người vận
hành quyết định) thay cho page_feed qua tài khoản: tài khoản Facebook chỉ còn vài cái, để dành hạn mức cho tìm kiếm. Không
còn cần tài khoản sẵn sàng hay tránh giờ yên lặng của Meta; proxy bị chặn thì spider-hub tự cho nghỉ.

Phim đang chiếu: từ ngày chiếu sớm (movie_measure_settings.premiere_date, không có thì ngày công chiếu) tới
SHOWING_DAYS_AFTER_RELEASE ngày sau công chiếu.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import func, select

from app.clients.redis import get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.repositories.d1.sql import d1_exec
from app.repositories.d1.store import get_or_create_keyword
from app.repositories.d1.tables import movie_measure_settings, movies
from app.services.crawl.source_pages import _PUBLIC_QUEUE_KEY as PUBLIC_QUEUE_KEY
from app.services.crawl.source_pages import _movie_title, source_pages
from app.services.crawl.task_queue import enqueue_published

logger = get_logger(__name__)

TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")
SHOWING_DAYS_AFTER_RELEASE = 30
# Hàng đợi public_page còn chừng này job thì lượt này không xếp thêm (spider-hub đọc chậm / proxy đang bị chặn).
MAX_PENDING_PUBLIC_PAGES = 20


def pick_pages(rows: list[dict[str, Any]], showing_ids: set[str], limit: int) -> list[dict[str, Any]]:
    """Page của phim đang chiếu trước, rồi page chung, cùng nhóm thì page hay có bài mới trước. Page của phim khác bị bỏ."""
    eligible = [row for row in rows if not row.get("movie_id") or row["movie_id"] in showing_ids]
    eligible.sort(key=lambda row: (0 if row.get("movie_id") else 1, int(row.get("quiet_runs") or 0)))
    return eligible[:limit]


async def showing_movie_ids(now: datetime) -> set[str]:
    today = now.astimezone(TIMEZONE).date().isoformat()
    m, s = movies.alias("m"), movie_measure_settings.alias("s")
    opening = func.coalesce(func.nullif(s.c.premiere_date, ""), m.c.released_at)
    rows = await d1_exec(
        select(m.c.id)
        .select_from(m.outerjoin(s, s.c.movie_id == m.c.id))
        .where(
            m.c.enabled == 1,
            m.c.released_at.is_not(None),
            func.date(opening) <= func.date(today),
            func.date(m.c.released_at, f"+{SHOWING_DAYS_AFTER_RELEASE} day") >= func.date(today),
        ),
        quiet=True,
    )
    return {row["id"] for row in rows or []}


async def queue_public_page(row: dict[str, Any], now: datetime) -> bool:
    """Một job public_page cho page: chỉ trang bảng tin mới nhất, gửi lại cả bài đã thấy (spider-hub jobs/public_page.py)."""
    keyword_id = None
    if row.get("movie_id"):
        title = await _movie_title(row["movie_id"])
        keyword = await get_or_create_keyword(row["movie_id"], "facebook", title) if title else None
        if keyword is None:
            return False
        keyword_id = keyword["id"]
    request: dict[str, Any] = {
        "type": "public_page",
        "platform": "facebook",
        "page_url": row["url"],
        "keyword_id": keyword_id,
        "max_pages": settings.facebook_watch_pages_max_pages,
        "since_days": max(1, round(settings.facebook_watch_pages_since_days)),
        "dedupe": "false",
        "run_id": str(uuid.uuid4()),
        "published_at": now.timestamp(),
    }
    await enqueue_published(request)  # hiện trên trang Hàng đợi
    await get_redis_client().lpush(PUBLIC_QUEUE_KEY, json.dumps(request, ensure_ascii=False))
    return True


async def queue_round(now: datetime) -> int:
    pending = await get_redis_client().llen(PUBLIC_QUEUE_KEY)
    if pending >= MAX_PENDING_PUBLIC_PAGES:
        logger.info("watch_pages_skipped_backlog", pending_public_pages=pending)
        return 0
    showing = await showing_movie_ids(now)
    if not showing:
        return 0
    rows = await d1_exec(
        select(source_pages).where(source_pages.c.enabled == 1, source_pages.c.platform == "facebook"), quiet=True
    )
    picked = pick_pages(rows or [], showing, settings.facebook_watch_pages_per_round)
    queued = 0
    for row in picked:
        if await queue_public_page(row, now):
            queued += 1
    logger.info("watch_pages_queued", queued=queued, picked=len(picked), showing=len(showing))
    return queued
