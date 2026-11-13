"""Kênh Threads làm nguồn cào (13/11/2026) - đọc không đăng nhập ở spider-hub (job threads_channel), không tốn tài khoản.

Kênh nằm chung bảng source_pages với trang Facebook (platform='threads', url https://www.threads.com/@handle), bật/tắt và
"Chạy ngay" như trang Facebook. Tìm kiếm và bình luận Threads vẫn đi qua tài khoản.

- fill_queue(): mỗi nhịp scheduler, hàng đợi crawl_public_pages:threads vơi thì nạp kênh tới hạn. Kênh của phim đọc mỗi
  threads_channel_interval_hours, kênh chung gấp đôi; lần đọc trước không có bài mới (spider-hub ghi
  threads_channel_last:<url>) thì giãn gấp đôi, tối đa x MAX_BACKOFF. Khoảng ngày lần sau chỉ phủ từ lần đọc trước.
- auto_add_authors(): mỗi ngày thêm các tác giả Threads có nhiều bài liên quan về >= 2 phim trong 30 ngày (kênh chung,
  added_by='auto_threads'), không qua AI - ingest vẫn lọc từng bài theo phim.
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.sqlite import insert

from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.repositories.d1.posts import relevant_post
from app.repositories.d1.sql import d1_exec
from app.repositories.d1.store import get_or_create_keyword
from app.repositories.d1.tables import movies
from app.repositories.d1.tables import posts as posts_table
from app.services.accounts.platform_config_db import crawl_setting
from app.services.crawl.source_pages import _ensure_table, _movie_title, _set_source, source_pages
from app.services.crawl.task_queue import enqueue_published

logger = get_logger(__name__)

QUEUE_KEY = f"{REDIS_KEY_PREFIX}crawl_public_pages:threads"
LAST_KEY_TMPL = f"{REDIS_KEY_PREFIX}threads_channel_last:{{url}}"
SINCE_DAYS = 30
MAX_BACKOFF = 8
SUGGEST_WINDOW_DAYS = 30
SUGGEST_MIN_POSTS = 3
SUGGEST_MIN_FILMS = 2
_POST_URL_HANDLE = re.compile(r"threads\.(?:com|net)/@([A-Za-z0-9._]{1,30})/post/", re.IGNORECASE)

_ENABLED_CHANNELS = (
    select(source_pages)
    .select_from(source_pages.outerjoin(movies, movies.c.id == source_pages.c.movie_id))
    .where(
        source_pages.c.platform == "threads",
        source_pages.c.enabled == 1,
        (source_pages.c.movie_id.is_(None)) | (movies.c.enabled == 1),
    )
)


async def _last_run(url: str) -> dict[str, Any]:
    try:
        raw = await get_redis_client().get(LAST_KEY_TMPL.format(url=url))
        return json.loads(raw) if raw else {}
    except Exception:  # noqa: BLE001 - không đọc được thì coi như chưa có lần đọc nào
        return {}


def _interval(source: dict[str, Any]) -> timedelta:
    base = settings.threads_channel_interval_hours * (1 if source.get("movie_id") else 2)
    return timedelta(hours=base * min(MAX_BACKOFF, 2 ** int(source.get("quiet_runs") or 0)))


def _due(source: dict[str, Any], now: datetime) -> bool:
    try:
        return now - datetime.fromisoformat(str(source.get("last_queued_at") or "")) >= _interval(source)
    except ValueError:
        return True


async def _sync_quiet_runs(source: dict[str, Any]) -> None:
    last = await _last_run(source["url"])
    if not last or last.get("status") != "ok":
        return
    quiet = 0 if int(last.get("new") or 0) > 0 else int(source.get("quiet_runs") or 0) + 1
    if last.get("user_id") and not source.get("page_id"):
        await _set_source(source["id"], page_id=str(last["user_id"]), quiet=True)
        source["page_id"] = str(last["user_id"])
    if quiet != int(source.get("quiet_runs") or 0):
        await _set_source(source["id"], quiet_runs=quiet, quiet=True)
        source["quiet_runs"] = quiet


async def _since_days(source: dict[str, Any], now: datetime) -> int:
    """Lần đọc trước xong (đã tới mốc / hết bài) thì chỉ đọc phần mới, cộng 2 ngày phòng bài đăng trễ."""
    last = await _last_run(source["url"])
    if last.get("status") != "ok" or last.get("stop_reason") not in ("since", "exhausted") or not last.get("at"):
        return SINCE_DAYS
    return max(1, min(SINCE_DAYS, int((now.timestamp() - float(last["at"])) / 86400) + 2))


async def queue_channel(source: dict[str, Any], now: datetime) -> bool:
    keyword_id = None
    if source.get("movie_id"):
        title = await _movie_title(source["movie_id"])
        keyword = await get_or_create_keyword(source["movie_id"], "threads", title) if title else None
        if keyword is None:
            return False
        keyword_id = keyword["id"]
    request = {
        "type": "threads_channel",
        "platform": "threads",
        "page_url": source["url"],
        # None với kênh chung - ingest tự tìm phim theo nội dung bài.
        "keyword_id": keyword_id,
        "since_days": await _since_days(source, now),
        "run_id": str(uuid.uuid4()),
        "published_at": now.timestamp(),
    }
    await enqueue_published(request)  # hiện trên trang Hàng đợi
    await get_redis_client().lpush(QUEUE_KEY, json.dumps(request, ensure_ascii=False))
    await _set_source(source["id"], last_queued_at=now.isoformat(), quiet=True)
    return True


async def fill_queue(now: datetime | None = None) -> int:
    now = now or datetime.now(tz=UTC)
    if not settings.threads_channels_enabled:
        return 0
    room = int(await crawl_setting("threads_channels_queue_target")) - await get_redis_client().llen(QUEUE_KEY)
    if room <= 0:
        return 0
    await _ensure_table()
    due = []
    for row in await d1_exec(_ENABLED_CHANNELS, quiet=True) or []:
        await _sync_quiet_runs(row)
        if _due(row, now):
            due.append(row)
    due.sort(
        key=lambda r: (int(r.get("quiet_runs") or 0), 0 if r.get("movie_id") else 1, str(r.get("last_queued_at") or ""))
    )
    queued = 0
    for row in due[:room]:
        if await queue_channel(row, now):
            queued += 1
    if queued:
        logger.info("threads_channels_queued", queued=queued, due=len(due))
    return queued


def handle_from_post_url(url: str | None) -> str | None:
    match = _POST_URL_HANDLE.search(url or "")
    return match.group(1).lower() if match else None


async def suggest_authors(limit: int | None = None) -> list[dict[str, Any]]:
    """Tác giả Threads nói về >= SUGGEST_MIN_FILMS phim đang theo dõi, >= SUGGEST_MIN_POSTS bài liên quan trong 30 ngày,
    chưa có trong danh sách - nhiều bài trước."""
    await _ensure_table()
    since = (datetime.now(tz=UTC) - timedelta(days=SUGGEST_WINDOW_DAYS)).isoformat()
    p = posts_table.alias("p")
    rows = (
        await d1_exec(
            select(p.c.movie_id, p.c.author, func.min(p.c.url).label("url"), func.count().label("posts"))
            .select_from(p.join(movies, movies.c.id == p.c.movie_id))
            .where(
                p.c.platform == "threads",
                movies.c.enabled == 1,
                p.c.posted_at >= since,
                relevant_post(p),
                p.c.author.is_not(None),
                p.c.author != "",
            )
            .group_by(p.c.movie_id, p.c.author),
            quiet=True,
            timeout=60.0,
        )
        or []
    )
    taken = {
        r["url"]
        for r in await d1_exec(select(source_pages.c.url).where(source_pages.c.platform == "threads"), quiet=True) or []
    }
    by_handle: dict[str, dict[str, Any]] = {}
    for row in rows:
        handle = handle_from_post_url(row.get("url"))
        if not handle:
            continue
        url = f"https://www.threads.com/@{handle}"
        if url in taken:
            continue
        entry = by_handle.setdefault(handle, {"url": url, "name": row["author"], "posts": 0, "films": set()})
        entry["posts"] += int(row["posts"] or 0)
        entry["films"].add(row["movie_id"])
    out = [
        {"url": e["url"], "name": e["name"], "posts": e["posts"], "films": len(e["films"])}
        for e in by_handle.values()
        if e["posts"] >= SUGGEST_MIN_POSTS and len(e["films"]) >= SUGGEST_MIN_FILMS
    ]
    out.sort(key=lambda e: (-e["films"], -e["posts"]))
    return out[:limit] if limit else out


async def auto_add_authors(*, dry_run: bool = False) -> int:
    """Thêm tối đa settings.threads_channels_auto_add kênh chung từ suggest_authors()."""
    limit = settings.threads_channels_auto_add
    if limit <= 0:
        return 0
    picks = await suggest_authors(limit)
    if dry_run:
        return len(picks)
    now = datetime.now(tz=UTC).isoformat()
    for pick in picks:
        await d1_exec(
            insert(source_pages).values(
                id=f"src_{uuid.uuid4()}",
                platform="threads",
                url=pick["url"],
                name=pick["name"],
                movie_id=None,
                enabled=1,
                created_at=now,
                added_by="auto_threads",
                ai_reason=f"{pick['posts']} bài liên quan về {pick['films']} phim trong {SUGGEST_WINDOW_DAYS} ngày",
            ),
            quiet=True,
        )
    if picks:
        logger.info("threads_channels_auto_added", added=len(picks))
    return len(picks)
