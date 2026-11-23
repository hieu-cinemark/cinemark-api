"""Cào ngay một hashtag theo yêu cầu của người dùng (thêm mới hoặc cào lại) và báo tiến độ."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import Any

import httpx
from sqlalchemy import select

from app.clients.omar import OmarStop, map_video
from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.config import settings
from app.core.jsonutil import loads_or_none
from app.core.logging import get_logger
from app.repositories.d1.keywords import get_enabled_keywords
from app.repositories.d1.sql import d1_exec
from app.repositories.d1.tables import movies
from app.services.accounts.config_db.omar import get_omar_api_settings
from app.services.omar.omar_budget import _Pacer
from app.services.omar.omar_fetch import _fetch_page, publish_videos
from app.services.omar.omar_seen import _count_cooccur

logger = get_logger(__name__)

_STATE_KEY = f"{REDIS_KEY_PREFIX}omar:hashtag_state"

_MAX_PAGE_FAILURES = 3

# Hashtag đang được cào riêng (crawl_hashtag_now) - lượt Omar bỏ qua để không cào trùng con trỏ.
_manual_running: set[str] = set()

_manual_tasks: set[asyncio.Task] = set()

_RESET_KEEP = ("video_count", "view_count", "info_at", "hashtag_id")


async def _crawl_hashtag_now(keyword: dict[str, Any]) -> None:
    kw_id = keyword["id"]
    api_key = ((await get_omar_api_settings() or {}).get("api_key") or "").strip()
    if not api_key:
        return
    redis = get_redis_client()
    raw = await redis.hget(_STATE_KEY, kw_id)
    entry = json.loads(raw) if raw else {}
    if entry.get("done_at"):
        entry = {k: entry[k] for k in _RESET_KEEP if k in entry}
    entry.pop("missing_at", None)
    entry.setdefault("started_at", datetime.now(tz=UTC).isoformat())
    cursor = entry.get("cursor") or "0"
    stats: dict[str, Any] = {"pages": 0, "videos": 0, "published": 0, "skipped_seen": 0}
    pacer = _Pacer(settings.omar_manual_requests_per_minute)
    failed = 0
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        while True:
            try:
                videos, following, hashtag_id = await _fetch_page(
                    client, pacer, api_key, str(keyword["keyword"]).lstrip("#"), cursor, entry.get("hashtag_id")
                )
            except OmarStop as exc:
                logger.error("omar_manual_stopped", hashtag=keyword["keyword"], error=exc)
                break
            except Exception as exc:  # noqa: BLE001 - thử lại đúng trang, lỗi liền thì để lượt Omar đi tiếp
                failed += 1
                logger.warning("omar_manual_page_failed", hashtag=keyword["keyword"], cursor=cursor, error=exc)
                if failed >= _MAX_PAGE_FAILURES:
                    if cursor == "0" and not entry.get("pages"):
                        entry["missing_at"] = datetime.now(tz=UTC).isoformat()
                    break
                await asyncio.sleep(30 * failed)
                continue
            failed = 0
            if hashtag_id:
                entry["hashtag_id"] = hashtag_id
            mapped = [video for video in (map_video(item) for item in videos) if video is not None]
            stats["pages"] += 1
            stats["videos"] += len(mapped)
            await _count_cooccur(str(keyword["keyword"]), kw_id, mapped)
            await publish_videos(mapped, kw_id, stats)
            entry["pages"] = int(entry.get("pages") or 0) + 1
            if following is None:
                entry.update(cursor=None, done_at=datetime.now(tz=UTC).isoformat())
                break
            cursor = following
            entry["cursor"] = cursor
            await redis.hset(_STATE_KEY, kw_id, json.dumps(entry))
    await redis.hset(_STATE_KEY, kw_id, json.dumps(entry))
    logger.info("omar_manual_finished", hashtag=keyword["keyword"], done=bool(entry.get("done_at")), **stats)


async def recrawl_hashtag_now(keyword: dict[str, Any]) -> bool:
    """Nút "Cào lại" của một hashtag ở tab Omar: xoá con trỏ / mốc xong của hashtag (giữ số liệu hashtag_info) rồi cào lại từ trang
    đầu ngay (crawl_hashtag_now). False khi hashtag đang được cào riêng - không đụng con trỏ đang chạy. Lượt Omar đang chạy sẽ
    bỏ qua hashtag này (xem _manual_running); lượt sau thấy nó đã xong lại thì cào lại theo omar_recrawl_days như mọi hashtag."""
    kw_id = keyword["id"]
    if kw_id in _manual_running:
        return False
    redis = get_redis_client()
    raw = await redis.hget(_STATE_KEY, kw_id)
    try:
        entry = json.loads(raw) if raw else {}
    except TypeError, ValueError:
        entry = {}
    await redis.hset(_STATE_KEY, kw_id, json.dumps({k: entry[k] for k in _RESET_KEEP if k in entry}))
    return crawl_hashtag_now(keyword)


def crawl_hashtag_now(keyword: dict[str, Any]) -> bool:
    """Cào NGAY một hashtag tới hết (ô "Thêm hashtag" ở tab Omar), độc lập với lượt đang chạy - lượt chế độ liên tục có
    thể kéo dài nhiều giờ và chỉ nạp danh sách hashtag lúc bắt đầu. False khi hashtag đang được cào riêng."""
    kw_id = keyword["id"]
    if kw_id in _manual_running:
        return False
    _manual_running.add(kw_id)

    async def run() -> None:
        try:
            await _crawl_hashtag_now(keyword)
        except Exception as exc:  # noqa: BLE001
            logger.error("omar_manual_failed", hashtag=keyword.get("keyword"), error=exc)
        finally:
            _manual_running.discard(kw_id)

    task = asyncio.create_task(run())
    _manual_tasks.add(task)
    task.add_done_callback(_manual_tasks.discard)
    return True


async def hashtag_progress() -> list[dict[str, Any]]:
    """Mỗi hashtag TikTok đang bật: tên, phim, số trang đã lấy, cursor, đã cào xong lúc nào (từ Redis omar:hashtag_state)."""
    keywords = [k for k in await get_enabled_keywords(platform="tiktok") if str(k["keyword"]).startswith("#")]
    titles = {row["id"]: row["title"] for row in await d1_exec(select(movies.c.id, movies.c.title), quiet=True) or []}
    raw = await get_redis_client().hgetall(_STATE_KEY) or {}
    state = {}
    for key, value in raw.items():
        parsed = loads_or_none(value, source="omar_hashtag_state")
        if parsed is not None:
            state[key.decode() if isinstance(key, bytes) else key] = parsed
    out = []
    for kw in keywords:
        entry = state.get(kw["id"]) or {}
        out.append(
            {
                "keyword_id": kw["id"],
                "hashtag": kw["keyword"],
                "movie": titles.get(kw["movie_id"]),
                "pages": int(entry.get("pages") or 0),
                "cursor": entry.get("cursor"),
                "done_at": entry.get("done_at"),
                "fresh_at": entry.get("fresh_at"),
                "video_count": entry.get("video_count"),
                "view_count": entry.get("view_count"),
                "info_at": entry.get("info_at"),
                "missing_at": entry.get("missing_at"),
            }
        )
    return sorted(out, key=lambda row: (row["done_at"] is not None, -row["pages"]))
