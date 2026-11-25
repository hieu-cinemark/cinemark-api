"""Quy mô hashtag YouTube: mỗi ngày chụp tổng video / số kênh của các hashtag phim (bảng youtube_hashtag_stats) để Social
Topic vẽ thẻ "Quy mô trên nền tảng" cạnh TikTok.

Hashtag lấy từ các từ khoá TikTok dạng "#..." đang bật (bộ hashtag phim đã chọn; từ khoá YouTube là câu tìm thường). Mỗi
hashtag một GET trang youtube.com/hashtag/<tên> (app/clients/youtube.py), không key, không proxy; giãn nhịp giữa các lần gọi."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import httpx

from app.clients import youtube
from app.clients.redis import get_redis_client
from app.core.logging import get_logger
from app.repositories.d1 import youtube_hashtag_stats
from app.repositories.d1.keywords import get_enabled_keywords
from app.services.omar.omar_hashtag_stats import clean_name

logger = get_logger(__name__)

TIMEOUT_SECONDS = 20.0
# Nghỉ giữa hai hashtag: vài chục GET mỗi ngày, không cần nhanh.
PAUSE_SECONDS = 2.0
# Cùng giờ với bản chụp TikTok để hai nền tảng chụp cùng mốc.
SNAPSHOT_FROM_HOUR = 6
_DAY_KEY = "youtube_hashtag_stats:day"


async def snapshot_all(now: datetime | None = None, pause_seconds: float = PAUSE_SECONDS) -> dict[str, int]:
    """Chụp tổng video / kênh YouTube của mọi hashtag phim. Hashtag YouTube không có trang (hoặc không đọc được số) thì bỏ qua."""
    from app.services.crawl.scheduler.core import TIMEZONE

    moment = (now or datetime.now(tz=UTC)).astimezone(TIMEZONE)
    day, captured_at = moment.date().isoformat(), datetime.now(tz=UTC).isoformat()
    keywords = [k for k in await get_enabled_keywords(platform="tiktok") if str(k["keyword"]).startswith("#")]
    stats = {"hashtags": len(keywords), "saved": 0, "missing": 0, "failed": 0}
    async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT_SECONDS)) as client:
        for i, kw in enumerate(keywords):
            if i and pause_seconds:
                await asyncio.sleep(pause_seconds)
            name = clean_name(str(kw["keyword"]))
            try:
                counts = await youtube.hashtag_counts(client, name)
            except httpx.HTTPError as exc:
                stats["failed"] += 1
                logger.warning("youtube_hashtag_stats_failed", hashtag=kw["keyword"], error=exc)
                continue
            if counts is None:
                stats["missing"] += 1
                continue
            await youtube_hashtag_stats.upsert_snapshot(
                day=day,
                keyword_id=kw["id"],
                movie_id=kw["movie_id"],
                hashtag=name,
                video_count=counts["video_count"],
                channel_count=counts["channel_count"],
                captured_at=captured_at,
            )
            stats["saved"] += 1
    logger.info("youtube_hashtag_stats_finished", **stats)
    return stats


async def snapshot_due(now: datetime) -> bool:
    """Đến giờ chụp của hôm nay và chưa chụp (khoá Redis theo ngày nên restart không chụp lặp)."""
    if now.hour < SNAPSHOT_FROM_HOUR:
        return False
    redis = get_redis_client()
    day = now.date().isoformat()
    if await redis.get(_DAY_KEY) == day:
        return False
    await redis.set(_DAY_KEY, day, ex=2 * 24 * 3600)
    return True
