"""AI tự thêm nguồn cào mỗi ngày một lần ở settings.hashtag_discovery_time: hashtag TikTok, câu tìm Facebook/Threads, trang
nguồn gợi ý (app/services/discovery/*, app/services/crawl/source_pages.py)."""

from __future__ import annotations

from datetime import datetime

from app.clients.redis import get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.services.accounts import platform_config_db as db
from app.services.crawl import threads_channels
from app.services.crawl.scheduler.core import KEY_PREFIX, TIMEZONE, is_due, spawn
from app.services.crawl.source_pages import review_suggestions as review_source_pages
from app.services.discovery.hashtag_discovery import run_discovery
from app.services.discovery.query_discovery import run_query_discovery

logger = get_logger(__name__)

_HASHTAG_DISCOVERY_KEY = f"{KEY_PREFIX}hashtag_discovery:last_run"


async def discovery_enabled() -> bool:
    """Công tắc "AI tự thêm nguồn cào" trên dashboard (bảng discovery_settings). Tắt -> không chạy hashtag_discovery,
    query_discovery, duyệt trang nguồn gợi ý, kể cả bước BFS sau lượt ShortVid: chỉ cào từ khoá / trang đang có. Đọc lỗi thì
    coi như bật (hành vi trước khi có công tắc)."""
    try:
        return bool((await db.get_discovery_settings()).get("enabled", True))
    except Exception as exc:  # noqa: BLE001
        logger.warning("discovery_settings_read_failed", error=str(exc)[:200])
        return True


async def tiktok_hashtags_enabled() -> bool:
    """AI duyệt hashtag TikTok: bật riêng bằng settings.tiktok_hashtag_discovery, hoặc theo công tắc chung."""
    return settings.tiktok_hashtag_discovery or await discovery_enabled()


async def _run_discovery(*, everything: bool) -> None:
    # everything=False (công tắc chung tắt): chỉ hashtag TikTok. Kênh Threads tự thêm theo số bài liên quan, không dùng AI -
    # chạy cả khi công tắc chung tắt. Lỗi một phần không chặn phần kia.
    jobs = [("hashtag_discovery", run_discovery), ("threads_channels_auto_add", threads_channels.auto_add_authors)]
    if everything:
        jobs += [("query_discovery", run_query_discovery), ("source_page_review", review_source_pages)]
    for name, job in jobs:
        try:
            await job()
        except Exception as exc:  # noqa: BLE001 - một lượt lỗi chỉ để mai chạy lại
            logger.error(f"{name}_failed", error=str(exc)[:300])


async def hashtag_tick() -> None:
    run_time = (settings.hashtag_discovery_time or "").strip()
    now = datetime.now(TIMEZONE)
    if not run_time or not is_due(run_time, now):
        return
    today = now.date().isoformat()
    redis = get_redis_client()
    if await redis.get(_HASHTAG_DISCOVERY_KEY) == today:
        return
    await redis.set(_HASHTAG_DISCOVERY_KEY, today, ex=3 * 24 * 3600)
    everything = await discovery_enabled()
    if not everything and not settings.tiktok_hashtag_discovery:
        logger.info("hashtag_discovery_skipped_disabled", run_time=run_time)
        return
    logger.info("hashtag_discovery_firing", run_time=run_time, tiktok_only=not everything)
    spawn(_run_discovery(everything=everything))
