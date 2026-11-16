"""Làm mới định kỳ bài và kênh: bài hưởng ứng trend, page phim đang chiếu, bài booking đang theo dõi, follower kênh."""

from __future__ import annotations

from datetime import datetime

from app.clients.redis import get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.services.crawl import channel_profiles, priority_channels, tracked_posts, watch_pages
from app.services.crawl.scheduler.core import KEY_PREFIX, TIMEZONE, claim, cycle_ttl, facebook_crawl_enabled, spawn
from app.services.shortvid.shortvid_tiktok import get_pause as get_shortvid_pause
from app.services.relevance.trend_posts import mark_showing_movies

logger = get_logger(__name__)

TREND_POSTS_INTERVAL_SECONDS = 3 * 3600
# Bài booking (tracked_posts): cào lại mỗi ngày một lần, từ giờ này buổi sáng.
TRACKED_POSTS_HOUR = 6
_WATCH_PAGES_KEY = f"{KEY_PREFIX}facebook_watch_pages:last_run"
_threads_collecting = False


async def _run_trend_posts() -> None:
    try:
        logger.info("trend_posts_marked", changed=await mark_showing_movies())
    except Exception as exc:  # noqa: BLE001 - lượt lỗi chỉ để lượt sau chạy lại
        logger.error("trend_posts_failed", error=str(exc)[:300])


async def trend_posts_tick() -> None:
    """Đánh dấu bài hưởng ứng trend của phim đang chiếu (app/services/relevance/trend_posts.py)."""
    if await claim("trend_posts:last_run", TREND_POSTS_INTERVAL_SECONDS, datetime.now(TIMEZONE).isoformat()):
        spawn(_run_trend_posts())


async def watch_pages_tick() -> None:
    """Quét lại page của phim đang chiếu (watch_pages). Lượt không xếp được gì (giờ yên lặng, chưa bật tài khoản) thì nhả khoá
    để nhịp sau thử lại, thay vì mất trọn một chu kỳ."""
    if not settings.facebook_watch_pages_enabled:
        return
    now = datetime.now(TIMEZONE)
    if not await claim(
        "facebook_watch_pages:last_run", cycle_ttl(settings.facebook_watch_pages_interval_hours), now.isoformat()
    ):
        return
    if not await watch_pages.queue_round(now):
        await get_redis_client().delete(_WATCH_PAGES_KEY)


async def _run_tracked_posts() -> None:
    try:
        await tracked_posts.run_daily(include_authenticated_facebook=await facebook_crawl_enabled())
    except Exception as exc:  # noqa: BLE001 - lượt lỗi: hôm sau chạy lại (hoặc restart trong ngày chạy lại)
        logger.error("tracked_posts_failed", error=str(exc)[:300])


async def tracked_posts_tick() -> None:
    now = datetime.now(TIMEZONE)
    if now.hour < TRACKED_POSTS_HOUR or tracked_posts.is_running():
        return
    if await claim(f"tracked_posts:{now.date().isoformat()}", 2 * 86400, now.isoformat()):
        logger.info("tracked_posts_firing")
        spawn(_run_tracked_posts())


async def _collect_threads_profiles() -> None:
    global _threads_collecting
    try:
        await channel_profiles.collect_threads_results()
    except Exception as exc:  # noqa: BLE001 - kết quả còn trong Redis, vòng sau gom lại
        logger.error("channel_profiles_threads_collect_failed", error=str(exc))
    finally:
        _threads_collecting = False


async def channel_profiles_tick() -> None:
    """Follower kênh (channel_profiles) mỗi settings.channel_profiles_interval_hours giờ; chạy nền vì một lượt mất vài phút.

    - Kênh ưu tiên (danh sách booking chưa có bài): crawl ngay, mỗi vòng (priority_channels).
    - Facebook / Threads: không tự crawl (spider-hub lo); kết quả spider-hub ghi vào Redis được gom mỗi vòng.
    - TikTok (ShortVid): ShortVid hết hạn mức thì chờ."""
    global _threads_collecting
    if not priority_channels.is_running():
        spawn(priority_channels.process_pending())
    if settings.channel_profiles_interval_hours <= 0:
        return
    if not _threads_collecting and await get_redis_client().hlen(channel_profiles.THREADS_RESULTS_KEY):
        _threads_collecting = True
        spawn(_collect_threads_profiles())
    if channel_profiles.is_running() or await get_shortvid_pause():
        return
    ttl = cycle_ttl(settings.channel_profiles_interval_hours)
    if await claim("channel_profiles", ttl, datetime.now(TIMEZONE).isoformat()):
        spawn(channel_profiles.refresh_followers())
