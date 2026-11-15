"""Omar API: lượt hashtag TikTok mỗi giờ (app/services/omar/omar_tiktok.py), bình luận tuần, "video đang nổi"
(omar_trending) và bước BFS hashtag sau mỗi lượt."""

from __future__ import annotations

import time
from datetime import datetime

from app.clients.redis import get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.services.crawl.scheduler.core import KEY_PREFIX, TIMEZONE, claim, cycle_ttl, spawn
from app.services.crawl.scheduler.discovery import tiktok_hashtags_enabled
from app.services.discovery.hashtag_discovery import run_discovery
from app.services.omar import omar_comments, omar_trending
from app.services.omar.omar_tiktok import claim_round as claim_omar_round
from app.services.omar.omar_tiktok import clear_pause as clear_omar_pause
from app.services.omar.omar_tiktok import get_pause as get_omar_pause
from app.services.omar.omar_tiktok import is_running as omar_is_running
from app.services.omar.omar_tiktok import pause_over as omar_pause_over
from app.services.omar.omar_tiktok import run_omar_round_tracked

logger = get_logger(__name__)

_OMAR_ROUND_KEY = f"{KEY_PREFIX}omar_round:last_hour"
_BFS_DISCOVERY_KEY = f"{KEY_PREFIX}hashtag_discovery:after_omar_at"


async def _run_omar_round(trigger: str) -> None:
    try:
        await run_omar_round_tracked(trigger)
    except Exception as exc:  # noqa: BLE001 - lượt lỗi chỉ để giờ sau chạy lại
        logger.error("omar_round_failed", error=str(exc)[:300])
    # BFS: AI duyệt hashtag đi kèm mà lượt Omar vừa đếm, không đợi tới sáng hôm sau.
    try:
        redis = get_redis_client()
        last = float(await redis.get(_BFS_DISCOVERY_KEY) or 0)
        if time.time() - last >= settings.hashtag_discovery_after_omar_hours * 3600 and await tiktok_hashtags_enabled():
            await redis.set(_BFS_DISCOVERY_KEY, str(time.time()), ex=7 * 24 * 3600)
            logger.info("hashtag_discovery_after_omar")
            await run_discovery()
    except Exception as exc:  # noqa: BLE001
        logger.error("hashtag_discovery_after_omar_failed", error=str(exc)[:300])


async def _enqueue_weekly_comments(week: str) -> None:
    # Chạy nền: xếp hàng bình luận tuần mất lâu hơn một tick (một truy vấn D1 mỗi hashtag) - chạy trong tick thì bị cắt và
    # chặn luôn việc mở lượt Omar (lỗi 2026-10-12).
    try:
        await omar_comments.enqueue_weekly_job(week=week)
    except Exception as exc:  # noqa: BLE001 - khoá hết hạn sau 15 phút, tick sau thử lại
        logger.error("omar_comments_enqueue_failed", error=str(exc)[:300])


def trigger_omar_now() -> bool:
    """Nút "Chạy ngay" của tab Omar (app/api/routes/omar.py). False khi đang có lượt chạy."""
    if not claim_omar_round("manual"):
        return False
    spawn(_run_omar_round("manual"))
    return True


async def omar_tick() -> None:
    """Mỗi giờ một lượt, từ phút settings.omar_round_minute. Lượt trước chưa xong (chế độ chạy liên tục: một lượt có thể kéo
    dài nhiều giờ tới khi hết hạn mức) thì không mở lượt chồng lên. Khoá theo giờ trong Redis để restart không chạy lặp. Tới
    mốc tuần thì xếp hàng bình luận - lượt kế tiếp xử lý hàng đợi trước phần hashtag."""
    now = datetime.now(TIMEZONE)
    try:
        if await omar_comments.weekly_enqueue_due(now):
            spawn(_enqueue_weekly_comments(omar_comments.iso_week(now)))
    except Exception as exc:  # noqa: BLE001 - tick sau thử lại
        logger.error("omar_comments_enqueue_failed", error=str(exc)[:300])
    if omar_is_running():
        return
    # Đang dừng vì hết hạn mức / key sai: không gọi Omar tới lúc hết dừng, rồi mở lượt NGAY (không đợi phút
    # omar_round_minute hay giờ kế tiếp) để cào tiếp từ con trỏ đã lưu.
    resume = False
    if pause := await get_omar_pause():
        if not await omar_pause_over(pause):
            return
        await clear_omar_pause()
        resume = True
        logger.info("omar_resuming_after_pause", reason=pause.get("reason"), paused_until=pause.get("until"))
    if not resume and now.minute < settings.omar_round_minute:
        return
    hour_key = now.strftime("%Y-%m-%dT%H")
    redis = get_redis_client()
    if not resume and await redis.get(_OMAR_ROUND_KEY) == hour_key:
        return
    if not claim_omar_round("schedule"):
        return
    await redis.set(_OMAR_ROUND_KEY, hour_key, ex=2 * 3600)
    logger.info("omar_round_firing", hour=hour_key)
    spawn(_run_omar_round("schedule"))


async def _run_omar_trending() -> None:
    try:
        await omar_trending.run_refresh_tracked()
    except Exception as exc:  # noqa: BLE001 - lượt lỗi chỉ để lượt sau chạy lại
        logger.error("omar_trending_failed", error=str(exc)[:300])


async def omar_trending_tick() -> None:
    """Mỗi settings.omar_trending_interval_hours giờ, độc lập với lượt chính; chạy nền vì một lượt mất hàng chục phút."""
    if not settings.omar_trending_enabled or omar_trending.is_running():
        return
    # Hết hạn mức thì làm mới video đang lên cũng chỉ ra 429 - chờ cùng lúc với lượt chính.
    if await get_omar_pause():
        return
    ttl = cycle_ttl(settings.omar_trending_interval_hours)
    if await claim("omar_trending:last_run", ttl, datetime.now(TIMEZONE).isoformat()):
        logger.info("omar_trending_firing")
        spawn(_run_omar_trending())
