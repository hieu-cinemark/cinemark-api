"""ShortVid API: lượt hashtag TikTok mỗi giờ (app/services/shortvid/shortvid_tiktok.py), bình luận tuần, "video đang nổi"
(shortvid_trending) (không còn bước AI duyệt hashtag sau mỗi lượt, bỏ 17/11/2026)."""

from __future__ import annotations

from datetime import datetime

from app.clients.redis import get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.services.crawl.scheduler.core import KEY_PREFIX, TIMEZONE, claim, cycle_ttl, spawn
from app.services.shortvid import shortvid_comments, shortvid_trending
from app.services.shortvid.shortvid_pause import clear_pause as clear_shortvid_pause
from app.services.shortvid.shortvid_pause import get_pause as get_shortvid_pause
from app.services.shortvid.shortvid_pause import pause_over as shortvid_pause_over
from app.services.shortvid.shortvid_tiktok import claim_round as claim_shortvid_round
from app.services.shortvid.shortvid_tiktok import is_running as shortvid_is_running
from app.services.shortvid.shortvid_tiktok import run_shortvid_round_tracked

logger = get_logger(__name__)

_SHORTVID_ROUND_KEY = f"{KEY_PREFIX}shortvid_round:last_hour"


async def _run_shortvid_round(trigger: str) -> None:
    try:
        await run_shortvid_round_tracked(trigger)
    except Exception as exc:  # noqa: BLE001 - lượt lỗi chỉ để giờ sau chạy lại
        logger.error("shortvid_round_failed", error=str(exc)[:300])


async def _enqueue_weekly_comments(week: str) -> None:
    # Chạy nền: xếp hàng bình luận tuần mất lâu hơn một tick (một truy vấn D1 mỗi hashtag) - chạy trong tick thì bị cắt và
    # chặn luôn việc mở lượt ShortVid (lỗi 2026-10-12).
    try:
        await shortvid_comments.enqueue_weekly_job(week=week)
    except Exception as exc:  # noqa: BLE001 - khoá hết hạn sau 15 phút, tick sau thử lại
        logger.error("shortvid_comments_enqueue_failed", error=str(exc)[:300])


def trigger_shortvid_now() -> bool:
    """Nút "Chạy ngay" của tab ShortVid (app/api/routes/shortvid.py). False khi đang có lượt chạy."""
    if not claim_shortvid_round("manual"):
        return False
    spawn(_run_shortvid_round("manual"))
    return True


async def shortvid_tick() -> None:
    """Mỗi giờ một lượt, từ phút settings.shortvid_round_minute. Lượt trước chưa xong (chế độ chạy liên tục: một lượt có thể kéo
    dài nhiều giờ tới khi hết hạn mức) thì không mở lượt chồng lên. Khoá theo giờ trong Redis để restart không chạy lặp. Tới
    mốc tuần thì xếp hàng bình luận - lượt kế tiếp xử lý hàng đợi trước phần hashtag."""
    now = datetime.now(TIMEZONE)
    try:
        if await shortvid_comments.weekly_enqueue_due(now):
            spawn(_enqueue_weekly_comments(shortvid_comments.iso_week(now)))
    except Exception as exc:  # noqa: BLE001 - tick sau thử lại
        logger.error("shortvid_comments_enqueue_failed", error=str(exc)[:300])
    if shortvid_is_running():
        return
    # Đang dừng vì hết hạn mức / key sai: không gọi ShortVid tới lúc hết dừng, rồi mở lượt NGAY (không đợi phút
    # shortvid_round_minute hay giờ kế tiếp) để cào tiếp từ con trỏ đã lưu.
    resume = False
    if pause := await get_shortvid_pause():
        if not await shortvid_pause_over(pause):
            return
        await clear_shortvid_pause()
        resume = True
        logger.info("shortvid_resuming_after_pause", reason=pause.get("reason"), paused_until=pause.get("until"))
    if not resume and now.minute < settings.shortvid_round_minute:
        return
    hour_key = now.strftime("%Y-%m-%dT%H")
    redis = get_redis_client()
    if not resume and await redis.get(_SHORTVID_ROUND_KEY) == hour_key:
        return
    if not claim_shortvid_round("schedule"):
        return
    await redis.set(_SHORTVID_ROUND_KEY, hour_key, ex=2 * 3600)
    logger.info("shortvid_round_firing", hour=hour_key)
    spawn(_run_shortvid_round("schedule"))


async def _run_shortvid_trending() -> None:
    try:
        await shortvid_trending.run_refresh_tracked()
    except Exception as exc:  # noqa: BLE001 - lượt lỗi chỉ để lượt sau chạy lại
        logger.error("shortvid_trending_failed", error=str(exc)[:300])


async def shortvid_trending_tick() -> None:
    """Mỗi settings.shortvid_trending_interval_hours giờ, độc lập với lượt chính; chạy nền vì một lượt mất hàng chục phút."""
    if not settings.shortvid_trending_enabled or shortvid_trending.is_running():
        return
    # Hết hạn mức thì làm mới video đang lên cũng chỉ ra 429 - chờ cùng lúc với lượt chính.
    if await get_shortvid_pause():
        return
    ttl = cycle_ttl(settings.shortvid_trending_interval_hours)
    if await claim("shortvid_trending:last_run", ttl, datetime.now(TIMEZONE).isoformat()):
        logger.info("shortvid_trending_firing")
        spawn(_run_shortvid_trending())
