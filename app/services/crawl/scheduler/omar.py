"""Omar API: lượt hashtag TikTok (app/services/omar/omar_tiktok.py) chạy liên tục khi key đúng + xếp hàng bình luận hằng ngày."""

from __future__ import annotations

from datetime import datetime

from app.clients.redis import get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.services.crawl.scheduler.core import KEY_PREFIX, TIMEZONE, spawn
from app.services.omar import omar_comments
from app.services.omar.omar_pause import clear_pause as clear_omar_pause
from app.services.omar.omar_pause import get_pause as get_omar_pause
from app.services.omar.omar_pause import pause_over as omar_pause_over
from app.services.omar.omar_tiktok import claim_round as claim_omar_round
from app.services.omar.omar_tiktok import is_running as omar_is_running
from app.services.omar.omar_tiktok import run_omar_round_tracked

logger = get_logger(__name__)

_OMAR_ROUND_KEY = f"{KEY_PREFIX}omar_round:last_hour"


async def _run_omar_round(trigger: str) -> None:
    try:
        await run_omar_round_tracked(trigger)
    except Exception as exc:  # noqa: BLE001 - lượt lỗi chỉ để giờ sau chạy lại
        logger.error("omar_round_failed", error=str(exc)[:300])


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
