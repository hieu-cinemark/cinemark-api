"""Nhịp của Attack Detection (docs/review-angles-va-attack.md mục 2 và 4).

- comment_hourly_tick: mỗi COMMENT_HOURLY_EVERY_SECONDS tính lại COMMENT_HOURLY_LOOKBACK_HOURS giờ gần nhất của bảng
  movie_comment_hourly (dấu hiệu A - chê dồn dập). Đếm theo giờ đăng nên bình luận về muộn vẫn vào đúng giờ của nó; khoảng
  ngắn để câu tổng hợp chỉ chạm vài giờ bình luận qua index comments(posted_at).
- attack_detect_tick: mỗi giờ (từ phút ATTACK_DETECT_FROM_MINUTE, sau timer embedding cinemark-embed lúc phút 0) chạy
  app/services/ai_jobs/attack_detect.run_tick cho mọi phim đang chiếu, chạy nền (một lượt có thể mất vài phút).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.core.logging import get_logger
from app.repositories.d1.comment_hourly import rebuild_hours
from app.services.crawl.scheduler.core import claim, spawn

logger = get_logger(__name__)

COMMENT_HOURLY_EVERY_SECONDS = 600
COMMENT_HOURLY_LOOKBACK_HOURS = 3
ATTACK_DETECT_EVERY_SECONDS = 3600
ATTACK_DETECT_FROM_MINUTE = 15


async def comment_hourly_tick() -> None:
    if not await claim("comment_hourly", COMMENT_HOURLY_EVERY_SECONDS):
        return
    now = datetime.now(tz=UTC).replace(minute=0, second=0, microsecond=0)
    if not await rebuild_hours(now - timedelta(hours=COMMENT_HOURLY_LOOKBACK_HOURS), now + timedelta(hours=1)):
        logger.warning("comment_hourly_rebuild_failed")


async def attack_detect_tick() -> None:
    if datetime.now(tz=UTC).minute < ATTACK_DETECT_FROM_MINUTE:
        return
    if not await claim("attack_detect", ATTACK_DETECT_EVERY_SECONDS - 120):
        return
    from app.services.ai_jobs.attack_detect import run_tick

    async def job() -> None:
        try:
            logger.info("attack_detect_finished", statuses=await run_tick())
        except Exception as exc:  # noqa: BLE001 - giờ sau chạy lại
            logger.warning("attack_detect_tick_failed", error=exc)

    spawn(job())
