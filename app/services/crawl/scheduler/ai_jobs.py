"""Job AI nền (app/services/ai_jobs): gắn nhãn tác giả mỗi 2 phút, niche kênh mỗi giờ, Review Angles mỗi ~3 phút (mỗi phút khi
đang ưu tiên một phim). Mỗi job chạy nền, không chồng lượt."""

from __future__ import annotations

import time

from app.clients.redis import get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.services.ai_jobs import author_classify, channel_niches, review_angles
from app.services.crawl.scheduler.core import claim, spawn

logger = get_logger(__name__)

AI_JOB_EVERY = {"author_classify": 120, "review_angles": 200, "review_angles_focus": 60, "channel_niches": 3600}
_running: set[str] = set()
_last_run: dict[str, float] = {}


def _run(name: str, coro_factory) -> None:
    if name in _running:
        return
    _running.add(name)
    _last_run[name] = time.monotonic()

    async def runner() -> None:
        try:
            if result := await coro_factory():
                logger.info("ai_job_finished", job=name, result=result)
        except Exception as exc:  # noqa: BLE001 - lượt sau thử lại
            logger.warning("ai_job_failed", job=name, error=exc)
        finally:
            _running.discard(name)

    spawn(runner())


async def ai_jobs_tick() -> None:
    if not settings.ai_jobs_enabled:
        return
    now = time.monotonic()

    def due(key: str) -> bool:
        return now - _last_run.get(key, 0.0) >= AI_JOB_EVERY[key]

    if due("author_classify"):
        _run("author_classify", author_classify.process_author_queue)
    focus = await get_redis_client().exists(review_angles.REVIEW_FOCUS_KEY)
    if due("review_angles_focus" if focus else "review_angles"):
        _run("review_angles", review_angles.run_tick)
    if await claim("channel_niches", AI_JOB_EVERY["channel_niches"]):
        _run("channel_niches", channel_niches.process_channel_niches)
