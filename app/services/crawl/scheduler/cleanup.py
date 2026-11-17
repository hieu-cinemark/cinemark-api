"""Dọn bài không liên quan mỗi ngày (app/services/relevance/cleanup.py) + nút "chạy ngay" trên dashboard."""

from __future__ import annotations

from datetime import datetime

from app.clients.redis import get_redis_client
from app.core.logging import get_logger
from app.services.crawl.scheduler.core import TIMEZONE, is_due, spawn
from app.services.relevance.cleanup import resolve_cleanup_settings, run_purge

logger = get_logger(__name__)

_PURGE_KEY = "cinemark_api:cleanup:irrelevant_posts:last_run"
# Chỉ báo "running" của dashboard đọc cờ trong tiến trình này; cleanup.run_purge tự giữ khoá liên tiến trình.
_purge_running = False


async def _run_purge(triggered_by: str) -> None:
    global _purge_running
    try:
        await run_purge(triggered_by=triggered_by)
    except Exception as exc:  # noqa: BLE001 - lượt dọn bài lỗi chỉ báo lỗi (Telegram), không làm sập scheduler
        logger.error("irrelevant_purge_failed", error=str(exc), telegram=True)
    finally:
        _purge_running = False


def _start(triggered_by: str) -> bool:
    global _purge_running
    if _purge_running:
        return False
    _purge_running = True
    spawn(_run_purge(triggered_by))
    return True


async def cleanup_tick() -> None:
    if _purge_running:
        return
    # Tham số lưu trên dashboard ưu tiên hơn mặc định từ env (resolve_cleanup_settings).
    cfg = await resolve_cleanup_settings()
    now = datetime.now(TIMEZONE)
    if not cfg["enabled"] or not is_due(cfg["run_time"], now):
        return
    today = now.date().isoformat()
    redis = get_redis_client()
    if await redis.get(_PURGE_KEY) == today:
        return
    await redis.set(_PURGE_KEY, today, ex=3 * 24 * 60 * 60)
    logger.info("irrelevant_purge_firing", run_time=cfg["run_time"])
    _start("schedule")


async def run_purge_now() -> bool:
    """Kích hoạt tay từ dashboard. False nếu đang có lượt chạy (dashboard báo cho người dùng thay vì chờ mãi)."""
    return _start("manual")


def purge_in_progress() -> bool:
    return _purge_running
