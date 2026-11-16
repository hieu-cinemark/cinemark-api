"""Bộ lập lịch chạy trong tiến trình `cinemark-scheduler`: nguồn duy nhất quyết định khi nào cào và khi nào chạy job nền.

Vòng lặp thức dậy mỗi POLL_INTERVAL_SECONDS và chạy lần lượt các nhịp trong `TICKS`. Mỗi nhịp có timeout, lỗi hay treo
của một nhịp không chặn các nhịp sau. Việc nặng được chạy nền (`core.spawn`) thay vì await trong nhịp.

| File | Nhịp |
|---|---|
| crawl_slots.py | Lượt cào bài theo lịch (`crawl_schedules`, "HH:MM" giờ Việt Nam sửa trên dashboard) |
| comments.py | Lượt cào bình luận mỗi giờ |
| cleanup.py | Dọn bài không liên quan mỗi ngày |
| discovery.py | AI tự thêm hashtag / câu tìm / trang nguồn |
| shortvid.py | ShortVid TikTok mỗi giờ + video đang nổi |
| refresh.py | Bài trend, page phim đang chiếu, bài booking, follower kênh |
| ops.py | Cảnh báo, sức khoẻ tài khoản, đối soát thống kê, report theo lịch, đồng bộ AI sang D1 |
| ai_jobs.py | Job AI nền (tác giả, niche kênh, Review Angles) |
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime

from app.core.logging import get_logger
from app.services.crawl import crawl_lanes, public_continuous, threads_channels
from app.services.crawl.comment_planner import apply_comment_completions
from app.services.crawl.scheduler import ai_jobs, cleanup, comments, crawl_slots, discovery, ops, refresh, shortvid
from app.services.crawl.scheduler.cleanup import purge_in_progress, run_purge_now
from app.services.crawl.scheduler.core import POLL_INTERVAL_SECONDS, TIMEZONE, facebook_crawl_enabled
from app.services.crawl.scheduler.shortvid import trigger_shortvid_now
from app.services.crawl.source_pages import fill_public_pages_queue

__all__ = ["purge_in_progress", "run_purge_now", "start", "stop", "trigger_shortvid_now"]

logger = get_logger(__name__)

# Một nhịp không bao giờ được làm treo vòng lặp: ngày 2026-09-30 một socket Postgres mở từ mạng trước đã làm treo lượt đó
# (và mọi lịch sau nó) suốt một đêm trong khi API vẫn trả lời /health.
TICK_TIMEOUT_SECONDS = 25


async def _news_tick() -> None:
    from app.services.post_release_news import start_tick

    start_tick()


async def _deep_crawl_tick() -> None:
    if await facebook_crawl_enabled():
        await crawl_lanes.run_deep_tick(datetime.now(TIMEZONE))


# (tên, nhịp, báo Telegram khi timeout). Thứ tự = thứ tự chạy trong mỗi vòng.
TICKS: list[tuple[str, Callable[[], Awaitable[None]], bool]] = [
    ("crawl", crawl_slots.crawl_tick, True),
    ("cleanup", cleanup.cleanup_tick, True),
    ("comments", comments.comments_tick, True),
    ("public_pages", fill_public_pages_queue, False),
    ("threads_channels", threads_channels.fill_queue, False),
    ("youtube_continuous", public_continuous.youtube_idle_tick, False),
    ("hashtag", discovery.hashtag_tick, False),
    ("shortvid", shortvid.shortvid_tick, False),
    ("shortvid_trending", shortvid.shortvid_trending_tick, False),
    ("trend_posts", refresh.trend_posts_tick, False),
    ("watch_pages", refresh.watch_pages_tick, False),
    ("channel_profiles", refresh.channel_profiles_tick, False),
    ("comment_completions", apply_comment_completions, False),
    ("ops_alerts", ops.ops_alerts_tick, False),
    ("health_checks", ops.health_checks_tick, False),
    ("tracked_posts", refresh.tracked_posts_tick, False),
    ("ai_jobs", ai_jobs.ai_jobs_tick, False),
    ("ai_runtime", ops.ai_runtime_tick, False),
    ("report_schedule", ops.report_schedule_tick, False),
    ("news", _news_tick, False),
    ("stats_reconcile", ops.stats_reconcile_tick, False),
    ("deep_crawl", _deep_crawl_tick, False),
]


async def run_tick(name: str, tick: Callable[[], Awaitable[None]], alert: bool) -> None:
    """Một nhịp: lỗi (D1/Kafka/DB trục trặc) hay treo chỉ được log, không giết vòng lặp."""
    try:
        await asyncio.wait_for(tick(), timeout=TICK_TIMEOUT_SECONDS)
    except TimeoutError:
        logger.error(f"scheduler_{name}_tick_timeout", timeout_seconds=TICK_TIMEOUT_SECONDS, telegram=alert)
    except Exception as exc:  # noqa: BLE001
        logger.error(f"scheduler_{name}_tick_failed", error=str(exc)[:300])


async def _loop() -> None:
    while True:
        for name, tick, alert in TICKS:
            await run_tick(name, tick, alert)
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


_task: asyncio.Task[None] | None = None


def start() -> None:
    global _task
    if _task is None:
        _task = asyncio.create_task(_loop())
        logger.info("scheduler_started")


def stop() -> None:
    from app.services.post_release_news import stop as stop_news

    stop_news()
    global _task
    if _task is not None:
        _task.cancel()
        _task = None
