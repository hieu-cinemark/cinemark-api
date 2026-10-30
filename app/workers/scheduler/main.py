"""Bộ lập lịch chạy thành service riêng (30/10/2026), tách khỏi tiến trình web: lượt crawl theo lịch, comment mỗi giờ,
Omar, đối soát thống kê, dọn dẹp, báo cáo... (app/services/crawl/scheduler.py) cùng bộ lấy mẫu ops (ops_metrics). Trước
đây chạy trong cinemark-api-web nên một tác vụ nặng làm chậm dashboard, và không chạy được nhiều worker uvicorn (mỗi
worker sẽ có một bộ lập lịch).

    python -m app.workers.scheduler.main

Chạy service này thì đặt SCHEDULER_IN_WEB=false cho web (settings.scheduler_in_web) - không chạy cả hai cùng lúc.
"""

from __future__ import annotations

import asyncio
import signal

from app.clients.kafka import start_kafka_producer, stop_kafka_producer
from app.core.logging import get_logger
from app.services.accounts import platform_config_db
from app.services.crawl import scheduler
from app.services.crawl.platforms import COMMENT_CRAWL_PLATFORMS, registered_platforms
from app.services.ops import ops_metrics

logger = get_logger(__name__)

_SEED_TIMEOUT_SECONDS = 10.0
_SHUTDOWN_STEP_TIMEOUT_SECONDS = 5


async def _seed_schedules() -> None:
    """Dòng lịch mặc định cho nền tảng mới (giống lúc web khởi động) - không bao giờ được làm treo service."""
    for coro, name in (
        (platform_config_db.ensure_default_crawl_schedules(registered_platforms()), "crawl"),
        (platform_config_db.ensure_default_comment_crawl_schedules(COMMENT_CRAWL_PLATFORMS), "comment"),
    ):
        try:
            await asyncio.wait_for(coro, timeout=_SEED_TIMEOUT_SECONDS)
        except Exception as exc:  # noqa: BLE001
            logger.warning("schedule_seed_failed", schedule=name, error=str(exc))


async def main() -> None:
    await start_kafka_producer()
    await _seed_schedules()
    scheduler.start()
    ops_metrics.start_sampler()
    logger.info("scheduler_service_started")

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    await stop.wait()

    logger.info("scheduler_service_stopping")
    scheduler.stop()
    ops_metrics.stop_sampler()
    for step, coro in (("db_pool", platform_config_db.close_pool()), ("kafka_producer", stop_kafka_producer())):
        try:
            await asyncio.wait_for(coro, timeout=_SHUTDOWN_STEP_TIMEOUT_SECONDS)
        except TimeoutError:
            logger.warning("shutdown_step_timeout", step=step)


if __name__ == "__main__":
    asyncio.run(main())
