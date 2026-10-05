"""Điểm vào FastAPI - nối logging, bộ chặn request và các handler lỗi tuỳ chỉnh lại với
nhau. Chạy bằng:
    uvicorn app.main:app --reload
"""

from __future__ import annotations

import asyncio

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes.auth import router as auth_router
from app.api.routes.cron import router as cron_router
from app.api.routes.facebook import router as facebook_router
from app.api.routes.health import router as health_router
from app.api.routes.jobs import router as jobs_router
from app.api.routes.logs import router as logs_router
from app.api.routes.movies import router as movies_router
from app.api.routes.settings import router as settings_router
from app.api.routes.stats import router as stats_router
from app.api.routes.threads import router as threads_router
from app.api.routes.tiktok import router as tiktok_router
from app.clients.kafka import start_kafka_producer, stop_kafka_producer
from app.core.auth import ApiKeyMiddleware
from app.core.config import settings
from app.core.errors import register_exception_handlers
from app.core.logging import get_logger
from app.core.middleware import RequestContextMiddleware
from app.services import ops_metrics, platform_config_db, refresh_tracker, scheduler
from app.services.platforms import COMMENT_CRAWL_PLATFORMS, registered_platforms

logger = get_logger(__name__)

app = FastAPI(title="spider-api")

# Thêm trước CORS => nằm bên trong CORS, nên cả phản hồi 401 cũng mang header CORS (nếu
# không trình duyệt chỉ thấy "CORS error" thay vì 401).
if not settings.api_auth_key:
    logger.warning("api_auth_disabled", hint="set API_AUTH_KEY in .env to require X-API-Key")
app.add_middleware(ApiKeyMiddleware, api_key=settings.api_auth_key)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(RequestContextMiddleware)
register_exception_handlers(app)

app.include_router(health_router)
app.include_router(auth_router)
app.include_router(jobs_router)
app.include_router(facebook_router)
app.include_router(threads_router)
app.include_router(tiktok_router)
app.include_router(stats_router)
app.include_router(logs_router)
app.include_router(movies_router)
app.include_router(settings_router)
app.include_router(cron_router)


# ensure_default_crawl_schedules chỉ tạo sẵn một dòng mặc định cho tiện (xem docstring
# của nó) - nó tuyệt đối không được làm treo phần còn lại của quá trình khởi động (mọi
# route, scheduler, Kafka) chỉ vì Postgres không truy cập được/chậm. connect_timeout=5
# của psycopg (platform_config_db._connect) không giới hạn bước phân giải DNS, bước
# này có thể treo lâu hơn nhiều - đã gặp thực tế 2026-09-21 khi mạng chập chờn lúc
# khởi động làm treo cả app, kể cả /health, hơn 18 phút mà không có dòng log nào.
_STARTUP_SCHEDULE_SEED_TIMEOUT_SECONDS = 10.0


@app.on_event("startup")
async def on_startup() -> None:
    await start_kafka_producer()
    try:
        await asyncio.wait_for(
            platform_config_db.ensure_default_crawl_schedules(registered_platforms()),
            timeout=_STARTUP_SCHEDULE_SEED_TIMEOUT_SECONDS,
        )
    except Exception as exc:
        logger.warning("crawl_schedule_seed_failed", error=str(exc))
    try:
        await asyncio.wait_for(
            platform_config_db.ensure_default_comment_crawl_schedules(COMMENT_CRAWL_PLATFORMS),
            timeout=_STARTUP_SCHEDULE_SEED_TIMEOUT_SECONDS,
        )
    except Exception as exc:
        logger.warning("comment_crawl_schedule_seed_failed", error=str(exc))
    scheduler.start()
    ops_metrics.start_sampler()
    asyncio.create_task(_build_tab_filter_indexes())
    logger.info("app_started")


async def _build_tab_filter_indexes() -> None:
    from app.repositories.d1 import comments as comments_repo
    from app.repositories.d1 import posts as posts_repo

    # Để các query đầu tiên của dashboard chạy xong trước khi chiếm D1 bằng CREATE INDEX.
    await asyncio.sleep(8)
    try:
        await posts_repo.ensure_tab_filter_indexes()
        await comments_repo.ensure_tab_filter_indexes()
    except Exception as exc:
        logger.warning("tab_filter_indexes_failed", error=str(exc) or repr(exc))


_SHUTDOWN_STEP_TIMEOUT_SECONDS = 5


@app.on_event("shutdown")
async def on_shutdown() -> None:
    # Việc flush Kafka có giới hạn thời gian: không giới hạn thì nó từng làm
    # `uvicorn --reload` kẹt giữa lúc restart (port đã đóng, worker mới không bao giờ khởi
    # động) vào 2026-09-30.
    refresh_tracker.shutdown()
    scheduler.stop()
    ops_metrics.stop_sampler()
    try:
        await asyncio.wait_for(platform_config_db.close_pool(), timeout=_SHUTDOWN_STEP_TIMEOUT_SECONDS)
    except TimeoutError:
        logger.warning("shutdown_step_timeout", step="db_pool")
    try:
        await asyncio.wait_for(stop_kafka_producer(), timeout=_SHUTDOWN_STEP_TIMEOUT_SECONDS)
    except TimeoutError:
        logger.warning("shutdown_step_timeout", step="kafka_producer")
