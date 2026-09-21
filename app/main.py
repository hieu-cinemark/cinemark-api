"""FastAPI entrypoint - wires logging, the request interceptor, and the
custom error handlers together. Run with:
    uvicorn app.main:app --reload
"""

from __future__ import annotations

import asyncio

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

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
from app.core.config import settings
from app.core.errors import register_exception_handlers
from app.core.logging import get_logger
from app.core.middleware import RequestContextMiddleware
from app.services import platform_config_db, refresh_tracker, scheduler
from app.services.kafka import start_kafka_producer, stop_kafka_producer
from app.services.platforms import registered_platforms

logger = get_logger(__name__)

app = FastAPI(title="spider-api")

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
app.include_router(jobs_router)
app.include_router(facebook_router)
app.include_router(threads_router)
app.include_router(tiktok_router)
app.include_router(stats_router)
app.include_router(logs_router)
app.include_router(movies_router)
app.include_router(settings_router)
app.include_router(cron_router)


# ensure_default_crawl_schedules just seeds a nice-to-have default row (see
# its own docstring) - it must never be able to hold up the rest of startup
# (every route, the scheduler, Kafka) behind an unreachable/slow Postgres.
# psycopg's own connect_timeout=5 (platform_config_db._connect) doesn't
# bound DNS resolution, which can hang well past that - hit live 2026-09-21
# when a network blip during startup stalled the whole app, including
# /health, for 18+ minutes with zero log output.
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
    scheduler.start()
    logger.info("app_started")


@app.on_event("shutdown")
async def on_shutdown() -> None:
    refresh_tracker.shutdown()
    scheduler.stop()
    await stop_kafka_producer()
