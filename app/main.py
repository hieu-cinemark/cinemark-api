"""FastAPI entrypoint - wires logging, the request interceptor, and the
custom error handlers together. Run with:
    uvicorn app.main:app --reload
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes.facebook import router as facebook_router
from app.api.routes.health import router as health_router
from app.core.config import settings
from app.core.errors import register_exception_handlers
from app.core.logging import get_logger
from app.core.middleware import RequestContextMiddleware
from app.services.kafka import start_kafka_producer, stop_kafka_producer

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
app.include_router(facebook_router)


@app.on_event("startup")
async def on_startup() -> None:
    await start_kafka_producer()
    logger.info("app_started")


@app.on_event("shutdown")
async def on_shutdown() -> None:
    await stop_kafka_producer()
