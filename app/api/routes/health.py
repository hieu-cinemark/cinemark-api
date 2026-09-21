"""Liveness check + a debug route that deliberately raises AppError, so the
error-handling wiring in main.py can be smoke-tested with a single curl
instead of needing a real failing resource lookup somewhere."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from app.core.errors import NotFoundError
from app.services.ops_metrics import record_and_list_samples

router = APIRouter(tags=["health"])


class OpsMetricPoint(BaseModel):
    ts: int
    load_1: float
    load_5: float = 0
    load_15: float = 0
    rss_mb: float
    running: int
    queued: int
    done_15m: int = 0
    failed_15m: int = 0


class OpsMetricsResponse(BaseModel):
    current: OpsMetricPoint
    series: list[OpsMetricPoint]


@router.get("/health")
async def health() -> dict[str, bool]:
    return {"ok": True}


@router.get("/health/metrics", response_model=OpsMetricsResponse)
async def health_metrics() -> OpsMetricsResponse:
    data: dict[str, Any] = await record_and_list_samples()
    return OpsMetricsResponse(
        current=OpsMetricPoint(**data["current"]),
        series=[OpsMetricPoint(**row) for row in data["series"]],
    )


@router.get("/health/error-demo")
async def error_demo() -> None:
    raise NotFoundError("This is what a custom AppError looks like as a response.")
