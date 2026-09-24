"""GET /jobs - running, waiting, and recent finished collection tasks."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from app.services.kafka import get_consumer_lag
from app.services.task_queue import snapshot

router = APIRouter(prefix="/jobs", tags=["jobs"])


class JobTask(BaseModel):
    id: str = ""
    platform: str
    type: str = "search"
    label: str = ""
    keyword_id: str | None = None
    post_id: str | None = None
    status: str = "queued"
    queued_at: int | None = None
    started_at: int | None = None
    finished_at: int | None = None
    error: str | None = None


class JobsSnapshot(BaseModel):
    running: list[JobTask]
    queued: list[JobTask]
    history: list[JobTask]


@router.get("", response_model=JobsSnapshot)
async def jobs() -> JobsSnapshot:
    data: dict[str, list[dict[str, Any]]] = await snapshot()
    return JobsSnapshot(
        running=[JobTask(**row) for row in data["running"]],
        queued=[JobTask(**row) for row in data["queued"]],
        history=[JobTask(**row) for row in data["history"]],
    )


class KafkaLagEntry(BaseModel):
    label: str
    topic: str
    group_id: str
    lag: int | None
    error: str | None = None


@router.get("/kafka-lag", response_model=list[KafkaLagEntry])
async def kafka_lag() -> list[KafkaLagEntry]:
    """Real, broker-computed backlog per consumer group (app.services.kafka.
    get_consumer_lag) - a different, ground-truth number from the `queued`
    array above, which is app-tracked bookkeeping in Redis that can get
    stuck if a consumer never gets to clear an entry (see that function's
    own docstring for the incident that motivated adding this route)."""
    rows = await get_consumer_lag()
    return [KafkaLagEntry(**row) for row in rows]
