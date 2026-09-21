"""GET /jobs - running, waiting, and recent finished collection tasks."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

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
