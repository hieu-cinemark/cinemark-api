"""Dashboard-visible collection queue: pending Kafka work, plus a short
history of finished/skipped/failed tasks. Written when cinemark-api
publishes a crawl_requests message; spider-hub pops/updates the same keys
as it runs (see social_crawler/services/task_queue.py). Redis, same prefix
as crawl_job:* so both processes share one list."""

from __future__ import annotations

import json
import time
from typing import Any

from app.services.redis import REDIS_KEY_PREFIX, get_redis_client

PLATFORMS = ("facebook", "threads", "tiktok")
HISTORY_LIMIT = 200


def _pending_key(platform: str) -> str:
    return f"{REDIS_KEY_PREFIX}task_pending:{platform}"


def history_key() -> str:
    return f"{REDIS_KEY_PREFIX}task_history"


def task_label(request: dict[str, Any]) -> str:
    kind = request.get("type") or "search"
    if kind == "comments":
        return str(request.get("post_id") or "")
    if kind == "channel_videos":
        username = str(request.get("username") or "").lstrip("@")
        return f"@{username}" if username else ""
    if kind == "nurture":
        return str(request.get("account") or "")
    if kind in ("refresh_token", "cookie_import"):
        return str(request.get("account_key") or request.get("account") or "")
    return str(request.get("keyword") or "")


def task_from_request(request: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": request.get("run_id") or "",
        "platform": request.get("platform") or "",
        "type": request.get("type") or "search",
        "label": task_label(request),
        "keyword_id": request.get("keyword_id"),
        "post_id": request.get("post_id"),
        "queued_at": int(time.time()),
        "status": "queued",
    }


async def enqueue_published(request: dict[str, Any]) -> None:
    platform = request.get("platform")
    if not platform:
        return
    item = task_from_request(request)
    if not item["id"]:
        return
    client = get_redis_client()
    await client.rpush(_pending_key(platform), json.dumps(item, ensure_ascii=False))


async def clear_pending(platform: str) -> None:
    """UI goes empty immediately on Stop; Kafka leftovers are skipped by
    platform_drain / comments_drain in the consumer."""
    client = get_redis_client()
    await client.delete(_pending_key(platform))


async def list_pending(platform: str) -> list[dict[str, Any]]:
    client = get_redis_client()
    raw_items = await client.lrange(_pending_key(platform), 0, -1)
    out: list[dict[str, Any]] = []
    for raw in raw_items or []:
        try:
            out.append(json.loads(raw))
        except json.JSONDecodeError:
            continue
    return out


async def list_history(limit: int = 100) -> list[dict[str, Any]]:
    client = get_redis_client()
    raw_items = await client.lrange(history_key(), 0, limit - 1)
    out: list[dict[str, Any]] = []
    for raw in raw_items or []:
        try:
            out.append(json.loads(raw))
        except json.JSONDecodeError:
            continue
    return out


async def snapshot() -> dict[str, list[dict[str, Any]]]:
    from app.services.crawl_jobs import get_running_job, is_platform_draining

    running: list[dict[str, Any]] = []
    queued: list[dict[str, Any]] = []
    for platform in PLATFORMS:
        if await is_platform_draining(platform):
            continue
        job = await get_running_job(platform)
        if job:
            running.append(
                {
                    "id": job.get("run_id") or "",
                    "platform": platform,
                    "type": job.get("type") or "search",
                    "label": (
                        job.get("keyword")
                        or (f"@{job['username']}" if job.get("username") else "")
                        or job.get("post_id")
                        or job.get("account")
                        or ""
                    ),
                    "keyword_id": job.get("keyword_id"),
                    "post_id": job.get("post_id"),
                    "started_at": job.get("started_at"),
                    "status": "running",
                }
            )
        queued.extend(await list_pending(platform))
    return {"running": running, "queued": queued, "history": await list_history()}
