"""Hàng đợi thu thập hiển thị trên dashboard: công việc Kafka đang chờ, cộng một lịch sử
ngắn các task đã xong/bỏ qua/thất bại. Được ghi khi cinemark-api publish một message
crawl_requests; spider-hub pop/cập nhật cùng các key đó khi chạy (xem
social_crawler/services/task_queue.py). Dùng Redis, cùng prefix với crawl_job:* để hai
tiến trình dùng chung một danh sách."""

from __future__ import annotations

import json
import time
from typing import Any

from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client

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
        "bypass_drain": bool(request.get("bypass_drain")),
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
    """Giao diện trống ngay khi bấm Dừng; các message Kafka còn sót được consumer bỏ qua nhờ
    platform_drain / comments_drain."""
    client = get_redis_client()
    await client.delete(_pending_key(platform))


async def remove_pending(platform: str, run_id: str) -> bool:
    """Chỉ xoá đúng một mục còn đang chờ theo run_id của nó, cho nút Dừng trên từng dòng
    (xem crawl_jobs.cancel_job) - khác với clear_pending, mọi mục đang chờ khác của nền
    tảng này được giữ nguyên. Mục này chỉ là một dấu "hiện là đang chờ" phía dashboard
    (xem enqueue_published); bản thân nó không bao giờ được consume từ một topic Kafka,
    nên xoá nó khỏi list Redis này là đủ - phía spider-hub không còn gì cần được báo để
    bỏ qua nó."""
    client = get_redis_client()
    raw_items = await client.lrange(_pending_key(platform), 0, -1)
    for raw in raw_items or []:
        try:
            item = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if item.get("id") == run_id:
            removed = await client.lrem(_pending_key(platform), 1, raw)
            return removed > 0
    return False


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
        # Job đang chạy vẫn được hiển thị kể cả khi nền tảng đang drain sau khi bấm Dừng: các
        # lần kích hoạt có chủ đích (nurture, comments) vẫn chạy tiếp qua đó.
        draining = await is_platform_draining(platform)
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
        # Lần Dừng đã xoá danh sách; mọi thứ xếp hàng sau đó mà không có bypass_drain sẽ bị bỏ
        # qua, nên không hiển thị là đang chờ.
        queued.extend(item for item in await list_pending(platform) if not draining or item.get("bypass_drain"))
    return {"running": running, "queued": queued, "history": await list_history()}
