"""Reads/writes spider-hub's crawl_job:<platform> and
crawl_job_cancel:<run_id> Redis keys - the dashboard-facing half of the
"stop a running job" mechanism spider-hub's own crawl_request_consumer.py
implements on the other side (see its _run_subprocess/_run_spider). Same
shared-Redis pattern as platform_token.py: this side never runs the
subprocess itself, it only reads/writes the coordination keys spider-hub's
consumer sets before a crawl and polls for while one is running."""

from __future__ import annotations

import json
from typing import Any

from app.services.redis import REDIS_KEY_PREFIX, get_redis_client
from app.services.task_queue import clear_pending

# How long a stop flag stays armed - covers spider-hub's consumer being
# briefly down/slow to notice it, without leaving a stale flag around
# forever if it never does. Comfortably above crawl_request_consumer.py's
# own JOB_CANCEL_POLL_SECONDS - this is a safety ceiling, not the expected
# latency.
STOP_FLAG_TTL_SECONDS = 3600

# How long a Stop click keeps skipping still-queued crawl_requests for
# this platform (comments, keyword searches, BFS follow-ups) without
# running them. Long enough to drain a bulk "fetch comments" click.
BFS_DRAIN_TTL_SECONDS = 900


async def get_running_job(platform: str) -> dict[str, Any] | None:
    """None if no job is currently running for this platform right now -
    see crawl_request_consumer.py's _run_spider, which sets/clears this key
    around every dashboard-triggered (has a run_id) crawl subprocess."""
    client = get_redis_client()
    raw = await client.get(f"{REDIS_KEY_PREFIX}crawl_job:{platform}")
    return json.loads(raw) if raw else None


async def is_platform_draining(platform: str) -> bool:
    """True after Stop until TTL expires or a new Run clears the flags.
    The subprocess may still be shutting down; the dashboard treats the
    queue as already empty."""
    client = get_redis_client()
    return bool(await client.exists(f"{REDIS_KEY_PREFIX}platform_drain:{platform}"))


async def request_stop(platform: str) -> bool:
    """Cancels the in-flight subprocess and skips the rest of this
    platform's queued work (comments + searches) until the drain TTL
    expires. Always returns True after arming drain - a Stop click with
    nothing running still clears the waiting list.

    Arms both crawl_job_cancel:<run_id> (precise) and
    crawl_job_cancel_platform:<platform> (covers the gap before crawl_job
    is written - e.g. Facebook comments bootstrap - and any run_id race
    if a new job starts mid-Stop)."""
    client = get_redis_client()
    await client.set(f"{REDIS_KEY_PREFIX}bfs_drain:{platform}", "1", ex=BFS_DRAIN_TTL_SECONDS)
    await client.set(f"{REDIS_KEY_PREFIX}comments_drain:{platform}", "1", ex=BFS_DRAIN_TTL_SECONDS)
    await client.set(f"{REDIS_KEY_PREFIX}platform_drain:{platform}", "1", ex=BFS_DRAIN_TTL_SECONDS)
    await client.set(
        f"{REDIS_KEY_PREFIX}crawl_job_cancel_platform:{platform}",
        "1",
        ex=STOP_FLAG_TTL_SECONDS,
    )
    await clear_pending(platform)

    job = await get_running_job(platform)
    if job is not None and job.get("run_id"):
        await client.set(
            f"{REDIS_KEY_PREFIX}crawl_job_cancel:{job['run_id']}",
            "1",
            ex=STOP_FLAG_TTL_SECONDS,
        )
    return True


async def clear_drain(platform: str) -> None:
    """Clears Stop's skip flags so a new dashboard trigger actually runs.
    Kafka messages already consumed under drain are gone; this only unblocks
    work published after the user deliberately starts crawling again."""
    client = get_redis_client()
    await client.delete(
        f"{REDIS_KEY_PREFIX}bfs_drain:{platform}",
        f"{REDIS_KEY_PREFIX}comments_drain:{platform}",
        f"{REDIS_KEY_PREFIX}platform_drain:{platform}",
        f"{REDIS_KEY_PREFIX}crawl_job_cancel_platform:{platform}",
    )
