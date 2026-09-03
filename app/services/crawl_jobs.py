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

# How long a stop flag stays armed - covers spider-hub's consumer being
# briefly down/slow to notice it, without leaving a stale flag around
# forever if it never does. Comfortably above crawl_request_consumer.py's
# own JOB_CANCEL_POLL_SECONDS (2s) - this is a safety ceiling, not the
# expected latency.
STOP_FLAG_TTL_SECONDS = 3600


async def get_running_job(platform: str) -> dict[str, Any] | None:
    """None if no job is currently running for this platform right now -
    see crawl_request_consumer.py's _run_spider, which sets/clears this key
    around every dashboard-triggered (has a run_id) crawl subprocess."""
    client = get_redis_client()
    raw = await client.get(f"{REDIS_KEY_PREFIX}crawl_job:{platform}")
    return json.loads(raw) if raw else None


async def request_stop(platform: str) -> bool:
    """Flags whatever's currently running for this platform to be
    cancelled. Returns False (no-op, nothing to stop) if no job is running
    right now - the caller decides what that means for the response (see
    app/api/routes/platform_scraper.py)."""
    job = await get_running_job(platform)
    if job is None:
        return False
    client = get_redis_client()
    await client.set(f"{REDIS_KEY_PREFIX}crawl_job_cancel:{job['run_id']}", "1", ex=STOP_FLAG_TTL_SECONDS)
    return True
