"""Ops metrics for the dashboard performance chart.

Samples host load + this API process RSS + crawl queue depth, and keeps a
short Redis ring buffer so the overview can draw a live line chart without
a separate Prometheus stack. Each GET /health/metrics may append at most
one sample (throttled) then returns the series.
"""

from __future__ import annotations

import json
import os
import resource
import sys
import time
from typing import Any

from app.services.redis import REDIS_KEY_PREFIX, get_redis_client
from app.services.task_queue import PLATFORMS, list_history, list_pending

SAMPLES_KEY = f"{REDIS_KEY_PREFIX}ops_metrics_samples"
SAMPLES_LIMIT = 90
SAMPLE_MIN_INTERVAL_SECONDS = 15
HISTORY_WINDOW_SECONDS = 15 * 60


def _rss_mb() -> float:
    # macOS reports bytes; Linux reports kilobytes.
    rss = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    if sys.platform != "darwin":
        rss *= 1024.0
    return round(rss / (1024.0 * 1024.0), 2)


def _load_averages() -> tuple[float, float, float]:
    try:
        load1, load5, load15 = os.getloadavg()
        return round(load1, 2), round(load5, 2), round(load15, 2)
    except OSError:
        return 0.0, 0.0, 0.0


async def _queue_counts() -> tuple[int, int]:
    from app.services.crawl_jobs import get_running_job, is_platform_draining

    running = 0
    queued = 0
    for platform in PLATFORMS:
        if await is_platform_draining(platform):
            continue
        if await get_running_job(platform):
            running += 1
        queued += len(await list_pending(platform))
    return running, queued


async def _history_window_counts(now: int) -> tuple[int, int]:
    cutoff = now - HISTORY_WINDOW_SECONDS
    done = 0
    failed = 0
    for row in await list_history(200):
        finished = row.get("finished_at")
        if not isinstance(finished, int) or finished < cutoff:
            continue
        status = row.get("status")
        if status == "done":
            done += 1
        elif status == "failed":
            failed += 1
    return done, failed


async def collect_sample() -> dict[str, Any]:
    now = int(time.time())
    load1, load5, load15 = _load_averages()
    running, queued = await _queue_counts()
    done_15m, failed_15m = await _history_window_counts(now)
    return {
        "ts": now,
        "load_1": load1,
        "load_5": load5,
        "load_15": load15,
        "rss_mb": _rss_mb(),
        "running": running,
        "queued": queued,
        "done_15m": done_15m,
        "failed_15m": failed_15m,
    }


async def record_and_list_samples() -> dict[str, Any]:
    """Throttle-append the latest sample, then return current + series."""
    client = get_redis_client()
    sample = await collect_sample()

    raw_latest = await client.lindex(SAMPLES_KEY, 0)
    should_append = True
    if raw_latest:
        try:
            latest = json.loads(raw_latest)
            if sample["ts"] - int(latest.get("ts") or 0) < SAMPLE_MIN_INTERVAL_SECONDS:
                should_append = False
        except (json.JSONDecodeError, TypeError, ValueError):
            pass

    if should_append:
        await client.lpush(SAMPLES_KEY, json.dumps(sample, ensure_ascii=False))
        await client.ltrim(SAMPLES_KEY, 0, SAMPLES_LIMIT - 1)

    raw_items = await client.lrange(SAMPLES_KEY, 0, SAMPLES_LIMIT - 1)
    series: list[dict[str, Any]] = []
    for raw in reversed(raw_items or []):
        try:
            series.append(json.loads(raw))
        except json.JSONDecodeError:
            continue

    return {"current": sample, "series": series}
