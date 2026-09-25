"""In-process background queue for social-topic report generation -
backs the dashboard's "Tạo report" button (POST /movies/{id}/generate-
report, app/api/routes/movies.py). Added 2026-09-25: that route used to
run generate_report_for_movie synchronously in the request handler (two
sequential Bee/Kira calls, confirmed live the topics-clustering call alone
can take 2+ minutes), and the dashboard's own mutation state disabled
every OTHER row's button while one was in flight - so an operator could
never have more than one report generating at a time. Enqueuing here lets
the HTTP call return immediately and lets several movies' reports run
concurrently.

In-memory, not Redis: this deployment is one uvicorn process (see
Dockerfile's CMD - no --workers), so a plain dict is visible to every
request without the serialization cost of a shared store, same tradeoff
crawl_jobs.py/task_queue.py made the opposite way only because THEY
coordinate across this process and spider-hub's separate one. If this
API ever runs multiple workers/replicas, this needs to move to Redis
(same shape as crawl_jobs.py) - a status GET landing on the "wrong"
worker would otherwise show stale/missing state.

No explicit concurrency cap here - app.ai_client's own per-provider
semaphore (asyncio.Semaphore(2), see call_ai) already bounds how many
Bee/Kira calls run at once; enqueuing more movies than that just means
the extra ones sit inside generate_report_for_movie awaiting that
semaphore, which is exactly the same queueing behavior, just with the
existing single choke point instead of a second one duplicating it here."""

from __future__ import annotations

import asyncio
import time
from typing import Any, Literal

from app.core.logging import get_logger
from app.services.social_topic import ReportResult, generate_report_for_movie, get_movie_for_report

logger = get_logger(__name__)

JobStatus = Literal["queued", "running", "done", "failed"]

# Pruned lazily (see _prune) rather than on a timer - a job's entry only
# needs to survive long enough for the dashboard's post-enqueue polling to
# see its final state, not forever.
_JOB_TTL_SECONDS = 3600.0

_jobs: dict[str, dict[str, Any]] = {}


def _prune() -> None:
    cutoff = time.monotonic() - _JOB_TTL_SECONDS
    stale = [
        movie_id
        for movie_id, job in _jobs.items()
        if job["status"] in ("done", "failed") and job["updated_at"] < cutoff
    ]
    for movie_id in stale:
        del _jobs[movie_id]


def get_report_job(movie_id: str) -> dict[str, Any] | None:
    """None if no job has ever been enqueued for this movie (or its entry
    already aged out - see _JOB_TTL_SECONDS) - the dashboard treats that
    the same as "nothing to show", not an error."""
    return _jobs.get(movie_id)


async def enqueue_report_job(movie_id: str) -> dict[str, Any]:
    """Idempotent: a movie already queued/running just returns its
    existing job instead of starting a second concurrent run of itself -
    a double-click shouldn't race two generate_report_for_movie calls
    against the same social_topic_reports row."""
    _prune()
    existing = _jobs.get(movie_id)
    if existing is not None and existing["status"] in ("queued", "running"):
        return existing

    job: dict[str, Any] = {
        "movie_id": movie_id,
        "status": "queued",
        "result": None,
        "error": None,
        "queued_at": time.monotonic(),
        "started_at": None,
        "finished_at": None,
        "updated_at": time.monotonic(),
    }
    _jobs[movie_id] = job
    asyncio.create_task(_run(movie_id))
    return job


async def _run(movie_id: str) -> None:
    job = _jobs[movie_id]
    job["status"] = "running"
    job["started_at"] = time.monotonic()
    job["updated_at"] = time.monotonic()
    try:
        movie = await get_movie_for_report(movie_id)
        if movie is None:
            raise ValueError("movie_not_found")
        result: ReportResult = await generate_report_for_movie(movie)
        job["result"] = result
        job["status"] = "failed" if result != "generated" else "done"
        if result != "generated":
            job["error"] = result
    except Exception as exc:
        logger.exception("report_job_failed", movie_id=movie_id)
        job["status"] = "failed"
        job["error"] = str(exc) or repr(exc)
    finally:
        job["finished_at"] = time.monotonic()
        job["updated_at"] = time.monotonic()
