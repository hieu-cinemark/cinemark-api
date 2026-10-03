"""Daily purge of irrelevant post data from D1.

Removes posts the relevance pipeline labelled not_related (plus their
comments and engagement snapshots) once they've carried that label for
grace_hours. Every read path already filters these out
(app/repositories/d1/posts.py's RELEVANT_POST_SQL, the worker's
social-topic query), so they only cost D1 storage and row reads.

One scan per run picks the ids to delete (up to MAX_BATCHES * BATCH_SIZE,
oldest id first); deletes then go batch by batch against those explicit
ids, children before the post, so a comment is never left behind for a
post that's gone and a crash mid-run is simply resumed by the next run. A
large backlog is worked off over several days instead of in one long run -
each statement stays well inside D1's per-statement time limit, and the
whole run stays far below the Cloudflare API rate limit the dashboard and
ingest share.

The historical `dropped_posts` purge is gone - the lake writer
(app/workers/lake_writer/main.py) now owns the archive of every drop
decision via the ingest_decisions Kafka topic (see
app/clients/kafka.py:publish_ingest_decision), so there is nothing left
in D1 to age out."""

from __future__ import annotations

import uuid
from typing import Any

from app.clients.redis import get_redis_client
from app.core.config import settings
from app.core.logging import get_logger
from app.services import platform_config_db as db
from app.services.d1 import d1_query

logger = get_logger(__name__)

BATCH_SIZE = 500
MAX_BATCHES = 60

# Grace is measured from when the post got its not_related label, not from
# when it was scraped - a relabel sweep that downgrades old posts must still
# leave the operator grace_hours to notice and revert it.
_ELIGIBLE = (
    "relevance_label = 'not_related' "
    "AND COALESCE(relevance_labeled_at, scraped_at) < strftime('%Y-%m-%dT%H:%M:%S', 'now', ?)"
)

# Shared by the API's scheduler and scripts/purge_irrelevant_posts.py, so a
# manual run can't overlap a scheduled one in another process.
_LOCK_KEY = "cinemark_api:cleanup:irrelevant_posts:lock"
_LOCK_TTL_SECONDS = 2 * 60 * 60


async def _run(sql: str, params: list[Any]) -> list[dict[str, Any]]:
    rows = await d1_query(sql, params, timeout=60.0)
    if rows is None:
        raise RuntimeError(f"d1 statement failed: {sql[:80]}")
    return rows


def _id_list(ids: list[str]) -> str:
    # Inlined as quoted literals rather than bound: D1 caps a statement at
    # 100 bound parameters, and one statement per batch keeps a run to a
    # few hundred API calls. The ids come straight from the SELECT above.
    return ",".join("'" + str(i).replace("'", "''") + "'" for i in ids)


async def resolve_cleanup_settings() -> dict[str, Any]:
    """Effective cleanup knobs: the dashboard-stored cleanup_settings row
    for any key it has, the env fallback from app/core/config.py
    otherwise. Always the same shape, so callers need no conditionals."""
    row = await db.get_cleanup_settings()
    stored = row.get("settings") if isinstance(row.get("settings"), dict) else {}
    grace = stored.get("grace_hours")
    return {
        "run_time": str(stored.get("run_time") or settings.irrelevant_post_purge_time),
        "enabled": bool(stored.get("enabled", settings.irrelevant_post_purge_enabled)),
        # `is None`, not `or`: 0 is a valid stored value ("purge right away").
        "grace_hours": int(grace if grace is not None else settings.irrelevant_post_grace_hours),
        "updated_at": row.get("updated_at"),
    }


async def purge_irrelevant_posts(
    *,
    dry_run: bool = False,
    grace_hours: int | None = None,
) -> dict[str, Any]:
    """One purge pass. grace_hours defaults to the env fallback - callers
    that should honor the dashboard (the scheduler, the Run-now button, the
    script) go through run_purge, which resolves the stored value."""
    grace_value = max(0, grace_hours if grace_hours is not None else settings.irrelevant_post_grace_hours)
    grace = f"-{grace_value} hours"

    if dry_run:
        posts = await _run(f"SELECT count(*) AS n FROM posts WHERE {_ELIGIBLE}", [grace])
        comments = await _run(
            f"SELECT count(*) AS n FROM comments WHERE post_id IN (SELECT id FROM posts WHERE {_ELIGIBLE})", [grace]
        )
        result = {"dry_run": True, "grace_hours": grace_value, "posts": posts[0]["n"], "comments": comments[0]["n"]}
        logger.info("irrelevant_purge_dry_run", **result)
        return result

    ids = [
        row["id"]
        for row in await _run(
            f"SELECT id FROM posts WHERE {_ELIGIBLE} ORDER BY id LIMIT ?", [grace, BATCH_SIZE * MAX_BATCHES]
        )
    ]
    totals: dict[str, Any] = {"posts": 0, "comments": 0, "snapshots": 0, "batches": 0}
    for start in range(0, len(ids), BATCH_SIZE):
        id_list = _id_list(ids[start : start + BATCH_SIZE])
        totals["comments"] += len(await _run(f"DELETE FROM comments WHERE post_id IN ({id_list}) RETURNING id", []))
        totals["snapshots"] += len(
            await _run(f"DELETE FROM post_engagement_snapshots WHERE post_id IN ({id_list}) RETURNING id", [])
        )
        totals["posts"] += len(await _run(f"DELETE FROM posts WHERE id IN ({id_list}) RETURNING id", []))
        totals["batches"] += 1

    backlog = await _run(f"SELECT count(*) AS n FROM posts WHERE {_ELIGIBLE}", [grace])
    totals["remaining_posts"] = backlog[0]["n"]
    totals["grace_hours"] = grace_value
    logger.info("irrelevant_purge_done", telegram=True, **totals)
    return totals


async def run_purge(*, triggered_by: str) -> dict[str, Any] | None:
    """Full purge run as the scheduler, the dashboard's Run-now and the
    manual script all do it: take the cross-process lock, resolve the
    dashboard's grace_hours, record the run in cleanup_run_history.
    Returns the summary, or None when another run already holds the lock.
    Errors are recorded on the history row and re-raised."""
    redis = get_redis_client()
    token = uuid.uuid4().hex
    if not await redis.set(_LOCK_KEY, token, nx=True, ex=_LOCK_TTL_SECONDS):
        logger.warning("irrelevant_purge_already_running", triggered_by=triggered_by)
        return None
    run_id: int | None = None
    try:
        cfg = await resolve_cleanup_settings()
        run_id = await db.record_cleanup_run_start(dry_run=False, triggered_by=triggered_by)
        summary = await purge_irrelevant_posts(grace_hours=cfg["grace_hours"])
        await db.record_cleanup_run_finish(run_id, summary=summary)
        return summary
    except Exception as exc:
        if run_id is not None:
            try:
                await db.record_cleanup_run_finish(run_id, summary={}, error=str(exc))
            except Exception as history_exc:
                # Must not mask the original error.
                logger.error("irrelevant_purge_history_write_failed", error=str(history_exc))
        raise
    finally:
        if await redis.get(_LOCK_KEY) == token:
            await redis.delete(_LOCK_KEY)
