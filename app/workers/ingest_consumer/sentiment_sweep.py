"""Background comment-sentiment sweep, run inside the ingest consumer.

handle_comment persists comments with sentiment NULL; this loop picks up
the recent unclassified ones every SWEEP_INTERVAL_SECONDS and classifies
them through Bee in batches (app/kira/sentiment.py), so a comment gets its
label within a minute or two instead of stalling Kafka ingest on a ~14s
Bee call per comment.

Bee calls run one batch at a time, leaving the second slot of Bee's
2-call concurrency budget (app/ai_client.py) free for report writing.
A Redis lock keeps two running consumers from classifying the same rows.

scripts/backfill_comment_sentiment.py reuses classify_pending() for the
older backlog outside the sweep's recency window.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any

from app.core.logging import get_logger
from app.kira.sentiment import BATCH_SIZE, classify_sentiments
from app.services.d1 import MIN_CONTENT_LENGTH, d1_query
from app.services.redis import REDIS_KEY_PREFIX, get_redis_client

logger = get_logger(__name__)

SWEEP_INTERVAL_SECONDS = 60
SWEEP_MAX_ROWS = 200
# Only recent comments: older NULL rows are the backfill script's job, and
# the window bounds how long a comment Bee keeps failing on gets retried.
SWEEP_WINDOW = timedelta(hours=48)
# A comment Bee returned no valid label for this many times is left for
# the backfill script instead of occupying every sweep.
MAX_ATTEMPTS = 3
_LOCK_KEY = f"{REDIS_KEY_PREFIX}comment_sentiment_sweep_lock"
_LOCK_TTL_SECONDS = 600


async def _save(labels: dict[str, str]) -> int:
    """One UPDATE per label value (ids in IN (...)) - at most BATCH_SIZE+1
    bound params, well under D1's 100 per statement."""
    by_label: dict[str, list[str]] = {}
    for comment_id, label in labels.items():
        by_label.setdefault(label, []).append(comment_id)
    saved = 0
    for label, ids in by_label.items():
        placeholders = ",".join("?" * len(ids))
        result = await d1_query(
            f"UPDATE comments SET sentiment = ?, sentiment_classified_at = CURRENT_TIMESTAMP "
            f"WHERE id IN ({placeholders}) AND sentiment IS NULL",
            [label, *ids],
        )
        if result is None:
            logger.warning("comment_sentiment_save_failed", sentiment=label, count=len(ids))
        else:
            saved += len(ids)
    return saved


async def classify_pending(
    *,
    limit: int,
    since: datetime | None = None,
    platform: str | None = None,
    exclude: set[str] | frozenset[str] = frozenset(),
    dry_run: bool = False,
) -> dict[str, Any]:
    """Classifies up to `limit` NULL-sentiment comments, newest first, one
    Bee batch at a time, writing each batch as soon as it's labeled.
    Returns {"selected", "classified", "failed", "failed_ids", <label>:
    count...} - failed_ids so callers can cap retries per comment."""
    sql = f"SELECT id, message FROM comments WHERE sentiment IS NULL AND length(trim(message)) >= {MIN_CONTENT_LENGTH}"
    params: list[str | int] = []
    if since is not None:
        sql += " AND scraped_at >= ?"
        params.append(since.astimezone(UTC).isoformat())  # scraped_at is ISO-8601 UTC
    if platform:
        sql += " AND platform = ?"
        params.append(platform)
    sql += " ORDER BY scraped_at DESC LIMIT ?"
    params.append(limit + len(exclude))
    rows = await d1_query(sql, params)
    if rows is None:
        raise RuntimeError("comment_sentiment_select_failed")
    rows = [row for row in rows if row["id"] not in exclude][:limit]

    # classified set up front: with no rows the loop never touches it, and
    # the returned plain dict (unlike Counter) raises on a missing key.
    stats: Counter[str] = Counter(selected=len(rows), classified=0)
    failed_ids: list[str] = []
    for start in range(0, len(rows), BATCH_SIZE):
        batch = rows[start : start + BATCH_SIZE]
        labels = await classify_sentiments([row["message"] for row in batch])
        done = {row["id"]: label for row, label in zip(batch, labels, strict=True) if label is not None}
        failed_ids.extend(row["id"] for row, label in zip(batch, labels, strict=True) if label is None)
        stats.update(done.values())
        stats["classified"] += len(done) if dry_run else await _save(done)
    stats["failed"] = len(failed_ids)
    return {**stats, "failed_ids": failed_ids}


async def _acquire_lock() -> bool:
    try:
        return bool(await get_redis_client().set(_LOCK_KEY, "1", nx=True, ex=_LOCK_TTL_SECONDS))
    except Exception as exc:  # noqa: BLE001 - no Redis, run unlocked (single consumer is the norm)
        logger.warning("comment_sentiment_lock_unavailable", error=exc)
        return True


async def _release_lock() -> None:
    try:
        await get_redis_client().delete(_LOCK_KEY)
    except Exception as exc:  # noqa: BLE001 - the TTL expires it anyway
        logger.debug("comment_sentiment_lock_release_failed", error=exc)


async def sweep_forever() -> None:
    """Never returns; every failure is logged and retried next round."""
    attempts: Counter[str] = Counter()
    while True:
        full_batch = False
        try:
            if await _acquire_lock():
                try:
                    exclude = {cid for cid, n in attempts.items() if n >= MAX_ATTEMPTS}
                    result = await classify_pending(
                        limit=SWEEP_MAX_ROWS, since=datetime.now(tz=UTC) - SWEEP_WINDOW, exclude=exclude
                    )
                finally:
                    await _release_lock()
                failed_ids = result.pop("failed_ids")
                # Count a failure against the comment only when Bee labeled
                # others in the same round - a round where every batch failed
                # is Bee down/limited, and must not exclude everything.
                if result["classified"]:
                    attempts.update(failed_ids)
                if len(attempts) > 10_000:  # bound memory; worst case a few extra retries
                    attempts.clear()
                if result["selected"]:
                    logger.info("comment_sentiment_sweep_finished", **result)
                full_batch = result["selected"] >= SWEEP_MAX_ROWS and result["classified"] > 0
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - keep sweeping; ingest must not die over sentiment
            logger.warning("comment_sentiment_sweep_failed", error=exc)
        # A full page means a backlog - go again right away.
        await asyncio.sleep(1 if full_batch else SWEEP_INTERVAL_SECONDS)
