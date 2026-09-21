"""Classifies sentiment for comments that predate app/kira/sentiment.py (or
that ingest-time classification skipped/failed for). Only ever touches rows
where sentiment IS NULL, so it's safe to re-run - a partial run just leaves
the rest for the next one.

Usage:
    python -m scripts.backfill_comment_sentiment
    python -m scripts.backfill_comment_sentiment --platform facebook
    python -m scripts.backfill_comment_sentiment --limit 50 --dry-run
    python -m scripts.backfill_comment_sentiment --pause-seconds 1 --batch-size 2
"""

from __future__ import annotations

import argparse
import asyncio

from app.core.config import settings
from app.core.logging import get_logger
from app.kira.sentiment import classify_sentiment
from app.services.d1 import MIN_CONTENT_LENGTH, d1_query

logger = get_logger(__name__)

# PhoBERT (local HTTP) can take a larger batch than Kira's concurrency=2.
# Keep a modest default so a cold MPS/CPU server isn't flooded.
DEFAULT_BATCH_SIZE = 8 if (settings.sentiment_backend or "").lower() != "kira" else 2
DEFAULT_PAUSE_SECONDS = 0.05 if (settings.sentiment_backend or "").lower() != "kira" else 1.0
CONSECUTIVE_FAILURE_BACKOFF_SECONDS = 10.0
MAX_BACKOFF_SECONDS = 60.0


def _chunk(rows: list, size: int) -> list[list]:
    return [rows[i : i + size] for i in range(0, len(rows), size)]


async def backfill(
    platform: str | None, limit: int | None, dry_run: bool, pause_seconds: float, batch_size: int
) -> None:
    sql = f"SELECT id, message FROM comments WHERE sentiment IS NULL AND length(trim(message)) >= {MIN_CONTENT_LENGTH}"
    params: list[str | int] = []

    if platform:
        sql += " AND platform = ?"
        params.append(platform)

    sql += " ORDER BY scraped_at DESC"
    if limit:
        sql += " LIMIT ?"
        params.append(limit)

    rows = await d1_query(sql, params) or []
    batches = _chunk(rows, batch_size)
    logger.info(
        "backfill_sentiment_started",
        platform=platform,
        limit=limit,
        count=len(rows),
        batch_size=batch_size,
        pause_seconds=pause_seconds,
    )

    classified = skipped = 0
    consecutive_failed_batches = 0
    for batch_i, batch in enumerate(batches):
        results = await asyncio.gather(*(classify_sentiment(row["message"]) for row in batch))

        batch_had_success = False
        for row, sentiment in zip(batch, results):
            if sentiment is None:
                logger.warning("backfill_sentiment_skip", id=row["id"])
                skipped += 1
                continue
            batch_had_success = True
            if dry_run:
                logger.info("backfill_sentiment_dry_run_would_set", id=row["id"], sentiment=sentiment)
            else:
                await d1_query(
                    "UPDATE comments SET sentiment = ?, sentiment_classified_at = CURRENT_TIMESTAMP WHERE id = ?",
                    [sentiment, row["id"]],
                )
            classified += 1

        consecutive_failed_batches = 0 if batch_had_success else consecutive_failed_batches + 1

        if batch_i == len(batches) - 1:
            break  # no point pausing after the very last batch

        if consecutive_failed_batches >= 2:
            backoff = min(
                CONSECUTIVE_FAILURE_BACKOFF_SECONDS * (consecutive_failed_batches - 1), MAX_BACKOFF_SECONDS
            )
            logger.warning(
                "backfill_sentiment_backing_off", consecutive_failed_batches=consecutive_failed_batches, seconds=backoff
            )
            await asyncio.sleep(backoff)
        else:
            await asyncio.sleep(pause_seconds)

    logger.info("backfill_sentiment_finished", classified=classified, skipped=skipped, dry_run=dry_run)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--platform", help="Only backfill this platform")
    parser.add_argument("--limit", type=int, help="Max rows to process this run")
    parser.add_argument("--dry-run", action="store_true", help="Log what would happen, don't write anything")
    parser.add_argument(
        "--pause-seconds",
        type=float,
        default=DEFAULT_PAUSE_SECONDS,
        help=f"Delay between each batch's Kira calls (default {DEFAULT_PAUSE_SECONDS}s)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Comments classified concurrently per batch (default {DEFAULT_BATCH_SIZE}, matches KIRA_CONCURRENCY)",
    )
    args = parser.parse_args()
    asyncio.run(backfill(args.platform, args.limit, args.dry_run, args.pause_seconds, args.batch_size))
