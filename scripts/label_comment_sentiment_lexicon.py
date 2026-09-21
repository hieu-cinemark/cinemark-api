"""Bulk-label comments.sentiment via the lexicon in
app/kira/sentiment_lexicon.py (no Kira calls).

Usage:
    python -m scripts.label_comment_sentiment_lexicon
    python -m scripts.label_comment_sentiment_lexicon --dry-run --limit 100
    python -m scripts.label_comment_sentiment_lexicon --platform tiktok
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter

from app.core.logging import get_logger
from app.kira.sentiment_lexicon import classify_sentiment_lexicon
from app.services.d1 import MIN_CONTENT_LENGTH, d1_query

logger = get_logger(__name__)


async def run(*, platform: str | None, limit: int | None, dry_run: bool) -> None:
    sql = (
        f"SELECT c.id, c.message FROM comments c "
        f"JOIN posts p ON p.id = c.post_id "
        f"WHERE c.sentiment IS NULL AND c.message IS NOT NULL "
        f"AND length(trim(c.message)) >= {MIN_CONTENT_LENGTH}"
    )
    params: list[str] = []
    if platform:
        sql += " AND p.platform = ?"
        params.append(platform)
    sql += " ORDER BY c.reactions_count DESC"
    if limit:
        sql += f" LIMIT {int(limit)}"

    rows = await d1_query(sql, params) or []
    logger.info("lexicon_label_started", count=len(rows), platform=platform, dry_run=dry_run)

    counts: Counter[str] = Counter()
    skipped = 0
    for row in rows:
        sentiment = classify_sentiment_lexicon(row["message"])
        if sentiment is None:
            skipped += 1
            continue
        counts[sentiment] += 1
        if dry_run:
            continue
        await d1_query(
            "UPDATE comments SET sentiment = ?, sentiment_classified_at = CURRENT_TIMESTAMP WHERE id = ?",
            [sentiment, row["id"]],
        )

    logger.info(
        "lexicon_label_finished",
        labeled=sum(counts.values()),
        skipped=skipped,
        positive=counts["positive"],
        negative=counts["negative"],
        neutral=counts["neutral"],
        dry_run=dry_run,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--platform", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    asyncio.run(run(platform=args.platform, limit=args.limit, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
