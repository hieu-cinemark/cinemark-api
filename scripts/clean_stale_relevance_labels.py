"""Re-checks a movie's relevance_label='related' posts against the CURRENT
movie_hashtag_present (hashtag/exact-token + author-reputation gate), and
downgrades any that no longer pass to 'not_related'.

Exists because relevance_label itself is a point-in-time snapshot: it was
set by whatever verdict (AI or the old keyword substring check) was live
at ingest time, and never gets re-evaluated as movie_hashtag_present's own
logic improves. list_posts(sort="engagement")/get_comment_sample_for_movie
already re-run movie_hashtag_present at query time so their OWN output is
clean, but relevance_label on disk stays wrong until something rewrites
it - and get_movie_sentiment_counts (app/services/d1.py) counts straight
off relevance_label with no such re-check, so a movie whose title is
ordinary vocabulary (e.g. "Huyết Thống") keeps polluting its own sentiment
percentages even after the post-list/report views were fixed.

Scope: one movie per run, by id. Safe to re-run - every row is a plain
relevance_label='related' -> 'not_related' downgrade (never the reverse;
this script only removes false positives, it can't add missed true
positives back), so re-running just re-confirms/no-ops on rows already
downgraded.

Usage:
  .venv/bin/python -m scripts.clean_stale_relevance_labels <movie_id>
"""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timezone

from app.core.logging import get_logger
from app.repositories.d1.posts import movie_hashtag_present, reputable_authors
from app.services.d1 import d1_query

logger = get_logger(__name__)

PAGE_SIZE = 1000
# id (label CASE) + id (confidence CASE, literal 0.0) + id+now (labeled_at
# CASE) + id (IN clause) = 5 params/row - well under the D1 bound-parameter
# ceiling scripts/push_relevance_labels.py's own ROWS_PER_BATCH comment
# documents; 10 rows/batch (50 params) matches that script's batch size.
ROWS_PER_BATCH = 10


async def _downgrade_batch(ids: list[str], now: str) -> None:
    label_cases = " ".join("WHEN ? THEN 'not_related'" for _ in ids)
    conf_cases = " ".join("WHEN ? THEN 0.0" for _ in ids)
    time_cases = " ".join("WHEN ? THEN ?" for _ in ids)
    placeholders = ", ".join("?" for _ in ids)

    sql = f"""
        UPDATE posts SET
            relevance_label = CASE id {label_cases} END,
            relevance_confidence = CASE id {conf_cases} END,
            relevance_labeled_at = CASE id {time_cases} END
        WHERE id IN ({placeholders})
    """
    params: list[object] = list(ids)
    params.extend(ids)
    for post_id in ids:
        params.extend([post_id, now])
    params.extend(ids)

    await d1_query(sql, params)


async def main(movie_id: str) -> None:
    movie_rows = await d1_query("SELECT title FROM movies WHERE id = ?", [movie_id], timeout=30.0)
    if not movie_rows:
        raise RuntimeError(f"No movie with id={movie_id}")
    movie_title = movie_rows[0]["title"]
    reputable = await reputable_authors()
    print(f"movie: {movie_title!r}, reputable authors cached: {len(reputable)}")

    last_id = ""
    total_scanned = 0
    to_downgrade: list[str] = []
    kept = 0
    while True:
        rows = await d1_query(
            """
            SELECT p.id, p.platform, p.author, p.content, k.keyword
            FROM posts p
            LEFT JOIN keywords k ON k.id = p.keyword_id
            WHERE p.movie_id = ? AND p.relevance_label = 'related' AND p.id > ?
            ORDER BY p.id
            LIMIT ?
            """,
            [movie_id, last_id, PAGE_SIZE],
            timeout=30.0,
        )
        if not rows:
            break
        for row in rows:
            is_reputable = (row["platform"], row["author"]) in reputable
            if movie_hashtag_present(row.get("content"), movie_title, row.get("keyword"), is_reputable_author=is_reputable):
                kept += 1
            else:
                to_downgrade.append(row["id"])
        total_scanned += len(rows)
        last_id = rows[-1]["id"]
        print(f"scanned {total_scanned}, kept {kept}, to_downgrade {len(to_downgrade)}", flush=True)
        if len(rows) < PAGE_SIZE:
            break

    print(f"\ntotal scanned: {total_scanned}, kept: {kept}, downgrading: {len(to_downgrade)}")

    now = datetime.now(tz=timezone.utc).isoformat()
    for i in range(0, len(to_downgrade), ROWS_PER_BATCH):
        batch = to_downgrade[i : i + ROWS_PER_BATCH]
        await _downgrade_batch(batch, now)
        if (i // ROWS_PER_BATCH) % 20 == 0:
            print(f"downgraded {i + len(batch)}/{len(to_downgrade)}", flush=True)

    print(f"done - downgraded {len(to_downgrade)} posts to not_related")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: python -m scripts.clean_stale_relevance_labels <movie_id>")
    asyncio.run(main(sys.argv[1]))
