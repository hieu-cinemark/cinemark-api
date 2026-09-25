"""Builds/refreshes the author_reputation table: for every post with
relevance_label='related', re-checks it against movie_hashtag_present's
STRONG signals only (is_reputable_author defaults to False - see that
function's own docstring) and counts, per (platform, author), how many
DISTINCT movies they're confirmed for that way.

An author confirmed across >= MIN_MOVIES_FOR_REPUTABLE_AUTHOR different
movies gets to corroborate movie_hashtag_present's weakest signal (a bare
literal title match, no hashtag backing it) for OTHER movies whose title
happens to also be ordinary vocabulary - see that function's own module
docstring, the "Huyết Thống" incident this whole table exists to close.
No circularity: reputation is only ever earned via the hashtag-based
signals, never the literal-title-alone one it's meant to corroborate.

Full rebuild every run (not incremental) - cheap enough (one pass over
relevance_label='related' posts, paginated) to just re-derive from
scratch each time rather than tracking deltas. Re-run periodically as
more posts accumulate; this is a batch job, not something request paths
call.

Usage:
  .venv/bin/python -m scripts.build_author_reputation
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import datetime, timezone

from app.repositories.d1.posts import (
    MIN_MOVIES_FOR_REPUTABLE_AUTHOR,
    ensure_author_reputation_table,
    movie_hashtag_present,
)
from app.services.d1 import d1_query

PAGE_SIZE = 2000
# 7 params/row (platform, author, distinct_movies, confirmed_posts, updated_at
# - see push_relevance_labels.py's own ROWS_PER_BATCH comment for the same
# D1 bound-parameter ceiling reasoning) - 15 rows/batch stays comfortably
# under it.
ROWS_PER_INSERT_BATCH = 15


async def main() -> None:
    confirmed_movies: dict[tuple[str, str], set[str]] = defaultdict(set)
    confirmed_posts: dict[tuple[str, str], int] = defaultdict(int)

    last_id = ""
    total_scanned = 0
    while True:
        rows = await d1_query(
            """
            SELECT p.id, p.platform, p.author, p.movie_id, p.content, k.keyword, m.title AS movie_title
            FROM posts p
            LEFT JOIN keywords k ON k.id = p.keyword_id
            LEFT JOIN movies m ON m.id = p.movie_id
            WHERE p.relevance_label = 'related' AND p.author IS NOT NULL AND p.author != '' AND p.id > ?
            ORDER BY p.id
            LIMIT ?
            """,
            [last_id, PAGE_SIZE],
            timeout=30.0,
        )
        if not rows:
            break
        for row in rows:
            if movie_hashtag_present(row.get("content"), row.get("movie_title"), row.get("keyword")):
                key = (row["platform"], row["author"])
                confirmed_movies[key].add(row["movie_id"])
                confirmed_posts[key] += 1
        total_scanned += len(rows)
        last_id = rows[-1]["id"]
        reputable_so_far = sum(1 for v in confirmed_movies.values() if len(v) >= MIN_MOVIES_FOR_REPUTABLE_AUTHOR)
        print(f"scanned {total_scanned}, reputable-so-far {reputable_so_far}", flush=True)
        if len(rows) < PAGE_SIZE:
            break

    print(f"\ntotal scanned: {total_scanned}")
    print(f"authors with >=1 confirmed movie: {len(confirmed_movies)}")
    reputable_count = sum(1 for v in confirmed_movies.values() if len(v) >= MIN_MOVIES_FOR_REPUTABLE_AUTHOR)
    print(f"authors reaching MIN_MOVIES_FOR_REPUTABLE_AUTHOR={MIN_MOVIES_FOR_REPUTABLE_AUTHOR}: {reputable_count}")

    await ensure_author_reputation_table()
    await d1_query("DELETE FROM author_reputation")

    now = datetime.now(tz=timezone.utc).isoformat()
    all_rows = [
        (platform, author, len(movies), confirmed_posts[(platform, author)], now)
        for (platform, author), movies in confirmed_movies.items()
    ]
    for i in range(0, len(all_rows), ROWS_PER_INSERT_BATCH):
        batch = all_rows[i : i + ROWS_PER_INSERT_BATCH]
        placeholders = ", ".join("(?, ?, ?, ?, ?)" for _ in batch)
        params = [value for row in batch for value in row]
        await d1_query(
            f"INSERT INTO author_reputation (platform, author, distinct_movies, confirmed_posts, updated_at) "
            f"VALUES {placeholders}",
            params,
        )

    print(f"wrote {len(all_rows)} author_reputation rows")


if __name__ == "__main__":
    asyncio.run(main())
