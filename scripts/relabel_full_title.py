"""Hiện lại bài đang bị ẩn mà có đủ tên phim dài (quy tắc full_title của relevance_rules.resolve_relevance, 2026-10-16).

Bài đã lưu với nhãn "uncertain" (hoặc chưa có nhãn mà keyword_match = 0) nhưng nội dung chứa đủ tên phim từ 3 chữ trở
lên - ví dụ "t vừa coi suất chiều Bóng Ma Nhà Hát" - được đánh lại là liên quan. Bỏ qua phim "chặt" và phim tên chung
chung (settings.strict_relevance_movie_slugs / ambiguous_title_movie_slugs). Chạy lại an toàn.

    python -m scripts.relabel_full_title --dry-run --since 2026-10-01
    python -m scripts.relabel_full_title --since 2026-09-01
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter

from app.core.config import settings
from app.core.logging import get_logger
from app.repositories.d1.posts import RELEVANT_POST_SQL
from app.services.d1 import d1_query
from app.services.relevance_rules import mentions_full_title

logger = get_logger(__name__)

PAGE = 2000
CHUNK = 90  # id mỗi câu UPDATE (D1 tối đa 100 tham số)


async def run(since: str, dry_run: bool) -> None:
    movies = {
        m["id"]: m
        for m in await d1_query("SELECT id, title, slug FROM movies", quiet=True) or []
        if m.get("slug") not in settings.strict_relevance_movies
        and m.get("slug") not in settings.ambiguous_title_movies
    }
    totals: Counter[str] = Counter()
    last_id = ""
    while True:
        rows = await d1_query(
            f"SELECT p.id, p.platform, p.movie_id, p.content FROM posts p "
            f"WHERE p.scraped_at >= ? AND p.id > ? AND NOT COALESCE({RELEVANT_POST_SQL}, 0) ORDER BY p.id LIMIT ?",
            [since, last_id, PAGE],
            timeout=120.0,
        )
        if not rows:
            break
        last_id = rows[-1]["id"]
        lift = [
            r
            for r in rows
            if r["movie_id"] in movies and mentions_full_title(r["content"], movies[r["movie_id"]]["title"])
        ]
        totals["checked"] += len(rows)
        for row in lift:
            totals[row["platform"]] += 1
        if not dry_run:
            for i in range(0, len(lift), CHUNK):
                ids = [row["id"] for row in lift[i : i + CHUNK]]
                await d1_query(
                    f"UPDATE posts SET relevance_label = 'related', keyword_match = 1 WHERE id IN ({','.join('?' * len(ids))})",
                    ids,
                )
        totals["lifted"] += len(lift)
        logger.info("relabel_full_title_page", **totals)
    logger.info("relabel_full_title_finished", dry_run=dry_run, since=since, **totals)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--since", default="2026-09-01", help="bài thu từ ngày này (scraped_at, ISO)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    asyncio.run(run(args.since, args.dry_run))
