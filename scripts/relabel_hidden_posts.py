"""Xét lại mọi bài đang ẩn bằng quy tắc hiện hành của ingest (relevance_rules.resolve_relevance) và nâng những bài có
tín hiệu phim lên "liên quan" - để thay đổi quy tắc (tên đạo diễn tách "và", tên phim có số phần trước dấu ":", từ khoá
mới thêm...) áp dụng cả cho bài cũ, không chỉ bài mới vào. Chạy lại an toàn.

Bài ẩn = mọi bài không qua RELEVANT_POST_SQL ("uncertain", "not_related" do Kira/Jev, chưa có nhãn + không khớp từ khoá).
Bài được xét như nhãn "uncertain": có tín hiệu phim (diễn viên/đạo diễn, hashtag tên phim đặc trưng, "phim <tên>", tên
phim gần từ điện ảnh, đủ tên phim...) thì nâng; phim "chặt" (settings.strict_relevance_movie_slugs) chỉ nhận tín hiệu
mạnh như ở ingest.

    python -m scripts.relabel_hidden_posts --dry-run --since 2026-09-01 --samples 20
    python -m scripts.relabel_hidden_posts --since 2026-09-01
"""

from __future__ import annotations

import argparse
import asyncio
import random
from collections import Counter

from app.core.config import settings
from app.core.logging import get_logger
from app.repositories.d1.posts import RELEVANT_POST_SQL
from app.services.d1 import d1_query
from app.services.relevance_rules import resolve_relevance

logger = get_logger(__name__)

PAGE = 2000
CHUNK = 90  # id mỗi câu UPDATE (D1 tối đa 100 tham số)


async def load_movies() -> dict[str, dict]:
    """Cùng dữ liệu phim mà ingest dùng (app/workers/ingest_consumer/main.py:_tracked_movies)."""
    rows = await d1_query('SELECT id, title, slug, director, "cast", released_at FROM movies', quiet=True) or []
    movies = {r["id"]: {**r, "keywords": []} for r in rows if r.get("title")}
    for row in await d1_query("SELECT movie_id, keyword FROM keywords WHERE enabled = 1", quiet=True) or []:
        if row["movie_id"] in movies and row.get("keyword"):
            movies[row["movie_id"]]["keywords"].append(row["keyword"])
    return movies


def lift_reason(content: str | None, movie: dict, posted_at: object = None) -> str | None:
    slug = movie.get("slug")
    relevant, _, context = resolve_relevance(
        "uncertain",
        content,
        movie,
        has_keyword=False,
        strict=slug in settings.strict_relevance_movies,
        full_title_rule=slug not in settings.ambiguous_title_movies,
        posted_at=posted_at,
    )
    return context if relevant else None


# Lý do đủ chính xác để nâng hàng loạt (đo 17/10/2026 trên 225 bài ẩn): "cast" (tin đời tư diễn viên, diễn viên đóng
# nhiều phim) và "keyword_hashtag" (#teeyod2/#teeyod3 cho Tee Yod 4, hashtag quảng cáo thuốc cho Huyết Thống) sai nhiều.
DEFAULT_REASONS = "title_hashtag,phim_title,strong_near_title,quoted_title,full_title,release_window"


async def run(since: str, dry_run: bool, samples: int, allowed: set[str], only: set[str]) -> None:
    movies = await load_movies()
    if only:
        movies = {mid: m for mid, m in movies.items() if m.get("slug") in only}
    totals: Counter[str] = Counter()
    reasons: Counter[str] = Counter()
    per_movie: Counter[str] = Counter()
    picked: list[tuple[str, str, str]] = []
    last_id = ""
    while True:
        rows = await d1_query(
            f"SELECT p.id, p.platform, p.movie_id, p.content, p.posted_at FROM posts p "
            f"WHERE p.scraped_at >= ? AND p.id > ? AND NOT COALESCE({RELEVANT_POST_SQL}, 0) ORDER BY p.id LIMIT ?",
            [since, last_id, PAGE],
            timeout=120.0,
        )
        if not rows:
            break
        last_id = rows[-1]["id"]
        lift: list[str] = []
        for row in rows:
            movie = movies.get(row["movie_id"])
            if movie is None:
                continue
            reason = lift_reason(row["content"], movie, row["posted_at"])
            if not reason or reason not in allowed:
                continue
            lift.append(row["id"])
            reasons[reason] += 1
            per_movie[movie["title"]] += 1
            totals[row["platform"]] += 1
            picked.append((reason, movie["title"], (row["content"] or "").replace("\n", " ")[:130]))
        totals["checked"] += len(rows)
        totals["lifted"] += len(lift)
        if not dry_run:
            for i in range(0, len(lift), CHUNK):
                ids = lift[i : i + CHUNK]
                await d1_query(
                    f"UPDATE posts SET relevance_label = 'related', keyword_match = 1 WHERE id IN ({','.join('?' * len(ids))})",
                    ids,
                )
        logger.info("relabel_hidden_page", **totals)
    logger.info("relabel_hidden_finished", dry_run=dry_run, since=since, **totals)
    print("lý do:", dict(reasons.most_common()))
    print("theo phim:", dict(per_movie.most_common(12)))
    random.seed(7)
    for reason, title, text in random.sample(picked, min(samples, len(picked))):
        print(f"  [{reason}] {title[:24]} | {text}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--since", default="2026-09-01", help="bài thu từ ngày này (scraped_at, ISO)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--samples", type=int, default=0, help="in ngẫu nhiên N bài sẽ được nâng")
    parser.add_argument("--reasons", default=DEFAULT_REASONS, help="các lý do được nâng, cách nhau bằng dấu phẩy")
    parser.add_argument("--movies", default="", help="chỉ xét các phim này (slug, cách nhau bằng dấu phẩy)")
    args = parser.parse_args()
    asyncio.run(
        run(
            args.since,
            args.dry_run,
            args.samples,
            {r.strip() for r in args.reasons.split(",") if r.strip()},
            {m.strip() for m in args.movies.split(",") if m.strip()},
        )
    )
