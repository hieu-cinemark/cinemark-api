"""Tự chạy chuỗi gom nhóm bình luận cho các phim đang chiếu: gom cụm -> đặt tên -> gộp/tổng kết -> đưa lên D1 (Social Topic).

Trước 17/11/2026 bốn bước này (scripts/tools/cluster_film_comments, name_clusters, summarize_clusters, publish_clusters) phải
chạy tay từng phim. Job này gọi lần lượt đúng các bước đó, nên logic vẫn nằm ở các file kia.

Chạy trên máy có embedding (data/comment_embeddings.sqlite do scripts/jobs/embed_comments.py ghi - trên VPS là timer
`cinemark-embed`). Mỗi phim chỉ làm lại khi chưa có nhóm hoặc nhóm đã cũ hơn --min-age-hours; mỗi lượt tối đa --max-films phim
(phim cũ nhất làm trước) để giới hạn lời gọi AI (1 lời gọi đặt tên + tối đa MAX_SUMMARIES lời gọi tổng kết mỗi phim).
Lỗi của một phim không chặn các phim sau.

    python -m scripts.jobs.topic_clusters                    # mọi phim đang chiếu, theo luật trên
    python -m scripts.jobs.topic_clusters --movie-id <id> --force
    python -m scripts.jobs.topic_clusters --dry-run          # chỉ in phim nào sẽ được làm
"""

from __future__ import annotations

import argparse
import asyncio
from argparse import Namespace
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select

from app.core.logging import get_logger
from app.repositories.d1.sql import d1_exec
from app.repositories.d1.tables import comment_cluster_topics as topics
from app.services.relevance.trend_posts import showing_movies

logger = get_logger(__name__)

DEFAULT_K = 30


async def last_generated() -> dict[str, str]:
    """movie_id -> thời điểm (ISO) nhóm bình luận được tạo gần nhất; phim chưa có nhóm thì không có trong kết quả."""
    rows = await d1_exec(
        select(topics.c.movie_id, func.max(topics.c.generated_at).label("at")).group_by(topics.c.movie_id)
    )
    return {row["movie_id"]: str(row["at"]) for row in rows or [] if row.get("at")}


def pick_movies(
    movies: list[dict[str, Any]], generated: dict[str, str], *, now: datetime, min_age_hours: float, max_films: int
) -> list[dict[str, Any]]:
    """Phim cần làm lại: chưa có nhóm, hoặc nhóm cũ hơn `min_age_hours`. Phim chưa có nhóm / cũ nhất đứng trước; cắt còn `max_films`."""
    cutoff = (now - timedelta(hours=min_age_hours)).isoformat()
    due = [m for m in movies if generated.get(m["id"], "") < cutoff]
    due.sort(key=lambda m: generated.get(m["id"], ""))
    return due[:max_films]


async def run_movie(movie: dict[str, Any], k: int) -> None:
    """Bốn bước cho một phim; mỗi bước đọc file trung gian của bước trước trong data/clusters/."""
    from scripts.tools import cluster_film_comments, name_clusters, publish_clusters, summarize_clusters

    movie_id, title = movie["id"], movie["title"]
    await cluster_film_comments.main(Namespace(movie_id=movie_id, k=k))
    await name_clusters.main(Namespace(movie_id=movie_id, title=title))
    await summarize_clusters.main(Namespace(movie_id=movie_id, title=title))
    await publish_clusters.run(movie_id)


async def main(args: argparse.Namespace) -> int:
    now = datetime.now(tz=UTC)
    movies = await showing_movies(now)
    if args.movie_id:
        movies = [m for m in movies if m["id"] == args.movie_id] or [
            {"id": args.movie_id, "title": args.title or args.movie_id}
        ]
    generated = await last_generated()
    min_age = 0 if args.force else args.min_age_hours
    chosen = pick_movies(movies, generated, now=now, min_age_hours=min_age, max_films=args.max_films)
    logger.info("topic_clusters_plan", showing=len(movies), chosen=[m["title"] for m in chosen], dry_run=args.dry_run)
    failed = 0
    for movie in chosen:
        if args.dry_run:
            continue
        try:
            await run_movie(movie, args.k)
        except Exception as exc:  # noqa: BLE001 - một phim lỗi (thiếu embedding, AI hết hạn mức...) không được chặn phim sau
            failed += 1
            logger.warning("topic_clusters_failed", movie_id=movie["id"], error=exc)
    return 1 if failed and failed == len(chosen) else 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--movie-id")
    parser.add_argument("--title", help="chỉ cần khi --movie-id không nằm trong phim đang chiếu")
    parser.add_argument("--k", type=int, default=DEFAULT_K)
    parser.add_argument("--min-age-hours", type=float, default=24)
    parser.add_argument("--max-films", type=int, default=3)
    parser.add_argument("--force", action="store_true", help="làm lại dù nhóm còn mới")
    parser.add_argument("--dry-run", action="store_true")
    raise SystemExit(asyncio.run(main(parser.parse_args())))
