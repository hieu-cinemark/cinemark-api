"""Đánh dấu bài "hưởng ứng trend" (cột posts.trend_post) của phim đang chiếu, xem app/services/relevance/trend_posts.py.

python -m scripts.jobs.mark_trend_posts --dry-run --movie-id <id>
python -m scripts.jobs.mark_trend_posts
"""

from __future__ import annotations

import argparse
import asyncio
import random
from datetime import UTC, datetime

from sqlalchemy import select

from app.repositories.d1.sql import d1_exec
from app.repositories.d1.tables import movies as movies_table
from app.services.relevance.trend_posts import mark_movie, showing_movies


async def main(args: argparse.Namespace) -> None:
    movies = await showing_movies(datetime.now(tz=UTC))
    if args.movie_id:
        rows = await d1_exec(
            select(
                movies_table.c.id,
                movies_table.c.title,
                movies_table.c.cast.label("cast_json"),
            ).where(movies_table.c.id == args.movie_id),
            quiet=True,
        )
        movies = rows or []
    for movie in movies:
        result = await mark_movie(movie, dry_run=args.dry_run)
        print(
            f"{movie['title']}: {result['posts']} bài, {result['trend']} bài trend, đổi {result['changed']}", flush=True
        )
        if args.dry_run:
            rng = random.Random(7)
            for label, items in (("TREND", result["trend_samples"]), ("KHÔNG", result["other_samples"])):
                for post in rng.sample(items, min(args.samples, len(items))):
                    print(f"  [{label}] {post['author']}: {' '.join((post['content'] or '').split())[:110]}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--movie-id")
    parser.add_argument("--dry-run", action="store_true", help="in mẫu, không ghi")
    parser.add_argument("--samples", type=int, default=20)
    asyncio.run(main(parser.parse_args()))
