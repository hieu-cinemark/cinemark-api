"""Regenerates the AI "top 10 topics" social listening report for every
enabled movie (or one movie via --movie-id), overwriting
social_topic_reports by movie_id. This is deliberately a periodic batch job,
not computed per-pageview - topic-clustering a movie's comments is an
expensive Bee (Claude Sonnet 5) call, and a movie's discussion topics don't
meaningfully shift within a few hours at current comment volume. Run daily
via cron (see scripts/trigger_scheduled_crawl.sh); use --movie-id for an
on-demand one-off regeneration (e.g. right after a backfill run, without
waiting for the next cron cycle) - the dashboard's own manual "Tạo report"
button (POST /movies/{id}/generate-report) does the same one-movie call.

The actual per-movie algorithm lives in app/services/social_topic.py,
shared with that button - this script is just the enabled-movies sweep +
CLI around it.

Usage:
    python -m scripts.generate_social_topic_reports
    python -m scripts.generate_social_topic_reports --movie-id abc123
    python -m scripts.generate_social_topic_reports --dry-run
"""

from __future__ import annotations

import argparse
import asyncio

from app.core.logging import get_logger
from app.services.d1 import list_movies
from app.services.social_topic import generate_report_for_movie, get_movie_for_report

logger = get_logger(__name__)


async def generate(movie_id: str | None, dry_run: bool) -> None:
    if movie_id:
        movie = await get_movie_for_report(movie_id)
        if movie is None:
            logger.warning("report_movie_not_found", movie_id=movie_id)
            return
        movies = [movie]
    else:
        movies = await list_movies()

    logger.info("generate_social_topic_reports_started", count=len(movies), dry_run=dry_run)
    for movie in movies:
        await generate_report_for_movie(movie, dry_run=dry_run)
    logger.info("generate_social_topic_reports_finished")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--movie-id", help="Regenerate only this movie (skips the enabled-movies list)")
    parser.add_argument("--dry-run", action="store_true", help="Log what would happen, don't write anything")
    args = parser.parse_args()
    asyncio.run(generate(args.movie_id, args.dry_run))
