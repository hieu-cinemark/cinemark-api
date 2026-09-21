"""Regenerates the AI "top 10 topics" social listening report for every
enabled movie (or one movie via --movie-id), overwriting
social_topic_reports by movie_id. This is deliberately a periodic batch job,
not computed per-pageview - topic-clustering a movie's comments is an
expensive Kira call, and a movie's discussion topics don't meaningfully
shift within a few hours at current comment volume. Run daily via cron (see
scripts/trigger_scheduled_crawl.sh); use --movie-id for an on-demand
one-off regeneration (e.g. right after a backfill run, without waiting for
the next cron cycle).

For each movie:
1. Fetch an engagement-ranked, capped sample of its sentiment-classified
   comments (get_comment_sample_for_movie) for the topic-clustering call.
2. Separately, count EVERY classified comment for that movie by sentiment
   (get_movie_sentiment_counts) - this is what overall_sentiment's
   percentages are computed from, not the capped sample, so the numbers on
   screen reflect the true population even when it's larger than the
   sample the LLM saw.
3. Skip (log only, no write) if there are fewer than
   MIN_COMMENTS_FOR_REPORT classified comments - not enough signal for a
   meaningful topic cluster.
4. Two Kira calls: topics+verbatims (over the sample), then a narrative
   blurb fed the real percentages from step 2.
5. Assemble the exact ReportData['dashboard_data'] shape the frontend
   expects and upsert it.

Usage:
    python -m scripts.generate_social_topic_reports
    python -m scripts.generate_social_topic_reports --movie-id abc123
    python -m scripts.generate_social_topic_reports --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import json

from app.core.logging import get_logger
from app.kira.base import get_kira_ai
from app.kira.report import generate_narrative, generate_topics_and_verbatims
from app.services.d1 import (
    MIN_COMMENTS_FOR_REPORT,
    d1_query,
    get_comment_sample_for_movie,
    get_movie_sentiment_counts,
    list_movies,
    upsert_social_topic_report,
)

logger = get_logger(__name__)


def _percent(count: int, total: int) -> float:
    return round((count / total) * 100, 1) if total else 0.0


async def _get_movie(movie_id: str) -> dict | None:
    rows = await d1_query("SELECT id, title FROM movies WHERE id = ?", [movie_id])
    return rows[0] if rows else None


async def generate_for_movie(movie: dict, dry_run: bool) -> None:
    movie_id, movie_title = movie["id"], movie["title"]

    counts = await get_movie_sentiment_counts(movie_id)
    total_classified = sum(counts.values())
    if total_classified < MIN_COMMENTS_FOR_REPORT:
        logger.info("report_skip_insufficient_data", movie_id=movie_id, classified=total_classified)
        return

    sample = await get_comment_sample_for_movie(movie_id)
    post_count = len({c["post_id"] for c in sample})

    topics_result = await generate_topics_and_verbatims(movie_title, sample)
    if topics_result is None:
        logger.warning("report_skip_topics_failed", movie_id=movie_id)
        return

    positive_percent = _percent(counts.get("positive", 0), total_classified)
    negative_percent = _percent(counts.get("negative", 0), total_classified)
    neutral_percent = _percent(counts.get("neutral", 0), total_classified)
    topic_names = [t.get("topic_name", "") for t in topics_result.get("top_10_topics", [])[:5]]

    analysis = await generate_narrative(
        movie_title,
        positive_percent=positive_percent,
        negative_percent=negative_percent,
        neutral_percent=neutral_percent,
        topic_names=topic_names,
    )

    dashboard_data = {
        "report_title": f"Báo cáo mạng xã hội · {movie_title}",
        "overall_sentiment": {
            "positive_percent": positive_percent,
            "negative_percent": negative_percent,
            "neutral_percent": neutral_percent,
            "analysis": analysis or "",
        },
        "top_10_topics": topics_result.get("top_10_topics", []),
        "top_10_verbatims": topics_result.get("top_10_verbatims", []),
    }

    if dry_run:
        logger.info(
            "report_dry_run_would_upsert",
            movie_id=movie_id,
            comment_count=len(sample),
            post_count=post_count,
            topics=len(dashboard_data["top_10_topics"]),
            verbatims=len(dashboard_data["top_10_verbatims"]),
        )
        return

    ok = await upsert_social_topic_report(
        movie_id=movie_id,
        dashboard_data_json=json.dumps(dashboard_data, ensure_ascii=False),
        comment_count=len(sample),
        post_count=post_count,
        kira_model=get_kira_ai().model,
    )
    if not ok:
        logger.warning("report_upsert_failed", movie_id=movie_id)
        return
    logger.info("report_generated", movie_id=movie_id, comment_count=len(sample), post_count=post_count)


async def generate(movie_id: str | None, dry_run: bool) -> None:
    if movie_id:
        movie = await _get_movie(movie_id)
        if movie is None:
            logger.warning("report_movie_not_found", movie_id=movie_id)
            return
        movies = [movie]
    else:
        movies = await list_movies()

    logger.info("generate_social_topic_reports_started", count=len(movies), dry_run=dry_run)
    for movie in movies:
        await generate_for_movie(movie, dry_run)
    logger.info("generate_social_topic_reports_finished")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--movie-id", help="Regenerate only this movie (skips the enabled-movies list)")
    parser.add_argument("--dry-run", action="store_true", help="Log what would happen, don't write anything")
    args = parser.parse_args()
    asyncio.run(generate(args.movie_id, args.dry_run))
