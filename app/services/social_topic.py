"""Generates the AI "top 10 topics" social-listening report for one movie
- the per-movie logic shared by scripts/generate_social_topic_reports.py's
batch/cron sweep and the dashboard's manual "Tạo report" button
(POST /movies/{id}/generate-report in app/api/routes/movies.py).

For one movie:
1. Fetch an engagement-ranked, capped sample of its sentiment-classified
   comments (get_comment_sample_for_movie) for the topic-clustering call.
2. Separately, count EVERY classified comment for that movie by sentiment
   (get_movie_sentiment_counts) - this is what overall_sentiment's
   percentages are computed from, not the capped sample, so the numbers on
   screen reflect the true population even when it's larger than the
   sample the LLM saw.
3. Skip if there are fewer than MIN_COMMENTS_FOR_REPORT classified
   comments - not enough signal for a meaningful topic cluster.
4. Two Bee calls: topics+verbatims (over the sample), then a narrative
   blurb fed the real percentages from step 2.
5. Assemble the exact ReportData['dashboard_data'] shape the frontend
   expects and upsert it.
"""

from __future__ import annotations

import json
from typing import Literal

from app.bee.client import DEFAULT_BEE_MODEL
from app.bee.report import generate_narrative, generate_topics_and_verbatims
from app.core.logging import get_logger
from app.services.d1 import (
    MIN_COMMENTS_FOR_REPORT,
    d1_query,
    get_comment_sample_for_movie,
    get_movie_sentiment_counts,
    upsert_social_topic_report,
)

logger = get_logger(__name__)

ReportResult = Literal["generated", "insufficient_data", "topics_failed", "upsert_failed"]


def _percent(count: int, total: int) -> float:
    return round((count / total) * 100, 1) if total else 0.0


def _norm_comment_text(value: str | None) -> str:
    return " ".join((value or "").split()).casefold()


def _source_fields(comment: dict) -> dict:
    return {
        "author_name": comment.get("author_name"),
        "author_url": comment.get("author_url"),
        "author_profile_picture": comment.get("author_profile_picture"),
        "post_url": comment.get("post_url"),
        "post_content": comment.get("post_content"),
        "post_author": comment.get("post_author"),
        "platform": comment.get("platform"),
    }


def _lookup_sample_comment(item: dict, by_id: dict[str, dict], by_text: dict[str, dict]) -> dict | None:
    comment_id = item.get("id")
    if isinstance(comment_id, str) and comment_id in by_id:
        return by_id[comment_id]
    return by_text.get(_norm_comment_text(item.get("text") or item.get("message")))


def hydrate_report_comments(topics_result: dict, sample: list[dict]) -> dict:
    """Attach author + parent-post fields the dashboard renders next to
    evidence/verbatims. The LLM only sees id/message/likes; we stitch the
    rest back from the same sample after the call so existing reports can
    also be hydrated at read time by the same text match."""
    by_id = {c["id"]: c for c in sample if c.get("id")}
    by_text: dict[str, dict] = {}
    for comment in sample:
        key = _norm_comment_text(comment.get("message"))
        if not key:
            continue
        previous = by_text.get(key)
        if previous is None or (comment.get("reactions_count") or 0) > (previous.get("reactions_count") or 0):
            by_text[key] = comment

    topics = []
    for topic in topics_result.get("top_10_topics") or []:
        evidence = []
        for item in topic.get("evidence_comments") or []:
            if not isinstance(item, dict):
                continue
            match = _lookup_sample_comment(item, by_id, by_text)
            evidence.append({**item, **(_source_fields(match) if match else {})})
        topics.append({**topic, "evidence_comments": evidence})

    verbatims = []
    for item in topics_result.get("top_10_verbatims") or []:
        if not isinstance(item, dict):
            continue
        match = _lookup_sample_comment(item, by_id, by_text)
        verbatims.append({**item, **(_source_fields(match) if match else {})})

    return {**topics_result, "top_10_topics": topics, "top_10_verbatims": verbatims}


async def get_movie_for_report(movie_id: str) -> dict | None:
    rows = await d1_query("SELECT id, title FROM movies WHERE id = ?", [movie_id])
    return rows[0] if rows else None


async def generate_report_for_movie(movie: dict, *, dry_run: bool = False) -> ReportResult:
    movie_id, movie_title = movie["id"], movie["title"]

    counts = await get_movie_sentiment_counts(movie_id)
    total_classified = sum(counts.values())
    if total_classified < MIN_COMMENTS_FOR_REPORT:
        logger.info("report_skip_insufficient_data", movie_id=movie_id, classified=total_classified)
        return "insufficient_data"

    sample = await get_comment_sample_for_movie(movie_id)
    post_count = len({c["post_id"] for c in sample})

    topics_result = await generate_topics_and_verbatims(movie_title, sample)
    if topics_result is None:
        logger.warning("report_skip_topics_failed", movie_id=movie_id)
        return "topics_failed"

    topics_result = hydrate_report_comments(topics_result, sample)

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
        return "generated"

    ok = await upsert_social_topic_report(
        movie_id=movie_id,
        dashboard_data_json=json.dumps(dashboard_data, ensure_ascii=False),
        comment_count=len(sample),
        post_count=post_count,
        # Column is still named kira_model (schema predates the Kira->Bee
        # switch) - holds whichever provider actually generated the report.
        kira_model=DEFAULT_BEE_MODEL,
    )
    if not ok:
        logger.warning("report_upsert_failed", movie_id=movie_id)
        return "upsert_failed"
    logger.info("report_generated", movie_id=movie_id, comment_count=len(sample), post_count=post_count)
    return "generated"
