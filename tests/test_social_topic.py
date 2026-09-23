"""Exercises app/services/social_topic.py's generate_report_for_movie
against fakes for every D1/Bee call (no real D1 or Bee involved). What's
worth verifying: movies below MIN_COMMENTS_FOR_REPORT are skipped without
spending a Bee call, the upsert is keyed by movie_id with the real
SQL-computed percentages (not whatever the LLM might have guessed), and
--dry-run never writes. Shared by scripts/generate_social_topic_reports.py's
sweep and the dashboard's manual "Tạo report" button - see
test_generate_social_topic_reports.py for the sweep's own CLI-level test."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

from app.services.d1 import MIN_COMMENTS_FOR_REPORT
from app.services.social_topic import generate_report_for_movie

MOVIE = {"id": "movie_1", "title": "Phim Test"}

SAMPLE_COMMENTS = [
    {
        "id": "c1",
        "post_id": "post_1",
        "message": "Hay quá",
        "reactions_count": 50,
        "sentiment": "positive",
        "author_name": "Lan",
        "author_url": "https://example.com/lan",
        "author_profile_picture": "https://example.com/lan.jpg",
        "post_url": "https://example.com/p1",
        "post_content": "Trailer mới",
        "post_author": "Studio",
        "platform": "facebook",
    },
    {"id": "c2", "post_id": "post_2", "message": "Dở quá", "reactions_count": 10, "sentiment": "negative"},
]

TOPICS_RESULT = {
    "top_10_topics": [{"topic_name": "Diễn xuất", "sentiment": "Positive", "insight_summary": "...", "evidence_comments": []}],
    "top_10_verbatims": [{"text": "Hay quá", "likes": 50, "why_it_matters": "..."}],
}


async def test_movie_below_threshold_is_skipped_without_any_bee_call() -> None:
    counts = {"positive": MIN_COMMENTS_FOR_REPORT - 1}

    with (
        patch("app.services.social_topic.get_movie_sentiment_counts", AsyncMock(return_value=counts)),
        patch("app.services.social_topic.get_comment_sample_for_movie", AsyncMock()) as mock_sample,
        patch("app.services.social_topic.generate_topics_and_verbatims", AsyncMock()) as mock_topics,
        patch("app.services.social_topic.upsert_social_topic_report", AsyncMock()) as mock_upsert,
    ):
        result = await generate_report_for_movie(MOVIE, dry_run=False)

    assert result == "insufficient_data"
    mock_sample.assert_not_awaited()
    mock_topics.assert_not_awaited()
    mock_upsert.assert_not_awaited()


async def test_happy_path_upserts_with_sql_computed_percentages_not_llm_guessed() -> None:
    counts = {"positive": 30, "negative": 10}  # 75% / 25%, total 40

    with (
        patch("app.services.social_topic.get_movie_sentiment_counts", AsyncMock(return_value=counts)),
        patch("app.services.social_topic.get_comment_sample_for_movie", AsyncMock(return_value=SAMPLE_COMMENTS)),
        patch("app.services.social_topic.generate_topics_and_verbatims", AsyncMock(return_value=TOPICS_RESULT)),
        patch("app.services.social_topic.generate_narrative", AsyncMock(return_value="Nhìn chung tích cực.")),
        patch("app.services.social_topic.upsert_social_topic_report", AsyncMock(return_value=True)) as mock_upsert,
    ):
        result = await generate_report_for_movie(MOVIE, dry_run=False)

    assert result == "generated"
    mock_upsert.assert_awaited_once()
    kwargs = mock_upsert.await_args.kwargs
    assert kwargs["movie_id"] == "movie_1"
    assert kwargs["post_count"] == 2  # 2 distinct post_ids across SAMPLE_COMMENTS

    data = json.loads(kwargs["dashboard_data_json"])
    assert data["overall_sentiment"]["positive_percent"] == 75.0
    assert data["overall_sentiment"]["negative_percent"] == 25.0
    assert data["overall_sentiment"]["neutral_percent"] == 0.0
    assert data["overall_sentiment"]["analysis"] == "Nhìn chung tích cực."
    assert data["top_10_topics"] == TOPICS_RESULT["top_10_topics"]
    assert data["top_10_verbatims"][0]["text"] == "Hay quá"
    assert data["top_10_verbatims"][0]["author_name"] == "Lan"
    assert data["top_10_verbatims"][0]["post_url"] == "https://example.com/p1"


async def test_topics_call_failure_skips_the_upsert() -> None:
    counts = {"positive": 20}

    with (
        patch("app.services.social_topic.get_movie_sentiment_counts", AsyncMock(return_value=counts)),
        patch("app.services.social_topic.get_comment_sample_for_movie", AsyncMock(return_value=SAMPLE_COMMENTS)),
        patch("app.services.social_topic.generate_topics_and_verbatims", AsyncMock(return_value=None)),
        patch("app.services.social_topic.upsert_social_topic_report", AsyncMock()) as mock_upsert,
    ):
        result = await generate_report_for_movie(MOVIE, dry_run=False)

    assert result == "topics_failed"
    mock_upsert.assert_not_awaited()


async def test_dry_run_never_writes() -> None:
    counts = {"positive": 20}

    with (
        patch("app.services.social_topic.get_movie_sentiment_counts", AsyncMock(return_value=counts)),
        patch("app.services.social_topic.get_comment_sample_for_movie", AsyncMock(return_value=SAMPLE_COMMENTS)),
        patch("app.services.social_topic.generate_topics_and_verbatims", AsyncMock(return_value=TOPICS_RESULT)),
        patch("app.services.social_topic.generate_narrative", AsyncMock(return_value="...")),
        patch("app.services.social_topic.upsert_social_topic_report", AsyncMock()) as mock_upsert,
    ):
        result = await generate_report_for_movie(MOVIE, dry_run=True)

    assert result == "generated"
    mock_upsert.assert_not_awaited()
