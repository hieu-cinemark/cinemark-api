"""Exercises scripts/generate_social_topic_reports.py against fakes for
every D1/Kira call (no real D1 or Kira involved). What's worth verifying:
movies below MIN_COMMENTS_FOR_REPORT are skipped without spending a Kira
call, the upsert is keyed by movie_id with the real SQL-computed
percentages (not whatever the LLM might have guessed), and --dry-run never
writes."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from app.services.d1 import MIN_COMMENTS_FOR_REPORT
from scripts.generate_social_topic_reports import generate, generate_for_movie

MOVIE = {"id": "movie_1", "title": "Phim Test"}

SAMPLE_COMMENTS = [
    {"id": "c1", "post_id": "post_1", "message": "Hay quá", "reactions_count": 50, "sentiment": "positive"},
    {"id": "c2", "post_id": "post_2", "message": "Dở quá", "reactions_count": 10, "sentiment": "negative"},
]

TOPICS_RESULT = {
    "top_10_topics": [{"topic_name": "Diễn xuất", "sentiment": "Positive", "insight_summary": "...", "evidence_comments": []}],
    "top_10_verbatims": [{"text": "Hay quá", "likes": 50, "why_it_matters": "..."}],
}


async def test_movie_below_threshold_is_skipped_without_any_kira_call() -> None:
    counts = {"positive": MIN_COMMENTS_FOR_REPORT - 1}

    with (
        patch("scripts.generate_social_topic_reports.get_movie_sentiment_counts", AsyncMock(return_value=counts)),
        patch("scripts.generate_social_topic_reports.get_comment_sample_for_movie", AsyncMock()) as mock_sample,
        patch("scripts.generate_social_topic_reports.generate_topics_and_verbatims", AsyncMock()) as mock_topics,
        patch("scripts.generate_social_topic_reports.upsert_social_topic_report", AsyncMock()) as mock_upsert,
    ):
        await generate_for_movie(MOVIE, dry_run=False)

    mock_sample.assert_not_awaited()
    mock_topics.assert_not_awaited()
    mock_upsert.assert_not_awaited()


async def test_happy_path_upserts_with_sql_computed_percentages_not_llm_guessed() -> None:
    counts = {"positive": 30, "negative": 10}  # 75% / 25%, total 40

    with (
        patch("scripts.generate_social_topic_reports.get_movie_sentiment_counts", AsyncMock(return_value=counts)),
        patch("scripts.generate_social_topic_reports.get_comment_sample_for_movie", AsyncMock(return_value=SAMPLE_COMMENTS)),
        patch("scripts.generate_social_topic_reports.generate_topics_and_verbatims", AsyncMock(return_value=TOPICS_RESULT)),
        patch("scripts.generate_social_topic_reports.generate_narrative", AsyncMock(return_value="Nhìn chung tích cực.")),
        patch("scripts.generate_social_topic_reports.upsert_social_topic_report", AsyncMock(return_value=True)) as mock_upsert,
    ):
        await generate_for_movie(MOVIE, dry_run=False)

    mock_upsert.assert_awaited_once()
    kwargs = mock_upsert.await_args.kwargs
    assert kwargs["movie_id"] == "movie_1"
    assert kwargs["post_count"] == 2  # 2 distinct post_ids across SAMPLE_COMMENTS
    import json

    data = json.loads(kwargs["dashboard_data_json"])
    assert data["overall_sentiment"]["positive_percent"] == 75.0
    assert data["overall_sentiment"]["negative_percent"] == 25.0
    assert data["overall_sentiment"]["neutral_percent"] == 0.0
    assert data["overall_sentiment"]["analysis"] == "Nhìn chung tích cực."
    assert data["top_10_topics"] == TOPICS_RESULT["top_10_topics"]
    assert data["top_10_verbatims"] == TOPICS_RESULT["top_10_verbatims"]


async def test_topics_call_failure_skips_the_upsert() -> None:
    counts = {"positive": 20}

    with (
        patch("scripts.generate_social_topic_reports.get_movie_sentiment_counts", AsyncMock(return_value=counts)),
        patch("scripts.generate_social_topic_reports.get_comment_sample_for_movie", AsyncMock(return_value=SAMPLE_COMMENTS)),
        patch("scripts.generate_social_topic_reports.generate_topics_and_verbatims", AsyncMock(return_value=None)),
        patch("scripts.generate_social_topic_reports.upsert_social_topic_report", AsyncMock()) as mock_upsert,
    ):
        await generate_for_movie(MOVIE, dry_run=False)

    mock_upsert.assert_not_awaited()


async def test_dry_run_never_writes() -> None:
    counts = {"positive": 20}

    with (
        patch("scripts.generate_social_topic_reports.get_movie_sentiment_counts", AsyncMock(return_value=counts)),
        patch("scripts.generate_social_topic_reports.get_comment_sample_for_movie", AsyncMock(return_value=SAMPLE_COMMENTS)),
        patch("scripts.generate_social_topic_reports.generate_topics_and_verbatims", AsyncMock(return_value=TOPICS_RESULT)),
        patch("scripts.generate_social_topic_reports.generate_narrative", AsyncMock(return_value="...")),
        patch("scripts.generate_social_topic_reports.upsert_social_topic_report", AsyncMock()) as mock_upsert,
    ):
        await generate_for_movie(MOVIE, dry_run=True)

    mock_upsert.assert_not_awaited()


async def test_movie_id_flag_targets_only_that_movie() -> None:
    with (
        patch("scripts.generate_social_topic_reports.d1_query", AsyncMock(return_value=[MOVIE])),
        patch("scripts.generate_social_topic_reports.list_movies", AsyncMock()) as mock_list_movies,
        patch("scripts.generate_social_topic_reports.generate_for_movie", AsyncMock()) as mock_generate_for_movie,
    ):
        await generate("movie_1", dry_run=False)

    mock_list_movies.assert_not_awaited()
    mock_generate_for_movie.assert_awaited_once()
    assert mock_generate_for_movie.await_args.args[0] == MOVIE
