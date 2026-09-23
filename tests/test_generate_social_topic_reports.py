"""Exercises scripts/generate_social_topic_reports.py's own CLI-sweep logic
(--movie-id targeting a single movie instead of the full enabled-movies
list) - see test_social_topic.py for the per-movie report-generation logic
itself, which now lives in app/services/social_topic.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from scripts.generate_social_topic_reports import generate

MOVIE = {"id": "movie_1", "title": "Phim Test"}


async def test_movie_id_flag_targets_only_that_movie() -> None:
    with (
        patch("scripts.generate_social_topic_reports.get_movie_for_report", AsyncMock(return_value=MOVIE)),
        patch("scripts.generate_social_topic_reports.list_movies", AsyncMock()) as mock_list_movies,
        patch("scripts.generate_social_topic_reports.generate_report_for_movie", AsyncMock()) as mock_generate,
    ):
        await generate("movie_1", dry_run=False)

    mock_list_movies.assert_not_awaited()
    mock_generate.assert_awaited_once()
    assert mock_generate.await_args.args[0] == MOVIE
